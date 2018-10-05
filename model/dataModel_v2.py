import os
import glob
import json
import copy
from model.parser import Parser
from model.database import DataBase, load_xml, load_image, after_query
from model.syncer_v2 import Syncer
from model.utils import load_json



class DataHandler(object):
    def __init__(self, config, project_dir):
        # config for db and parser
        self.config = config
        self.parser = Parser(config=config['XML'])
        self.DB = DataBase(
            host=config['DB']['HOST'],
            port=config['DB']['PORT']
        )
        # full path to a directories where project information (*.json) resides.
        self.project_dir = project_dir

        # list of project information from *.json
        self.projects = []
        #todo: make befow for loop as a function
        print('Load project *.json files ...')
        for filename in glob.glob(os.path.join(project_dir, '*.json')):
            data = load_json(filename)
            if data is not None:
                print('\tLoaded {}'.format(filename))
                self.projects.append(data)

        # maximum number of syncer threads
        self.max_num_syncers = 2
        self.num_syncers = 0
        # syncer pool, key: project file name, value: syncer
        self.syncer_pool = {}

    def get_projects(self):
        """Get information of all projects"""
        return json.dumps(self.projects)

    def update_project(self, project):
        updated = False
        for p_idx in range(len(self.projects)):
            proj = self.projects[p_idx]
            if project['filename'] == proj['filename']:
                self.projects[p_idx] = copy.deepcopy(project)
                updated = True
        if not updated:
            projects.append(project)

    def save_project(self, project):
        filename = project['filename']
        with open(os.path.join(self.project_dir, filename), 'w') as f:
            json.dump(project, f, indent=2, sort_keys=True)

    def run_syncer(self, project):
        """Initialize syncer, if it is available"""
        syncer_key = project['name']
        # check if the project is running (possibly by another clients)
        # if it is running, returns the project information currently used
        if syncer_key in self.syncer_pool and self.syncer_pool[syncer_key].t.is_alive():
            return 'RUNNING', self.syncer_pool[syncer_key].get_progress()
        # check max_num_syncer
        # if there are equal to or more than max_num_syncers running workers,
        #  returns with TRY AGAIN notification. Clients need to request again
        # later.
        if self.num_syncers > self.max_num_syncers:
            return 'TRY AGAIN', project
        # update project information in filesystem and this class
        #self.update_project(project)
        #self.save_project(project)

        # get DB cursors
        colCursor, fsCursor = self.DB.get_db(
            db=project['db'],
            col=project['col']
        )
        # initiate a worker
        def _onFinished():
            self.num_syncers -= 1

        worker = Syncer(
            name='syncer_{:s}'.format(project['name']),
            project=project,
            parser=self.parser,
            colCursor=colCursor,
            fsCursor=fsCursor,
            extensions=['xml', 'jpg', 'tiff'],
            interval=500,
            onFinished=_onFinished
        )
        # start updateing
        worker.start()
        # update information
        self.syncer_pool[project['name']] = worker
        self.num_syncers += 1

        # return status
        return 'RUNNING', worker.get_progress()

    def get_projects_in_sync(self):
        results = []
        to_delete = []
        for key, worker in self.syncer_pool.items():
            finished = not worker.t.is_alive()
            project = worker.get_progress()
            results.append((finished, project))

            if finished:
                to_delete.append(key)

        # delete based on the time stamp..
        # (1 hours later after finishing update)
        for key in to_delete:
            worker = self.syncer_pool[key]
            if abs(time.time() - worker.end_t) > 3600:
                del self.syncer_pool[key]

        return results

    def get_samplelist(self, project):
        db = project['db']
        col = project['col']

        h, _ = self.DB.get_db(db, col)
        pipeline = [
            {"$match": {"project": project['name']}},
            {"$match": {"sample": {"$exists": True, "$ne": None}}},
            {"$group": {"_id": "$sample", "count": {"$sum": 1}}}
        ]
        res = list(h.aggregate(pipeline))
        return res

    def get_samples(self, sampleNames, project):
        db = project['db']
        col = project['col']

        h, _ = self.DB.get_db(db, col)

        sampleData = {}
        for name in sampleNames:
            res = load_xml(h, name, project['name'])
            res = after_query(res)

            sampleData[name] = res

        return sampleData

    def get_tiff(self, id, db, col):
        colCursor, fsCursor = self.DB.get_db(db, col)

        res = load_image(colCursor, fsCursor, id, 'tiff')
        if res is None:
            return []

        if isinstance(res, list):
            res = res[0]

        data = res['tiff']['data']
        res['tiff']['data'] = data.tolist()
        return res['tiff']




    def check_syncer(self, syncer_key):
        if syncer_key not in self.syncer_pool:
            return None, None

        worker = self.syncer_pool[syncer_key]
        finished = not worker.t.is_alive()
        project = worker.get_progress()

        if finished:
            print('Syncing finished for {:s}'.format(syncer_key))
            self.num_syncers -= 1
            del self.syncer_pool[syncer_key]

        return finished, project


    def check_project(self, project):

        path = project['path']

        bPath = os.path.exists(path)
        if bPath:
            check_ext = ['*.xml', '*.jpg', '*.tiff']
            def _get_num_of_files(_path):
                return len(glob.glob(_path, recursive=True))

            nFiles = [_get_num_of_files(os.path.join(path, '**', ext))
                      for ext in check_ext]

            project['xml'] = nFiles[0]
            project['jpg'] = nFiles[1]
            project['tif'] = nFiles[2]
            project['path'] = path
        else:
            if "INVALID" not in path:
                path += ' (INVALID)'
            project['path'] = path

        return json.dumps(project)


if __name__ == '__main__':
    from config import CONFIG
    import time
    import pprint
    pp = pprint.PrettyPrinter(indent=4)

    project_dir = '../projects'
    DATA = DataHandler(CONFIG, project_dir)

    project = copy.deepcopy(DATA.projects[0])
    samplelist = DATA.get_samplelist(project)

    print(samplelist)

    # # for the debugging purpose, get project information from DATA
    # project_a = copy.deepcopy(DATA.projects[0])
    # project_a['filename'] = "test_saxs_a.json"
    # project_a['db'] = 'test_db'
    # project_a['col'] = 'test_col_a'
    # project_b = copy.deepcopy(DATA.projects[0])
    # project_b['filename'] = "test_saxs_b.json"
    # project_b['db'] = 'test_db'
    # project_b['col'] = 'test_col_b'
    # project_c = copy.deepcopy(DATA.projects[0])
    # project_c['filename'] = "test_saxs_c.json"
    # project_c['db'] = 'test_db_2'
    # project_c['col'] = 'test_col_c'
    # projects = [project_a, project_b, project_c]
    #
    # # run syncers
    # for proj in projects:
    #     DATA.run_syncer(proj)
    #
    # while DATA.num_syncers:
    #     for p_idx in range(len(projects)):
    #         syncer_key = projects[p_idx]['filename']
    #         finished, proj = DATA.check_syncer(syncer_key)
    #         if finished is None:
    #             print('Why None???')
    #         else:
    #             print('{:s}: {:s}, [{:s}, {:s}, {:s}]'.format(
    #                 proj['filename'],
    #                 'FINISHED' if finished else 'RUNNING',
    #                 proj['xml'], proj['jpg'], proj['tiff']
    #             ))
    #             projects[p_idx] = proj
    #
    #             if finished:
    #                 pp.pprint(proj)
    #     time.sleep(30)























