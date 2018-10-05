import os
import threading
import glob
import time
import copy
from datetime import datetime
from model.parser import Parser
from model.database import DataBase
from model.database import save_image_document, save_document


class Syncer(object):
    def __init__(self,
                 name:str,
                 project:dict,
                 parser:Parser,
                 colCursor, fsCursor,
                 extensions:list,
                 interval:int,
                 onFinished = None
    ):
        # thread name
        self.name = name
        # project
        self.project = project
        # document parser
        self.parser = parser
        # DB
        self.colCursor = colCursor
        self.fsCursor = fsCursor
        # file extensions to retrieve
        self.extensions = extensions
        # progress update interval
        self.interval = interval
        # callback on finished
        self.onFinished = onFinished
        # start & end time
        self.start_t = 0
        self.end_t = 0

        # thread
        self.t = None

    def start(self):
        self.t = threading.Thread(target=self._process)
        self.t.daemon = True
        self.t.start()

    def get_progress(self):
        return copy.deepcopy(self.project)

    def _process(self):
        self.start_t = time.time()

        data_root = self.project['path']
        separator = self.project['separator']
        project_name = self.project['name']
        print('{:s} starts syncing under {:s}'.format(self.name, data_root))

        # collect file names to sync with DB
        # this approach might be slow, but not bad (~ 0.5 sec for around 5000)
        start_t = time.time()
        all_pathes = [
            os.path.join(data_root, '**', '*.' + ext)
            for ext in self.extensions
        ]
        all_files = [glob.glob(path, recursive=True) for path in all_pathes]

        # count the number of files to be updated
        for ext, files in zip(self.extensions, all_files):
            self.project[ext] = '{:d}/{:d}'.format(0,len(files))
        end_t = time.time()

        count_info = ['{:s}: {:s}'.format(ext, self.project[ext])
                      for ext in self.extensions]
        print('{:s} retrived all files to update, {}, [{:.3f} sec]'.format(
            self.name, count_info, end_t - start_t))

        # process on the files
        start_t = time.time()
        for ext, files in zip(self.extensions, all_files):
            print('{:s} starts syncing *.{:s} files'.format(self.name, ext))
            count = 0
            for f in files:
                basename = os.path.basename(f)
                sample_name = basename.split(separator)[0]
                # parsing
                doc = self.parser.run(
                    f,
                    kind=ext,
                    sample_name=sample_name,
                    project_name=project_name
                )
                # update
                if ext == 'xml':
                    save_document(self.colCursor, doc)
                else:
                    save_image_document(self.colCursor, self.fsCursor, doc, ext)
                count = count + 1
                # update progress
                if count%self.interval == 0 or count == len(files):
                    self.project[ext] = '{:d}/{:d}'.format(count, len(files))
                    print('{:s} completed syncing *.{:s} files [{:s}]'.format(
                        self.name, ext, self.project[ext]
                    ))

        end_t = time.time()
        # update progress
        dateFormat = "%Y-%m-%d %H:%M:%S"
        self.project['last_updated'] = datetime.now().strftime(dateFormat)
        print('{:s} finished syncing under {:s}, [{:3f} min]'.format(
            self.name, data_root, (end_t - start_t) / 60
        ))

        self.end_t = time.time()
        if self.onFinished:
            self.onFinished()

if __name__ == '__main__':
    from config import CONFIG
    from model.utils import load_json
    import pprint

    pp = pprint.PrettyPrinter(indent=4)

    # load project file to update
    project_filename = '../projects/test_saxs.json'
    project = load_json(project_filename)

    # parser and database
    parser = Parser(config=CONFIG['XML'])
    DB = DataBase(
        host=CONFIG['DB']['HOST'],
        port=CONFIG['DB']['PORT']
    )
    colCursor, fsCursor = DB.get_db('test_db', 'test_col')

    # initiate and run worker
    worker = Syncer(
        name='syncer',
        project=project,
        parser=parser,
        colCursor=colCursor,
        fsCursor=fsCursor,
        extensions=['xml', 'jpg', 'tiff'],
        interval=500
    )
    worker.start()

    while worker.t.is_alive():
        time.sleep(1)

    pp.pprint(worker.project)
