import os
from flask import Flask, render_template, jsonify, request
from watchdog.observers import Observer
from db.db_config import MONGODB_CONFIG
from db.multiviewmongo import MultiViewMongo
from db.watcher_utils import xmlParser as Parser
from db.watcher import Handler
from threading import Thread
from bson.objectid import ObjectId
from time import sleep
from uuid import uuid4
import argparse

# Flask application
app = Flask(__name__)

# DB and file system handler
g_db = None
g_parser = None
root_dir = '.'


# directory path dependent
class Watcher(object):
    def __init__(self, wdir, db, parser):
        self.numUsers = 0
        self.wdir = wdir
        self.db = db
        self.parser = parser
        self.isRunning = False

        self.eventlist = []
        self.flag = 0 # 0: idle, 1: used by Watcher, 2: used by flask

        self.handler = Handler(db, parser)
        self.observer = None
        self.watcher_thread = None

    def getEventList(self):
        cp = list(self.eventlist)
        self.eventlist = []
        return cp

    def setFlag(self, flag):
        self.flag = flag

    def incNumUsers(self):
        self.numUsers += 1

    def decNumUsers(self):
        self.numUsers -= 1

    def _run_sync(self):
        for dirpath, dirnames, filenames in os.walk(self.wdir):
            print(dirpath, filenames)

    def start(self):
        self.isRunning = True
        self.handler.start()

        self.observer = Observer()
        self.observer.schedule(self.handler, self.wdir, recursive=True)
        self.observer.start()

        self.watcher_thread= Thread(target=self._run_watcher)
        self.watcher_thread.start()

    def _run_watcher(self):
        print('Start file watcher')
        try:
            while self.isRunning:
                if self.handler.get_jobdonelist_size() > 0 and self.flag == 0:
                    self.handler.set_jobq_hold_flag(True)
                    self.flag=1

                    if not self.handler.get_jobq_ready_flag():
                        self.flag = 0
                        sleep(.001)
                        continue

                    self.eventlist = self.eventlist + self.handler.get_jobdonelist()
                    self.handler.set_jobq_hold_flag(False)
                    self.flag = 0
                    #print('[DEBUG] eventlist: ', self.eventlist)
                sleep(1)
            print('end file watcher')
        except KeyboardInterrupt:
            self.observer.stop()
        self.observer.join()

    def stop(self):
        self.isRunning = False
        self.handler.stop()
        self.observer.stop()
        self.observer.join()

class Syncer(object):
    def __init__(self, wdir, db, parser):
        self.wdir = wdir
        self.db = db
        self.parser = parser

        all_files = []
        for dirpath, dirnames, filenames in os.walk(self.wdir):
            for f in filenames:
                if f.endswith('.xml') or f.endswith('.tiff') or f.endswith('.jpg'):
                    all_files.append(os.path.join(dirpath, f))

        self.all_files = all_files
        self.processed = 0
        self.finished = False

        self.isRunning = False

    def start(self):
        self.isRunning = True
        t = Thread(target=self._process)
        t.start()

    def stop(self):
        self.isRunning = False

    def _process(self):
        for f in self.all_files:
            if not self.isRunning:
                break

            if f.endswith('.xml'):
                doc = self.parser.xml_to_doc(f)
                self.db.save_doc_one(doc)
            elif f.endswith('.tiff'):
                doc = self.parser.tiff_to_doc(f)
                self.db.save_img_one(doc, 'tiff')
            elif f.endswith('.jpg'):
                doc = self.parser.jpg_to_doc(f)
                self.db.save_img_one(doc, 'jpg')
            self.processed += 1
            #print(f)
            #sleep(10)
        self.finished = True

watcherGroup = {}
syncerGroup = {}

def replace_objid_to_str(doc):
    if not isinstance(doc, dict):
        return doc

    for (key, value) in doc.items():
        if isinstance(value, ObjectId):
            doc[key] = str(value)
        elif isinstance(value, dict):
            doc[key] = replace_objid_to_str(value)

    return doc

def flatten_dict(d):
    def expand(key, value):
        if isinstance(value, dict):
            return [(key + '/' + k, v) for k, v in flatten_dict(value).items()]
        else:
            return [(key, value)]

    items = [item for k, v in d.items() for item in expand(k, v)]
    return dict(items)

def list_files(startpath):
    summary = {}
    count = 0
    for dirpath, dirnames, filenames in os.walk(startpath):
        dirpath = dirpath.replace(startpath,'')

        xml_count = 0
        jpg_count = 0
        tif_count = 0
        for f in filenames:
            if f.endswith('.xml'):
                xml_count += 1
            elif f.endswith('.jpg'):
                jpg_count += 1
            elif f.endswith('.tiff'):
                tif_count += 1

        tokens = dirpath.split(os.sep)[1:]
        parent_path = '/root'
        if len(tokens) > 1:
            parent_path = '/'
            for token in tokens[:-1]:
                parent_path += token + '/'
            parent_path = parent_path[:-1]

        node_id = 'N{:d}'.format(count)
        if len(dirpath) == 0:
            dirpath = '/root'
            summary[dirpath] = {
                'id': node_id,
                'name': os.path.basename(dirpath),
                'children': [],
                'count': [xml_count, jpg_count, tif_count],
                'path': dirpath
            }
        else:
            summary[parent_path]['children'].append(node_id)
            summary[dirpath] = {
                'id': node_id,
                'name': os.path.basename(dirpath),
                'children': [],
                'parent': summary[parent_path]['id'],
                'count': [xml_count, jpg_count, tif_count],
                'path': '/root' + dirpath
            }
        count += 1
    return summary

def _get_current_data_stat():
    pipeline = [{"$group": {"_id": "$sample", "count": {"$sum": 1}}}]
    res = list(g_db.collection.aggregate(pipeline))
    resDict = {}
    for r in res:
        sampleName = r['_id']
        count = r['count']
        resDict[sampleName] = count
    return resDict

@app.route('/api/watcher/dirlist', methods=['GET'])
def get_watcher_dirlist():
    wdir = request.args.get('wdir')
    key = str(wdir).replace('/root','')
    if len(key) == 0: key = '/root'

    dirDict = list_files(root_dir)
    nodeid = dirDict[key]['id']

    dirList = []
    for key, value in dirDict.items():
        dirList.append([value['id'], value])
    return jsonify({'dirList': dirList, 'nodeid': nodeid})

@app.route('/api/watcher/connect', methods=['GET'])
def get_watcher_connect():
    global watcherGroup
    args = request.args
    wdir = args.get('wdir')
    dir_to_watch = str(wdir).replace('/root', root_dir)

    if wdir in watcherGroup:
        _dh = watcherGroup[wdir]
    else:
        _dh = Watcher(dir_to_watch, g_db, g_parser)
        watcherGroup[wdir] = _dh

    try:
        if not _dh.isRunning:
            _dh.start()
        _dh.incNumUsers()
    except FileNotFoundError:
        return jsonify({'status': False, 'message': 'FileNotFoundError'})

    return jsonify({'status': True, 'message': 'DB connected'})

@app.route('/api/watcher/disconnect', methods=['GET'])
def get_watcher_disconnect():
    global watcherGroup
    args = request.args
    wdir = str(args.get('wdir'))
    try:
        _dh = watcherGroup[wdir]
        _dh.decNumUsers()
        if _dh.numUsers == 0:
            _dh.stop()
            del watcherGroup[wdir]
    except KeyError:
        return jsonify({'status': False, 'message': 'Unknow parameters {:s}'.format(wdir)})

    return jsonify({'status': False, 'message': 'DB disconnected'})

@app.route('/api/sync', methods=['GET'])
def get_sync():
    global syncerGroup, g_db, g_parser
    wdir = request.args.get('wdir')
    wdir = str(wdir).replace('/root', root_dir)
    syncId = str(uuid4())
    while syncId in syncerGroup:
        syncId = str(uuid4())

    sync_worker = Syncer(wdir, g_db, g_parser)
    syncerGroup[syncId] = sync_worker
    total = len(sync_worker.all_files)

    syncerGroup[syncId].start()
    return jsonify({'id': syncId, 'total': total})

@app.route('/api/sync/stop', methods=['GET'])
def get_sync_stop():
    global syncerGroup
    syncId = str(request.args.get('id'))
    print(syncId)

    if syncId not in syncerGroup:
        return jsonify({})

    sync_worker = syncerGroup[syncId]
    sync_worker.stop()
    del syncerGroup[syncId]
    return jsonify({})

@app.route('/api/sync/progress', methods=['GET'])
def get_sync_progress():
    global syncerGroup
    syncId = str(request.args.get('id'))

    if syncId not in syncerGroup:
        return jsonify({'id': None, 'processed': 0, 'total': 0, 'finished': True})

    sync_worker = syncerGroup[syncId]
    processed = sync_worker.processed
    total = len(sync_worker.all_files)
    finished = sync_worker.finished
    if sync_worker.finished:
        del syncerGroup[syncId]
    return jsonify({'id': syncId,'processed': processed, 'total': total, 'finished': finished})

@app.route('/api/watcher/monitor', methods=['GET'])
def get_watcher_monitor():
    global watcherGroup
    stat = _get_current_data_stat()

    wdir = request.args.get('wdir')
    try:
        _dh = watcherGroup[wdir]
    except KeyError:
        return jsonify({'sampleList': [], 'sampleData': {}, 'stat': stat})

    if _dh is None:
        print('[!][{:s}] No watcher handler'.format(wdir))
        return jsonify({'sampleList': [], 'sampleData': {}, 'stat': stat})
    if _dh.flag > 0:
        print('[!][{:s}] Busy watcher handler'.format(wdir))
        return jsonify({'sampleList': [], 'sampleData': {}, 'stat': stat})

    _dh.setFlag(2)
    eventlist = _dh.getEventList()
    if len(eventlist) == 0:
        print('[!][{:s}] Empty event list'.format(wdir))
        _dh.setFlag(0)
        return jsonify({'sampleList': [], 'sampleData': {}, 'stat': stat})
    _dh.setFlag(0)

    #print('[DEBUG] Handle events: ', eventlist)
    # event: tuple
    # - [0]: action in [ADD, UPDATE, DELETE]
    # - [1]: type in [.xml, .tiff, .jpg]
    # - [2]: item name (assume it is unique for each data point)
    resList = []
    processed = []
    sampleList = []
    sampleData = {}

    for event in list(reversed(eventlist)):
        action, doc_type, item_name = event
        if item_name in processed or action == 'DELETE':
            continue

        query = {'item': item_name}
        res = _dh.db.load(query=query, fields={}, getarrays=False)
        #print(item_name, res)

        if not isinstance(res, list): res = [res]
        res = [replace_objid_to_str(doc) for doc in res]
        res = [flatten_dict(d) for d in res]
        resList.append(res)
        processed.append(item_name)

    #print(resList)
    for res in resList:
        if isinstance(res, list): res = res[0]
        sampleName = res['sample']
        if sampleName in sampleList:
            sampleData[sampleName].append(res)
        else:
            sampleList.append(sampleName)
            sampleData[sampleName] = [res]
    return jsonify({
        'sampleList': sampleList,
        'sampleData': sampleData,
        'stat': stat
    })

@app.route('/api/data/stat', methods=['GET'])
def get_current_data_stat():
    return jsonify(_get_current_data_stat())

@app.route('/api/data/sample', methods=['GET'])
def get_sample():
    global g_db

    if g_db is None:
        return jsonify({'sampleList': [], 'sampleData': {}, 'stat': {}})
    sampleList = request.args.getlist('name[]')

    sampleData = {}
    for sample in sampleList:
        query = {"sample": sample}
        res = g_db.load(query=query, fields={}, getarrays=False)

        if not isinstance(res, list):
            res = [res]
        res = [replace_objid_to_str(doc) for doc in res]
        res = [flatten_dict(d) for d in res]
        sampleData[sample] = res

    return jsonify({
        'sampleList': sampleList,
        'sampleData': sampleData,
        'stat': _get_current_data_stat()
    })

@app.route('/api/data/tiff/<id>', methods=['GET'])
def get_tiff(id):
    global g_db
    if g_db is None:
        return jsonify({})

    query = {'_id': ObjectId(id), 'tiff':{'$exists':True}}
    fields = {'tiff': 1, '_id': 0}
    res = g_db.load(query, fields, getarrays=True)

    if res is None:
        return jsonify({})

    data = res['tiff']['data']
    res['tiff']['data'] = data.tolist()
    return jsonify(res['tiff'])

@app.route('/')
def start():
    return render_template('index.html')

def main(host, port, rootdir):
    print(host, port, rootdir)
    global watcherGroup, syncerGroup, g_db, g_parser, root_dir
    g_db = MultiViewMongo(
        db_name=MONGODB_CONFIG['DB']['NAME'],
        collection_name=MONGODB_CONFIG['DB']['COLLECTION'],
        hostname=MONGODB_CONFIG['DB']['HOST'],
        port=MONGODB_CONFIG['DB']['PORT']
    )
    g_parser = Parser(config=MONGODB_CONFIG['XML'])
    root_dir = root_dir

    try:
        app.run(host=host, port=port)
    except KeyboardInterrupt:
        pass
    finally:
        for id, watcher in watcherGroup.items():
            watcher.stop()

        for id, syncer in syncerGroup.items():
            syncer.stop()

        watcherGroup = {}
        syncerGroup = {}

if __name__ == '__main__':
    argparser = argparse.ArgumentParser(description="MultiSciView")
    argparser.add_argument("-s", "--serverhost", type=str, default='localhost', help="Web Server host address")
    argparser.add_argument("-p", "--serverport", type=int, default=8001, help="Web Server port number")
    argparser.add_argument("-r", "--rootdir", type=str, default='.', help="root directory to watch in a local filesystem")
    args = argparser.parse_args()

    main(host=args.serverhost, port=args.serverport, rootdir=args.rootdir)
