import os
from flask import Flask, render_template, jsonify, request
from db.db_config import MONGODB_CONFIG
#from db.multiviewmongo import MultiViewMongo
from db.groups import dbGroup, watcherGroup, syncerGroup
from db.watcher_utils import xmlParser as Parser
from db.watcher import Watcher
from threading import Thread
from bson.objectid import ObjectId
from bson.errors import InvalidId
from uuid import uuid4
import argparse
import json


# Flask application
app = Flask(__name__)

# DB and file system handler
g_db = dbGroup(
    rootDir=MONGODB_CONFIG['DATA_DIR'],
    dirTreeMapFn=MONGODB_CONFIG['DIR_TREEMAP'],
    hostname=MONGODB_CONFIG['DB']['HOST'],
    port=MONGODB_CONFIG['DB']['PORT']
)
g_watcher = watcherGroup(
    root_dir=MONGODB_CONFIG['DATA_DIR'],
    parser=Parser(config=MONGODB_CONFIG['XML'])
)
g_syncer = syncerGroup(
    root_dir=MONGODB_CONFIG['DATA_DIR'],
    parser=Parser(config=MONGODB_CONFIG['XML'])
)
#g_db = None
#g_parser = None
#root_dir = '.'


# directory path dependent
# class Watcher(object):
#     def __init__(self, wdir, db, parser):
#         self.numUsers = 0
#         self.wdir = wdir
#         self.db = db
#         self.parser = parser
#         self.isRunning = False
#
#         self.eventlist = []
#         self.flag = 0 # 0: idle, 1: used by Watcher, 2: used by flask
#
#         self.handler = Handler(db, parser)
#         self.observer = None
#         self.watcher_thread = None
#
#     def getEventList(self):
#         cp = list(self.eventlist)
#         self.eventlist = []
#         return cp
#
#     def setFlag(self, flag):
#         self.flag = flag
#
#     def incNumUsers(self):
#         self.numUsers += 1
#
#     def decNumUsers(self):
#         self.numUsers -= 1
#
#     def _run_sync(self):
#         for dirpath, dirnames, filenames in os.walk(self.wdir):
#             print(dirpath, filenames)
#
#     def start(self):
#         self.isRunning = True
#         self.handler.start()
#
#         self.observer = Observer()
#         self.observer.schedule(self.handler, os.path.realpath(self.wdir), recursive=True)
#         self.observer.start()
#
#         self.watcher_thread= Thread(target=self._run_watcher)
#         self.watcher_thread.start()
#
#     def _run_watcher(self):
#         print('Start file watcher')
#         try:
#             while self.isRunning:
#                 if self.handler.get_jobdonelist_size() > 0 and self.flag == 0:
#                     self.handler.set_jobq_hold_flag(True)
#                     self.flag=1
#
#                     if not self.handler.get_jobq_ready_flag():
#                         self.flag = 0
#                         sleep(.001)
#                         continue
#
#                     self.eventlist = self.eventlist + self.handler.get_jobdonelist()
#                     self.handler.set_jobq_hold_flag(False)
#                     self.flag = 0
#                     #print('[DEBUG] eventlist: ', self.eventlist)
#                 sleep(1)
#             print('end file watcher')
#         except KeyboardInterrupt:
#             self.observer.stop()
#         self.observer.join()
#
#     def stop(self):
#         self.isRunning = False
#         self.handler.stop()
#         self.observer.stop()
#         self.observer.join()


#watcherGroup = {}
#syncerGroup = {}

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



# deprecated, don't use
# def _get_current_data_stat():
#     pipeline = [{"$group": {"_id": "$sample", "count": {"$sum": 1}}}]
#     res = list(g_db.collection.aggregate(pipeline))
#     resDict = {}
#     for r in res:
#         sampleName = r['_id']
#         count = r['count']
#
#         if type(sampleName) is list:
#             continue
#
#         resDict[sampleName] = count
#
#         #print(sampleName, count)
#     #print(resDict)
#     return resDict

# to be deprecated
@app.route('/api/db', methods=['GET'])
def get_db_info():

    db = request.args.get('db')
    col = request.args.get('col')

    db_names = g_db.get_db_names()
    if db is None:
        try:
            db_selected = db_names[0]
        except IndexError:
            db_selected = None
    else:
        db_selected = db
        if db not in db_names:
            db_names = [db] + db_names

    col_names = g_db.get_col_names(db_selected) if db_selected is not None else []
    if col is None:
        try:
            col_selected = col_names[0]
        except IndexError:
            col_selected = None
    else:
        col_selected = col
        if col not in col_names:
            col_names = [col] + col_names

    sample_names = []
    if db_selected is not None and col_selected is not None:
        sample_names = g_db.get_sample_names(db_selected, col_selected)

    return json.dumps({
        'db': db_selected,
        'col': col_selected,
        'dbList': db_names,
        'colList': col_names,
        'sampleList': sample_names
    })

# to be deprecated
@app.route('/api/watcher/dirlist', methods=['GET'])
def get_watcher_dirlist():
    wdir = request.args.get('wdir')
    return json.dumps(g_watcher.get_dir_list(wdir))

# replaced with get_db_info() and get_watcher_dirlist()
@app.route('/api/db/treemap', methods=['GET'])
def get_treemap():
    return json.dumps(g_db.get_treemap())

#
@app.route('/api/db/create', methods=['GET'])
def get_db_create():
    """
    Create a database with given db and collection name

    NOTE:
    """
    return json.dumps({})


@app.route('/api/watcher/connect', methods=['GET'])
def get_watcher_connect():
    wdir = request.args.get('wdir')
    db = request.args.get('db')
    col = request.args.get('col')

    h_watcher = g_watcher.get_watcher(wdir, g_db.open(db, col))
    try:
        if not h_watcher.isRunning:
            h_watcher.start()
        h_watcher.incNumUsers()
    except FileNotFoundError:
        h_watcher.stop()
        return json.dumps({'status': False, 'message': 'FileNotFoundError'})
    return json.dumps({'status': True, 'message': 'DB connected'})

@app.route('/api/watcher/disconnect', methods=['GET'])
def get_watcher_disconnect():
    wdir = str(request.args.get('wdir'))
    db = request.args.get('db')
    col = request.args.get('col')

    #h_watcher = g_watcher.get_watcher(wdir, g_db.open(db, col))
    h_watcher = g_watcher.get_watcher_only(wdir, db, col)
    if h_watcher is None:
        return json.dumps({'status': False, 'message': 'The watcher is not in the pool.'})

    h_watcher.decNumUsers()
    if h_watcher.numUsers == 0:
        h_watcher.stop()
        g_watcher.delete_watcher(wdir, db, col)

    return json.dumps({'status': False, 'message': 'DB disconnected'})



@app.route('/api/sync', methods=['GET'])
def get_sync():
    wdir = request.args.get('wdir')
    db = request.args.get('db')
    col = request.args.get('col')

    h = g_syncer.get_syncer(wdir, g_db.open(db, col))
    if h.isRunning:
        total, processed = h.get_progress()
    else:
        h.start()
        total, processed = h.get_progress()

    return json.dumps({'total': total, 'processed': processed})

@app.route('/api/sync/stop', methods=['GET'])
def get_sync_stop():
    wdir = request.args.get('wdir')
    db = request.args.get('db')
    col = request.args.get('col')

    h = g_syncer.get_syncer_only(wdir, db, col)
    if h is not None:
        h.stop()
        g_syncer.delete_syncer(wdir, db, col)

    return json.dumps({})


@app.route('/api/sync/progress', methods=['GET'])
def get_sync_progress():
    global syncerGroup
    syncId = str(request.args.get('id'))

    if syncId not in syncerGroup:
        return json.dumps({'id': None, 'processed': 0, 'total': 0, 'finished': True})
        #return jsonify({'id': None, 'processed': 0, 'total': 0, 'finished': True})

    sync_worker = syncerGroup[syncId]
    processed = sync_worker.processed
    total = len(sync_worker.all_files)
    finished = sync_worker.finished
    if sync_worker.finished:
        del syncerGroup[syncId]

    return json.dumps({'id': syncId,'processed': processed, 'total': total, 'finished': finished})
    #return jsonify({'id': syncId,'processed': processed, 'total': total, 'finished': finished})

@app.route('/api/watcher/monitor', methods=['GET'])
def get_watcher_monitor():
    wdir = request.args.get('wdir')
    h_watcher = g_watcher.get_watcher_only(wdir)

    if h_watcher is None:
        print('[!][{:s}] No watcher handler'.format(wdir))
        return json.dumps({'sampleList': [], 'sampleData': {}})

    if h_watcher.flag > 0:
        print('[!][{:s}] Busy watcher handler'.format(wdir))
        return json.dumps({'sampleList': [], 'sampleData': {}})

    h_watcher.setFlag(2)
    eventlist = h_watcher.getEventList()
    if len(eventlist) == 0:
        print('[!][{:s}] Empty event list'.format(wdir))
        h_watcher.setFlag(0)
        return json.dumps({'sampleList': [], 'sampleData': {}})
    h_watcher.setFlag(0)

    #print('[DEBUG] Handle events: ', eventlist)
    # event: tuple
    # - [0]: action in [ADD, UPDATE, DELETE]
    # - [1]: type in [.xml, .tiff, .jpg]
    # - [2]: item name (assume it is unique for each data point)
    resList = []
    processed = []
    sampleList = []
    sampleData = {}

    db = h_watcher.db.db_name
    col = h_watcher.db.collection_name

    for event in list(reversed(eventlist)):
        action, doc_type, item_name = event
        if item_name in processed or action == 'DELETE':
            continue

        query = {'item': item_name}
        res = h_watcher.db.load(query=query, fields={}, getarrays=False)
        #print(item_name, res)

        if not isinstance(res, list): res = [res]
        res = [replace_objid_to_str(doc) for doc in res]
        res = [flatten_dict(d) for d in res]

        for d in res:
            d['sample'] = '[{:s}][{:s}]{:s}'.format(db, col, d['sample'])
            d['_id'] = '[{:s}][{:s}]{:s}'.format(db, col, d['_id'])

        resList.append(res)
        processed.append(item_name)

    for res in resList:
        if isinstance(res, list): res = res[0]
        sampleName = res['sample']
        if sampleName in sampleList:
            sampleData[sampleName].append(res)
        else:
            sampleList.append(sampleName)
            sampleData[sampleName] = [res]

    return json.dumps({
        'sampleList': sampleList,
        'sampleData': sampleData,
    })

# deprecated
# @app.route('/api/data/stat', methods=['GET'])
# def get_current_data_stat():
#     stat = _get_current_data_stat()
#     return json.dumps(stat)


@app.route('/')
def start():
    return render_template('index.html')

def main(host, port):
    try:
        app.run(host=host, port=port)
    except KeyboardInterrupt:
        pass
    finally:
        g_watcher.stop()

if __name__ == '__main__':
    argparser = argparse.ArgumentParser(description="MultiSciView")
    argparser.add_argument("-s", "--serverhost", type=str, default='0.0.0.0', help="Web Server host address")
    #argparser.add_argument("-s", "--serverhost", type=str, default='localhost', help="Web Server host address")
    argparser.add_argument("-p", "--serverport", type=int, default=8001, help="Web Server port number")
    args = argparser.parse_args()

    main(host=args.serverhost, port=args.serverport)
