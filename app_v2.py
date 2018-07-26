from flask import Flask, render_template, request
from db.db_config import MONGODB_CONFIG
from db.parser import Parser
from model.dbModel import dbModel
from model.syncModel import syncerGroup
from model.watcherModel import watcherGroup
from bson.objectid import ObjectId
from bson.errors import InvalidId
import json

app = Flask(__name__)

# ----------------------------------------------------------------------------
# Global variables
# ----------------------------------------------------------------------------
g_dbModel = dbModel(
    rootDir=MONGODB_CONFIG['DATA_DIR'],
    filesystemMapFn=MONGODB_CONFIG['FSMAP'],
    hostname=MONGODB_CONFIG['DB']['HOST'],
    port=MONGODB_CONFIG['DB']['PORT']
)
g_syncerGroup = syncerGroup(
    parser=Parser(MONGODB_CONFIG['XML'])
)
g_watcherGroup = watcherGroup(
    parser=Parser(MONGODB_CONFIG['XML'])
)


# ----------------------------------------------------------------------------
# db view
# ----------------------------------------------------------------------------
@app.route('/api/db/fsmap', methods=['GET'])
def get_db_treemap():
    return json.dumps(g_dbModel.get_fsmap_as_list())

@app.route('/api/db/create', methods=['GET'])
def get_db_create():
    wdir = request.args.get('wdir')
    db = request.args.get('db')
    col = request.args.get('col')
    return json.dumps({'status': g_dbModel.update_db(wdir, db, col)})

@app.route('/api/db/samplelist', methods=['GET'])
def get_db_samplelist():
    db = request.args.get('db')
    col = request.args.get('col')
    return json.dumps(g_dbModel.get_sample_list(db, col))

@app.route('/api/db/drop', methods=['GET'])
def get_db_drop():
    pass


# ----------------------------------------------------------------------------
# syncer view
# ----------------------------------------------------------------------------
def get_syncer_start(wdir):
    if wdir is None:
        return json.dumps({
            'status': False,
            'message': 'Unknown directory to sync.'
        })

    files_to_sync = g_dbModel.get_files_to_sync(wdir)
    syncer_id, h = g_syncerGroup.init_syncer(files_to_sync)
    h.start()
    total, processed, completed = h.get_stat()
    return json.dumps({
        'status': True,
        'id': syncer_id,
        'total': total,
        'processed': processed,
        'completed': completed,
        'message': 'Start syncing.'
    })

def get_syncer_stop(syncer_id):
    h = g_syncerGroup.get_syncer(syncer_id)

    if h is None:
        return json.dumps({
            'status': False,
            'message': 'Failed to find sync handler.'
        })

    h.stop()
    total, processed, completed = h.get_stat()
    g_syncerGroup.delete_syncer(syncer_id)
    g_dbModel.unlock_files_to_sync(h.get_items_to_sync())
    return json.dumps({
        'status': True,
        'id': None,
        'total': total,
        'processed': processed,
        'completed': completed,
        'message': 'Stop syncing.'
    })

def get_syncer_progress(syncer_id):
    h = g_syncerGroup.get_syncer(syncer_id)
    if h is None:
        return json.dumps({
            'status': False,
            'message': 'Failed to find sync handler.'
        })

    total, processed, completed = h.get_stat()
    if completed:
        g_syncerGroup.delete_syncer(syncer_id)
        g_dbModel.unlock_files_to_sync(h.get_items_to_sync())

    return json.dumps({
        'status': True,
        'id': None if completed else syncer_id,
        'total': total,
        'processed': processed,
        'completed': completed,
        'message': 'Stop syncing.'
    })

@app.route('/api/syncer', methods=['GET'])
def get_syncer():
    mode = request.args.get('mode')
    wdir = request.args.get('wdir')
    syncerID = request.args.get('syncerID')

    if mode == 'START':
        return get_syncer_start(wdir)
    elif mode == 'STOP':
        return get_syncer_stop(syncerID)
    elif mode == 'PROGRESS':
        return get_syncer_progress(syncerID)
    else:
        return json.dumps({
            'status': False,
            'message': 'Unknown mode for sync operation: {}'.format(mode)
        })

# ----------------------------------------------------------------------------
# watcher view
# ----------------------------------------------------------------------------
def get_watcher_connect():
    pass

def get_watcher_disconnect():
    pass

def get_watcher_monitor():
    pass

@app.route('/api/watcher', methods=['GET'])
def get_watcher():
    pass

# ----------------------------------------------------------------------------
# data view
# ----------------------------------------------------------------------------
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

@app.route('/api/data/sample', methods=['GET'])
def get_sample():
    sampleList = request.args.getlist('name[]')
    db = request.args.get('db')
    col = request.args.get('col')

    h = g_dbModel.get_client([db, col, 'fs'])
    sampleData = {}
    for sample in sampleList:
        query = {"sample": sample}
        res = h.load(query=query, fields={}, getarrays=False)

        if not isinstance(res, list):
            res = [res]

        res = [replace_objid_to_str(doc) for doc in res]
        res = [flatten_dict(d) for d in res]
        for d in res:
            d['sample'] = '[{:s}][{:s}]{:s}'.format(db, col, sample)
            d['_id'] = '[{:s}][{:s}]{:s}'.format(db, col, d['_id'])
        sampleData[sample] = res

    return json.dumps({
        'sampleList': sampleList,
        'sampleData': sampleData
    })

@app.route('/api/data/tiff', methods=['GET'])
def get_tiff():
    db = request.args.get('db')
    col = request.args.get('col')
    _id = request.args.get('_id')
    try:
        _id = ObjectId(_id)
    except InvalidId:
        print('[get_tiff] Invalid ObjectId')
        return json.dumps({})

    h = g_dbModel.get_client([db, col, 'fs'])
    query = {'_id': _id, 'tiff':{'$exists':True}}
    fields = {'tiff': 1, '_id': 0}
    res = h.load(query, fields, getarrays=True)

    if res is None: return json.dumps({})
    data = res['tiff']['data']
    res['tiff']['data'] = data.tolist()
    return json.dumps(res['tiff'])


# ----------------------------------------------------------------------------
# main
# ----------------------------------------------------------------------------
@app.route('/')
def start():
    """
    Render index page
    """
    return render_template('index.html')


def finalize():
    """
    Finalize web server before exiting the program
    """
    for key, h in g_syncerGroup.syncerPool.items():
        h.stop()
        g_dbModel.unlock_files_to_sync(h.get_items_to_sync())


def main(host, port):
    try:
        app.run(host=host, port=port)
    except KeyboardInterrupt:
        pass
    finally:
        finalize()


if __name__ == '__main__':
    import argparse
    argparser = argparse.ArgumentParser(description="MultiSciView")
    argparser.add_argument("-s", "--serverhost",
                           type=str,
                           default='0.0.0.0',
                           help="Web server host address")
    argparser.add_argument("-p", "--serverport",
                           type=int,
                           default=8001,
                           help="Web server port number")
    args = argparser.parse_args()

    main(host=args.serverhost, port=args.serverport)
