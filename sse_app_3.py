import json
from flask import Flask, Response, request, render_template
from model.dataModel import DataHandler

# ----------------------------------------------------------------------------
# Global variables
# ----------------------------------------------------------------------------
app = Flask(__name__)
Data = DataHandler(rootDir='/Users/scott/Desktop/test3')


# ----------------------------------------------------------------------------
# db route
# ----------------------------------------------------------------------------
@app.route('/api/db/fsmap', methods=['POST'])
def db_treemap():
    nodeList = request.get_json()['nodeList']
    if len(nodeList):
        Data.set_fsmap(nodeList)
    return json.dumps(Data.get_fsmap_as_list())

@app.route('/api/db/samplelist', methods=['POST'])
def db_samplelist():
    db = request.args.get('db')
    col = request.args.get('col')
    return json.dumps(Data.get_sample_list(db, col))

# ----------------------------------------------------------------------------
# syncer route
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

@app.route('/api/syncer/samples', methods=['POST'])
def syncer_samples():
    data = request.get_json()
    wdir = data['wdir']
    recursive = data['recursive']
    return json.dumps(Data.get_sync_samples(wdir, recursive))

@app.route('/api/syncer/start', methods=['POST'])
def syncer_start():
    info = request.get_json()
    print(info)

    # 1. set fsmap for sync
    resp = Data.set_sync_info(info)
    print(resp)

    # 2. then, run syncer
    syncer_id, resp = Data.run_syncer(resp)
    print(resp)

    # 3. return syncer id and progress information

    return json.dumps({
        'id': syncer_id,
        'resp': resp
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
# stream
# ----------------------------------------------------------------------------
def gen(dataframe):
    from threading import get_ident

    try:
        while True:
            frame = dataframe.get_frame()
            yield "data: %s\n\n" % frame
    except GeneratorExit:
        pass

@app.route('/stream')
def stream():
    return Response(
        gen(Data.get_dataframe()),
        mimetype='text/event-stream',
        headers={'Cache-Control': 'no-cache'}
    )

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
    pass


def main(host, port):
    try:
        app.run(host=host, port=port, threaded=True)
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