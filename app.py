"""
Web Server for MultiSciView

NOTES:
    1.

author: Sungsoo Ha (sungsooha@bnl.gov)
"""

import json
from flask import Flask, Response, request, render_template
from model.dataModel import DataHandler
from config import CONFIG


# ----------------------------------------------------------------------------
# Global variables
# ----------------------------------------------------------------------------
# web application
app = Flask(__name__)
# data handler including
# - managing MongoDB
# - sync files in folders user selected with MongoDB
# - monitoring folders, updating some events, and broadcasting the results
Data = DataHandler(
    rootDir=CONFIG['DATA_DIR'],
    fsMapFn=CONFIG['FSMAP'],
    db_host=CONFIG['DB']['HOST'],
    db_port=CONFIG['DB']['PORT'],
    xml_config=CONFIG['XML']
)


# ----------------------------------------------------------------------------
# db route
# ----------------------------------------------------------------------------
@app.route('/api/db/fsmap', methods=['POST'])
def db_treemap():
    """
    This is to communicate file system information.

    User will use this route to
        1. get the latest file system information
        2. update db field in the file system information

    In case, multiple clients attempt to update the db field for the same
    folder, ther server will take the first attempt and ignore the others.

    Server will always return the latest file system information with any
    updates from the clients. This also includes any changes in the file
    system itself, e.g. creating new folders or deleteing some folders. For
    this, server will do following operations:
        1. update fsmap with information a client provided, if any
        2. scan file system and update fsmap

        Note that thses operations will be serialized with Lock.

    Returns:
        updated fsmap
    """
    nodeList = request.get_json()['nodeList']
    if len(nodeList):
        Data.set_fsmap(nodeList)

    return json.dumps(Data.get_fsmap_as_list())

# ----------------------------------------------------------------------------
# syncer route
# ----------------------------------------------------------------------------
@app.route('/api/syncer/init', methods=['POST'])
def syncer_init():
    """
    This route is invoked when new directory is selected by clients.

    Returns:
        The server returns sync information related to the selected folder.
    """
    data = request.get_json()
    wdir = data['wdir']
    recursive = data['recursive']
    return json.dumps(Data.get_sync_info(wdir, recursive))

@app.route('/api/syncer/start', methods=['POST'])
def syncer_start():
    info = request.get_json()
    info = Data.update_sync_info(info)
    return json.dumps(info)

@app.route('/api/syncer/progress', methods=['POST'])
def syncer_progress():
    info = request.get_json()
    info = Data.update_sync_info(info)
    return json.dumps(info)


# ----------------------------------------------------------------------------
# stream
# ----------------------------------------------------------------------------
def gen(dataframe):
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
# Data
# ----------------------------------------------------------------------------
@app.route('/api/data/samplelist', methods=['POST'])
def get_db_samplelist():
    data = request.get_json()
    path = data['path']
    recursive = data['recursive']
    return json.dumps(Data.get_samplelist(path, recursive))

@app.route('/api/data/sample', methods=['POST'])
def get_sample():
    data = request.get_json()
    sampleNames = data['sampleNames']
    path = data['path']
    recursive = data['recursive']

    return json.dumps({
        'sampleList': sampleNames,
        'sampleData': Data.get_samples(sampleNames, path, recursive)
    })

@app.route('/api/data/tiff', methods=['POST'])
def get_tiff():
    data = request.get_json()
    id = data['id']
    path = data['path']
    return json.dumps(Data.get_tiff(id, path))


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