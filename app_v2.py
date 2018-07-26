from flask import Flask, render_template, request
from db.db_config import MONGODB_CONFIG
from model.dbModel import dbModel
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
# ----------------------------------------------------------------------------


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
@app.route('/api/syncer/start', methods=['GET'])
def get_syncer_start():
    pass

@app.route('/api/syncer/stop', methods=['GET'])
def get_syncer_stop():
    pass

@app.route('/api/syncer/progress', methods=['GET'])
def get_syncer_progress():
    pass



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
