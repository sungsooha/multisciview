import os
import json
from flask import Flask, Response, request, render_template
from model.dataModel_v2 import DataHandler
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
# Data = DataHandler(
#     rootDir=CONFIG['DATA_DIR'],
#     fsMapFn=CONFIG['FSMAP'],
#     db_host=CONFIG['DB']['HOST'],
#     db_port=CONFIG['DB']['PORT'],
#     xml_config=CONFIG['XML']
# )
Data = DataHandler(
    config=CONFIG,
    project_dir='./projects'
)

# ----------------------------------------------------------------------------
# Project managing route
# ----------------------------------------------------------------------------
@app.route('/api/project', methods=['POST'])
def post_project():
    data = request.get_json()

    action = data['action']
    project = data['project']

    if action not in ['get', 'check', 'update']:
        return json.dumps({})
    elif action == 'get':
        return Data.get_projects()
    elif action == 'check' and project is not None:
        return Data.check_project(project)

    return json.dumps({})

@app.route('/api/project/validate', methods=['POST'])
def post_project_validate():
    data = request.get_json()

    path = data['path']
    if os.path.exists(path):
        return json.dumps('')
    else:
        return json.dumps('invalid path')


# ----------------------------------------------------------------------------
# Syncing route (update db from filesystem)
# ----------------------------------------------------------------------------
def parse_progress(project:dict):
    """Return progress in percentage"""
    def _parse(_str):
        _nums_str = _str.split('/')
        _processed = int(_nums_str[0])
        _total = int(_nums_str[1])
        return _processed, _total
    p_xml, t_xml = _parse(project['xml'])
    p_jpg, t_jpg = _parse(project['jpg'])
    p_tif, t_tif = _parse(project['tiff'])
    total = t_xml + t_jpg + t_tif
    processed = p_xml + p_jpg + p_tif
    return int(processed/total*100) if total else 0.

@app.route('/api/sync/request', methods=['POST'])
def sync_request():
    """
    Request updating DB based on a given project.
    """
    project = request.get_json()
    status, project = Data.run_syncer(project)
    progress = parse_progress(project)
    project['status'] = status
    project['progress'] = progress
    return json.dumps(project)

@app.route('/api/sync/progress', methods=['POST'])
def sync_progress():
    """
    Queried by clients to get sync. information, if any.

    Returns:
        Returns sync. progress on multi-threaded operations
    """
    results = Data.get_projects_in_sync()
    projects = []
    for res in results:
        finished, project = res
        status = 'FINISHED' if finished else 'RUNNING'
        progress = parse_progress(project)
        project['status'] = status
        project['progress'] = progress
        projects.append(project)
    return json.dumps(projects)



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