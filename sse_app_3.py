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
def get_db_treemap():
    nodeList = request.get_json()['nodeList']
    if len(nodeList):
        Data.set_fsmap(nodeList)
    return json.dumps(Data.get_fsmap_as_list())

@app.route('/api/db/create', methods=['POST'])
def get_db_create():
    wdir = request.args.get('wdir')
    db = request.args.get('db')
    col = request.args.get('col')
    return json.dumps({'status': Data.set_db(wdir, db, col)})

@app.route('/api/db/samplelist', methods=['POST'])
def get_db_samplelist():
    db = request.args.get('db')
    col = request.args.get('col')
    return json.dumps(Data.get_sample_list(db, col))

@app.route('/api/db/drop', methods=['POST'])
def get_db_drop():
    return json.dumps({})


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