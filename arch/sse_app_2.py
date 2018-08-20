from flask import Flask, Response
import threading
from threading import get_ident
import time
from queue import Queue

class DataEvent(object):
    def __init__(self):
        self.events = {}

    def wait(self):
        ident = get_ident()
        if ident not in self.events:
            self.events[ident] = [threading.Event(), time.time()]
        return self.events[ident][0].wait()

    def set(self):
        now = time.time()
        remove = None
        for ident, event in self.events.items():
            if not event[0].isSet():
                event[0].set()
                event[1] = now
            else:
                if now - event[1] > 5:
                    remove = ident
        # multiple???
        if remove:
            del self.events[remove]

    def clear(self):
        self.events[get_ident()][0].clear()

class DataBase(object):
    thread = None
    frame = None
    last_access = 0
    event = DataEvent()

    q = Queue()
    q.put(['test 1', 'test 11', 'test 111'])
    q.put(['test 2', 'test 22', 'test 222'])
    q.put(['test 3', 'test 33', 'test 333'])

    def __init__(self):
        if DataBase.thread is None:
            DataBase.last_access = time.time()

            DataBase.thread = threading.Thread(target=self._thread)
            DataBase.thread.start()

            # need to wait???
            # while self.get_frame() is None:
            #     time.sleep(0.001)

    def get_frame(self):
        DataBase.last_access = time.time()

        DataBase.event.wait()
        DataBase.event.clear()

        return DataBase.frame

    @staticmethod
    def frames():
        while True:
            time.sleep(1)
            yield DataBase.q.get()
        #raise RuntimeError('Must be implemented by subclasses')

    @classmethod
    def _thread(cls):
        # frames_iterator = cls.frames()
        # print('iterator: ', frames_iterator)
        # for frame in frames_iterator:
        while True:
            frame = cls.q.get()
            for f in frame:
                #print('frame: ', f)
                DataBase.frame = f
                DataBase.event.set()
                time.sleep(0.001)

            if time.time() - DataBase.last_access > 10:
                #frames_iterator.close()
                print('kill data thread')
                break
        DataBase.thread = None

    @classmethod
    def publish(cls):
        DataBase.q.put(['this', 'is', 'new', 'data'])
        DataBase.q.put(['repeated'])


app = Flask(__name__)


@app.route("/publish")
def publish():
    DataBase.publish()
    return "OK"

def gen(data):
    while True:
        frame = data.get_frame()
        print(frame)
        yield "data: %s\n\n" % frame

@app.route('/stream')
def stream():
    return Response(
        gen(DataBase()),
        mimetype='text/event-stream'
    )

@app.route('/')
def index():
    template = """
    <html>
        <head></head>
        <body>
            <h1>Server Sent Events</h1>
            <button type="button">Add event</button>
            <div id="event"></div>
            <div id="demo" onclick="myFunction()">Click me to change my text color.</p>
            <script type="text/javascript">
                var container = document.getElementById("event");
                var ev = new EventSource('/stream');
                ev.onmessage = function(event){
                    console.log(event.data);
                    container.innerHTML = event.data;
                }
                
                function myFunction() {
                    document.getElementById("demo").style.color = "red";
                }
            </script>
        </body>
    </html>
    """
    return (template)

if __name__ == '__main__':
    app.debug = True
    app.run(threaded=True)