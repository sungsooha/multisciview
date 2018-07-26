import os
import threading
import time
from os.path import splitext
from threading import Thread
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from time import sleep

try:
    from Queue import Queue
except ImportError:
    from queue import Queue

class Handler(FileSystemEventHandler):
    def __init__(self, db, parser, pattern=None):
        self.pattern = pattern or (".xml", ".tiff", ".jpg")

        self.stop_flag = False
        self.eventScheduler = None
        self.dbManager = None

        # event queue, populated by Handler and consumed by eventScheduler
        self.q = Queue()

        # job list, managed by eventScheduler
        self.thresh = 2.
        self.joblist = []
        self.jobdonelist = []

        # job queue, populated by eventScheduler and consumed by dbManager
        self.jobq = Queue()
        self.jobq_hold_flag = False
        self.jobq_ready_flag = False

        # database (MongoDB)
        self.xml = parser
        self.db = db

    def set_jobq_hold_flag(self, value):
        self.jobq_hold_flag = value

    def set_jobq_ready_flag(self, value):
        self.jobq_ready_flag = value

    def get_jobq_hold_flag(self):
        return self.jobq_hold_flag

    def get_jobq_ready_flag(self):
        return self.jobq_ready_flag

    def get_jobdonelist(self):
        cp = list(self.jobdonelist)
        self.jobdonelist = []
        return cp

    def get_jobdonelist_size(self):
        return len(self.jobdonelist)

    def start(self):
        # NOTE: need to create new thread every start() call
        self.stop_flag = False
        self.eventScheduler = threading.Thread(target=self._process_q)
        #self.eventScheduler.daemon = True
        self.dbManager = threading.Thread(target=self._process_job)
        #self.dbManager.daemon = True
        self.eventScheduler.start()
        self.dbManager.start()

    def stop(self):
        # set flag to exit loop for the two local threads
        if not self.stop_flag:
            #print('stop handler')
            self.stop_flag = True
            while self.eventScheduler.is_alive() or self.dbManager.is_alive():
                sleep(0.001)
            #print('eventScheduler, dbManager threads are not alive')


    def on_modified(self, event):
        if not event.is_directory and event.src_path.endswith(self.pattern):
            self.q.put((event, time.time()))

    def on_deleted(self, event):
        if not event.is_directory and event.src_path.endswith(self.pattern):
            new_joblist = []
            for job in self.joblist:
                if not job[0] == event.src_path:
                    new_joblist.append(list(job))
            self.jobq.put(([event.src_path, event.event_type, time.time()], time.time()))
            self.joblist = new_joblist

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith(self.pattern):
            self.q.put((event, time.time()))

    def update_joblist(self, src_path, event_type, ts):
        the_job = None
        for job in self.joblist:
            if job[0] == src_path:
                the_job = job
                break

        if the_job is None:
            self.joblist.append([src_path, event_type, ts])
        else:
            the_job[2] = ts

    def check_joblist(self, ts):
        if len(self.joblist) == 0:
            return None

        the_job = None
        for job, idx in zip(self.joblist, range(len(self.joblist))):
            prev_ts = job[-1]
            if ts - prev_ts > self.thresh:
                the_job = list(job)
                del self.joblist[idx]
                break
        return the_job

    def _add_doc(self, doc):
        """
        Add a document. If a document having same 'item' value,
        it will update the exist one.
        """
        if doc is None:
            return None
        r = self.db.save_doc_one(doc)
        action = 'ADD' if r is None else 'UPDATE'
        print('[doc] {:s}: {:s}'.format(action, doc['item']))
        return action, 'xml', doc['item']

    def _add_img(self, doc, type='tiff'):
        """
        Add a tiff document. If a document having same 'item' value,
        it will update the exist one.
        """
        if doc is None:
            return None
        r = self.db.save_img_one(doc, type)
        action = 'ADD' if r is None else 'UPDATE'
        print('[{:s}] {:s}: {:s}'.format(type, action, doc['item']))
        return action, type, doc['item']

    def _del_doc(self, src_path):
        """
        Delete a document if it exists

        WARN: this will also delete all image data

        :param src_path:
        :return:
        """
        item_name, _ = splitext(src_path)
        item_name = item_name.split('/')[-1]
        query = {'item': item_name}
        out = self.db.load(query, {}, getarrays=False)
        if not out is None:
            # delete the document
            # WARN: this will also delete image data!!!!
            self.db.delete(out['_id'])
            print('DEL: {:s}'.format(item_name))
            return 'DELETE', 'xml', item_name
        return None

    def _process_job(self):
        print('start process job')
        while not self.stop_flag:
            #print('job empty: ', self.jobq.empty())
            if len(self.jobdonelist) and self.jobq_hold_flag:
                self.jobq_ready_flag = True
                time.sleep(0.001)
                continue
            self.jobq_ready_flag = False

            if self.jobq.empty():
                time.sleep(0.001)
                continue

            job, ts = self.jobq.get()
            #print('job: ', job)

            src_path = job[0]
            event_type = job[1]
            _, ext = os.path.splitext(src_path)

            #print(job)
            res = None
            if event_type == 'created' or event_type == 'modified':
                if ext == '.xml':
                    doc = self.xml.xml_to_doc(src_path)
                    res = self._add_doc(doc)
                elif ext == '.tiff':
                    doc = self.xml.tiff_to_doc(src_path)
                    res = self._add_img(doc, 'tiff')
                elif ext == '.jpg':
                    doc = self.xml.jpg_to_doc(src_path)
                    res = self._add_img(doc, 'jpg')
            elif event_type == 'deleted' and ext == '.xml':
                pass
                #res = self._del_doc(src_path)
            else:
                print("Unknown event type: ", event_type)

            if res is not None:
                self.jobdonelist.append(res)
                #print('[DEBUG]: add jobdone list: ', res)

            time.sleep(0.001)
        print('end process job')

    def _process_q(self):
        """
        process for eventScheduler
        : for events in the queue,
        :   1. update time stamp (when the event is inserted to job list)
        :   2. if the event exist, and time difference > threashold, pass to other thread (add to job queue)
        """
        print('start process q')
        while not self.stop_flag:
            #print(self.stop_flag)
            curr_ts = time.time()
            if self.q.empty():
                # update db (only one job at a time)
                job = self.check_joblist(curr_ts)
                if job is not None:
                    self.jobq.put((job, curr_ts))
                time.sleep(0.001)
                continue

            event, ts = self.q.get()
            #print('q: ', event)

            # update job list (event, ts)
            self.update_joblist(event.src_path, event.event_type, curr_ts)

            # update db (only one job at a time)
            job = self.check_joblist(curr_ts)
            if job is not None:
                self.jobq.put((job, curr_ts))

            #last_ts = time.time()

            time.sleep(0.001)
        print('end process q')

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
        self.observer.schedule(self.handler, os.path.realpath(self.wdir), recursive=True)
        self.observer.start()

        self.watcher_thread= Thread(target=self._run_watcher)
        #self.watcher_thread.daemon = True
        self.watcher_thread.start()

    def _run_watcher(self):
        print('Start file watcher')
        # try:
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
            sleep(.001)
        print('end file watcher')
        # except KeyboardInterrupt:
        #     self.observer.stop()
        #self.observer.join()

    def stop(self):
        print('stop watcher')
        self.handler.stop()

        if self.isRunning:
            self.isRunning = False
            while self.watcher_thread is not None and self.watcher_thread.is_alive():
                sleep(0.001)
            print("watcher thread died!")
            self.observer.stop()

class watcherGroup(object):
    def __init__(self, parser):
        self.parser = parser
        self.watcherPool = {}

















