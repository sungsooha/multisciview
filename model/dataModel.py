import os
import threading
import time
import pymongo
import json
from threading import get_ident
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from queue import Queue


class DataEvent(object):
    """An Event-like class that signals all active clients when a new stream data is available"""
    def __init__(self):
        self.events = {}

    def wait(self):
        """Invoked from each client's thread to wait for the next stream data."""
        ident = get_ident()
        if ident not in self.events:
            # this is a new client
            # add an entry for it in the self.events dict
            # each entry has two elements, a threading.Event() and a timestamp
            self.events[ident] = [threading.Event(), time.time()]
        self.events[ident][0].wait()

    def set(self):
        """Invoked by who background thread in DataFrame """
        now = time.time()
        remove = []
        for ident, event in self.events.items():
            if not event[0].isSet():
                # if this client's event is not set, then set it
                # also update the last set timestamp to now
                event[0].set()
                event[1] = now
            else:
                # if the client's event is already set, it means the client
                # did not process a previous data frame.
                # if the event stays set for more than 5 seconds, then assume
                # the client is gone and remove it
                if now - event[1] > 5:
                    remove.append(ident)

        for ident in remove:
            print('remove: ', ident)
            del self.events[ident]

    def clear(self):
        """Invoked from each client's thread after a stream data was processed."""
        self.events[get_ident()][0].clear()

class BaseDataFrame(object):
    thread = None   # background thread that reads frames from DataHandler
    frame = None    # current frame is stored here by background thread
    last_access = 0 # time of last client access to the DataHandler
    event = DataEvent()

    def __init__(self):
        """Start the background thread if it isn't running yet."""
        if BaseDataFrame.thread is None:
            BaseDataFrame.last_access = time.time()

            # start background thread
            BaseDataFrame.thread = threading.Thread(target=self._process)
            BaseDataFrame.thread.daemon = True
            BaseDataFrame.thread.start()

    def get_frame(self):
        """Return the current data frame."""
        BaseDataFrame.last_access = time.time()

        # wait for a signal from the background thread
        BaseDataFrame.event.wait()
        BaseDataFrame.event.clear()

        return BaseDataFrame.frame

    @staticmethod
    def frames():
        """Generator that returns frames from the dataHandler"""
        raise RuntimeError('Must be implemented by subclasses.')

    @classmethod
    def _process(cls):
        """background thread process"""
        #print('start background thread')
        frame_iterator = cls.frames()
        for frame in frame_iterator:
            BaseDataFrame.frame = frame

            # send signal to clients
            BaseDataFrame.event.set()

            # if there hasn't been any clients asking for frames in the last 10 seconds
            # then stop the thread
            # if time.time() - BaseDataFrame.last_access > 10:
            #     #frame_iterator.close()
            #     print('kill background thread')
            #     break
        BaseDataFrame.thread = None

class FSHandler(FileSystemEventHandler):
    def __init__(
            self,
            callback=None,
            extensions:list=None,
            skip_dirs:list=None,
            skip_filenames:list=None
    ):
        self.callback = callback
        self.extensions = extensions if extensions is not None else ['.tiff', '.jpg', '.xml']
        self.skip_dirs = skip_dirs if skip_dirs is not None else []
        self.skip_filenames = skip_filenames if skip_filenames is not None else []

    def on_any_event(self, event):
        """
        Args:
            event:
                .event_type: 'modified' | 'created' | 'moved' | 'deleted'
                .is_directory: True | False
                .src_path: path/to/observed/file
        """
        now = time.time()
        if event.is_directory:
            if event.src_path not in self.skip_dirs:
                self._dispatch_dir_event(event, now)
        else:
            ext = os.path.splitext(event.src_path)[1]
            if ext in self.extensions:
                self._dispatch_file_event(event, now)

    def _dispatch_dir_event(self, event, timestamp):
        event_type = event.event_type
        if event_type == 'modified':
            pass
        elif event_type == 'created':
            pass
        elif event_type == 'moved':
            pass
        elif event_type == 'deleted':
            pass
        else:
            pass

    def _dispatch_file_event(self, event, timestamp):
        event_type = event.event_type
        src_path = os.path.realpath(event.src_path)

        if self.callback is not None:
            self.callback(event_type, src_path, timestamp)

        if event_type == 'modified':
            pass
        elif event_type == 'created':
            pass
        elif event_type == 'moved':
            pass
        elif event_type == 'deleted':
            pass
        else:
            pass

class DBHandler(object):
    def __init__(
            self,
            rootDir,
            fsmapFn,
            db_host='localhost',
            db_port=27017
    ):
        self.rootDir = os.path.realpath(os.path.abspath(rootDir))
        self.fsmapFn = fsmapFn
        self.db_host = db_host
        self.db_port = db_port

        # to ensure safe operation on fsmap
        self.fsmap_lock = threading.Lock()

        self.fsMap = self._load()
        self._traverse()
        self._save() # for debuggin...

        # lazy connection to MongoDB server
        # Must ensure mongod is running!
        self.client = pymongo.MongoClient(self.db_host, self.db_port)
        self.clientPool = {}

    def __del__(self):
        self.client.close()

    def _load(self):
        if not os.path.exists(self.fsmapFn): return {}

        try:
            with open(self.fsmapFn) as f:
                data = json.load(f)
        except (FileNotFoundError, TypeError, json.decoder.JSONDecodeError):
            print('[WARN] Failed to load saved fsmap, {}!!!'.format(self.fsmapFn))
            print('[WARN] Previous fsmap will be ignored, if there is.')
            return {}

        def __recursive_flatten(fsmap:dict, flattened:dict):
            item = dict(fsmap)
            item['children'] = [__recursive_flatten(child, flattened) for child in item['children']]
            flattened[item['path']] = item

        t = {}
        for key, value in data.items():
            __recursive_flatten(value, t)
        return t

    def _save(self):
        def __convert_to_hierarchical_format(key: str, fsmap: dict):
            item = dict(fsmap[key])
            item['children'] = [__convert_to_hierarchical_format(c, fsmap) for c in item['children']]
            return item

        t = {}
        p_keys = [key for key, value in self.fsMap.items() if value['parent'] is None]

        for key in p_keys: t[key] = __convert_to_hierarchical_format(key, self.fsMap)
        with open(self.fsmapFn, 'w') as f:
            json.dump(t, f, indent=2, sort_keys=True)

    def _traverse(self):
        """Traverse root directory"""
        fsmap = {}
        for dirpath, _, _ in os.walk(self.rootDir, followlinks=True):
            path = dirpath.replace(self.rootDir, '')
            tokens = path.split(os.sep)[1:]
            parent_path = os.path.join(self.rootDir, *tokens[:-1])

            real_path = os.path.realpath(dirpath)
            if len(path) == 0:
                fsmap[dirpath] = {
                    'path': dirpath,                             # absolute path to current directory
                    'realpath': real_path,       # realpath for symlink
                    'name': dirpath,  # name of current directory for display
                    'children': [],                              # list of absolute pathes of direct children directories
                    'parent': None,                              # absolute path to direct parent directory
                    'db': None,                                  # related database (db, collection)
                    'valid': True,                               # valid path flag
                    'inSync': False,                             # sync lock flag
                    'fixed': False,
                    'link': None,
                }
            else:
                fsmap[parent_path]['children'].append(dirpath)
                fsmap[dirpath] = {
                    'path': dirpath,
                    'realpath': real_path,
                    'name': os.path.basename(path),
                    'children': [],
                    'parent': fsmap[parent_path]['path'],
                    'db': None,
                    'valid': True,
                    'inSync': False,
                    'fixed': False,
                    'link': None
                }

        # update for symlink
        for key, value in fsmap.items():
            if not (key == value['realpath']):
                if value['realpath'] in fsmap:
                    fsmap[value['realpath']]['link'] = key
                    value['link'] = fsmap[value['realpath']]['path']


        def __merge_fsmap(dstMap:dict, srcMap:dict):
            for key, srcItem in srcMap.items():
                if key in dstMap:
                    # Is parent same? yes, it must be same as key is the absolute path.
                    # But children could be different. For example, one might delete/move/add
                    # sub-directories. But, we do not care, here.
                    dstItem = dstMap[key]
                    for key in ['db', 'valid', 'inSync', 'fixed', 'link']:
                        dstItem[key] = srcItem[key]
                else:
                    # This branch can happen when one delete/move/add subdirectories.
                    # Keep it, so that one can fix it manually in the json file.
                    srcItem['children'] = []
                    srcItem['parent'] = None
                    srcItem['valid'] = False
                    #srcItem['inSync'] = False
                    dstMap[key] = srcItem

        __merge_fsmap(fsmap, self.fsMap)
        self.fsMap = fsmap

    def get_fsmap_as_list(self):
        with self.fsmap_lock:
            fsmap_list = [[key, value] for key, value in self.fsMap.items() if value['valid']]
        return fsmap_list

    def set_fsmap(self, fsmap_list):
        with self.fsmap_lock:
            for key, value in fsmap_list:
                if key not in self.fsMap: continue     # filesystem may be updated...
                if self.fsMap[key]['fixed']: continue  # maybe touched by other clients...
                if value['db']  is None: continue         # db is not set
                if len(value['db']) != 3: continue      # must be 3-D array

                new_db = value['db'][0]
                new_col = value['db'][1]
                if len(new_db) == 0 or len(new_col) == 0: continue # in-complete setting
                if new_db == 'null' or new_col == 'null': continue # in-complete setting

                item = self.fsMap[key]
                item['db'] = [new_db, new_col, 'fs']
                item['fixed'] = True

            self._save()

    def set_db(self, path, db, col):
        if path not in self.fsMap:
            return False

        def __recursive_update(key: str, fsmap: dict):
            item = fsmap[key]
            if item['db'] is None: item['db'] = [db, col, 'fs']
            for child in item['children']:
                __recursive_update(child, fsmap)

        # update db setting recursively
        # If a path is already set before (or maybe by other client),
        # it didn't modify it. Given path may be not set as a client wants.
        with self.fsmap_lock:
            __recursive_update(path, self.fsMap)
            self._save()

        return True

    def get_db(self, path):
        pass


    def get_sample_list(self, db, col):
        pass

    def lock_path_to_sync(self, path):
        pass

    def unlock_path_to_sync(self, path):
        pass


class DataHandler(DBHandler):
    """
    DataHandler
    """
    def __init__(
            self,
            rootDir,
            fsMapFn='./fsmap.json',
            db_host='localhost',
            db_port=27017
    ):
        super().__init__(
            os.path.realpath(os.path.abspath(rootDir)),
            os.path.abspath(fsMapFn),
            db_host,
            db_port
        )

        self.fs_event_q = Queue() # event queue in filesystem
        self.stream_q = Queue()   # streaming data queue

        # watchdog
        self.observer = Observer()
        self.observer.schedule(
            FSHandler(
                callback=self._add_fs_event,
                extensions=['.xml', '.tiff', '.jpg'],
                skip_dirs=[],
                skip_filenames=[]
            ),
            self.rootDir, recursive=True)
        self.observer.start()

        # thread to handle fs event
        self.fs_thread = threading.Thread(target=self._fs_process, name='fs_thread')
        self.fs_thread.daemon = True
        self.fs_thread.start()

        # establish MongoDB connection
        #self.client = pymongo.MongoClient(self.db_host, self.db_port)



    def __del__(self):
        super().__del__()
        self.observer.stop()
        self.observer.join()
        #self.client.close()

    def _add_fs_event(self, event_type, src_path, timestamp):
        """Invoked from observer when there are new events in filesystem"""
        self.fs_event_q.put((event_type, src_path, timestamp))

    def _fs_process(self):
        """target function of self.fs_thread (daemon, background thread)"""
        while True:
            event = self.fs_event_q.get()

            # based on the event,
            # if directory event... update fsmap...
            # if file event... update database...
            # [NOTE]: symlink comes with the absolute path!
            time.sleep(1)

            # update stream data
            # need to fix. it is streaming way.. so don't need to keep all,
            # if there are no clients.
            print('fs_thread({}): {}'.format(get_ident(), event))
            self.stream_q.put('{:s}: {:s} @ {}'.format(event[0], event[1], event[2]))

    def get_dataframe(self):
        class DataFrame(BaseDataFrame):
            @staticmethod
            def frames():
                while True:
                    # need to fix
                    f = self.stream_q.get()
                    yield f
        return DataFrame()




if __name__ == '__main__':

    DIR = '/Users/scott/Desktop/test3'
    h = FSMapHandler(DIR, './fsmap.json')
