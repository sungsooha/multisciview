import os
import threading
import time
import pymongo
import json
from bson.objectid import ObjectId
from bson.errors import InvalidId
from threading import get_ident
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from db.multiviewmongo import MultiViewMongo
from model.syncer import Syncer
from queue import Queue
from model.parser import Parser
import datetime


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
        if event.is_directory:
            if event.src_path not in self.skip_dirs:
                self._dispatch_dir_event(event)
        else:
            ext = os.path.splitext(event.src_path)[1]
            if ext in self.extensions:
                self._dispatch_file_event(event)

    def _dispatch_dir_event(self, event):
        event_type = event.event_type
        src_path = event.src_path

        if event_type in ['created', 'deleted']:
            self.callback('dir', event_type, src_path, None)
        elif event_type in ['moved']:
            self.callback('dir', event_type, src_path, event.dest_path)
        else:
            pass


    def _dispatch_file_event(self, event):
        event_type = event.event_type
        src_path = os.path.realpath(event.src_path)

        if event_type in ['created', 'deleted', 'modified']:
            self.callback('file', event_type, src_path, None)
        elif event_type in ['moved']:
            self.callback('file', event_type, src_path, event.dest_path)
        else:
            pass

class DBHandler(object):
    def __init__(
            self,
            rootDir,
            fsmapFn,
            db_host='localhost',
            db_port=27017,
            xml_config=None
    ):
        self.rootDir = os.path.realpath(os.path.abspath(rootDir))
        self.fsmapFn = fsmapFn
        self.db_host = db_host
        self.db_port = db_port

        self.parser = Parser(xml_config) if xml_config is not None else None

        self.extensions = ['.xml', '.jpg', '.tiff']

        # to ensure safe operation on fsmap
        self.fsmap_lock = threading.Lock()

        self.fsMap = self._load()
        self._traverse()
        self._save() # for debuggin...

        # lazy connection to MongoDB server
        # Must ensure mongod is running!
        self.client = pymongo.MongoClient(self.db_host, self.db_port)
        self.clientPool = {}

        # streaming queues
        self.fs_event_q = Queue()
        self.stream_q = Queue()

        # old map
        # This keeps old fsmap information when file system changes manually
        # e.g. folder move, rename, etc
        # If it is not empty dictionary, there is a bug....
        self._old_fsmap = {}

    def __del__(self):
        for _, h in self.clientPool.items():
            h.close()
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

    def _traverse(self, save_old=False):
        """Traverse root directory"""
        fsmap = {}
        for dirpath, _, _ in os.walk(self.rootDir, followlinks=True):
            path = dirpath.replace(self.rootDir, '')
            tokens = path.split(os.sep)[1:]
            parent_path = os.path.join(self.rootDir, *tokens[:-1])

            real_path = os.path.realpath(dirpath)
            if len(path) == 0:
                name = dirpath
                parent = None
            else:
                name = os.path.basename(path)
                fsmap[parent_path]['children'].append(dirpath)
                parent = fsmap[parent_path]['path']

            fsmap[dirpath] = {
                'path': dirpath,        # absolute path to current directory
                'realpath': real_path,  # realpath for symlink
                'name': name,           # name of current directory for display
                'children': [],         # list of absolute pathes of direct children directories
                'parent': parent,       # absolute path to direct parent directory
                'link': None,           # linked path

                # valid path flag
                # It will turn into False, if the given path doesn't exist by
                #  comparing with fsmap in the file.
                'valid': True,          # valid path flag

                # This set to Ture, once a client set the `db` field.
                # Then, `db` filed can be modified only manually via fsmap file.
                # Such modification requires to re-run the web server.
                'db': None,  # related database (db, collection)
                'fixed': False,         # can modify?

                # used for syncing
                'file': None,  # sample file name used to determine group name
                'sep': None,   # separator used to parse group name from the file
                'group': None, # group name in this folder
                'last_sync': None, # the last date and time sync is applied
            }

        # update for symlink
        for key, value in fsmap.items():
            if not (key == value['realpath']):
                if value['realpath'] in fsmap:
                    fsmap[value['realpath']]['link'] = key
                    value['link'] = fsmap[value['realpath']]['path']

        # save unregistered fsmap from old one
        if save_old:
            for key, value in self.fsMap.items():
                if key not in fsmap:
                    self._old_fsmap[key] = dict(value)

        _keys_to_copy = ['valid', 'db', 'fixed', 'file', 'sep', 'group',
                         'last_sync']
        def __merge_fsmap(dstMap:dict, srcMap:dict):
            for _path, _srcItem in srcMap.items():
                if _path in dstMap:
                    # Is parent same? yes, it must be same as key is the absolute path.
                    # But children could be different. For example, one might delete/move/add
                    # sub-directories. But, we do not care, here.
                    _dstItem = dstMap[_path]
                    for _k in _keys_to_copy:
                        _dstItem[_k] = _srcItem[_k]
                else:
                    # This branch can happen when one delete/move/add subdirectories.
                    # Keep it, so that one can fix it manually in the json file.
                    _srcItem['children'] = []
                    _srcItem['parent'] = None
                    _srcItem['valid'] = False
                    #srcItem['inSync'] = False
                    dstMap[key] = _srcItem
        __merge_fsmap(fsmap, self.fsMap)
        self.fsMap = fsmap

    def _update_fsmap(self, event_type, src_path, dst_path):
        """Invoked when filesystem changes (only for directory changes)"""
        with self.fsmap_lock:
            if event_type in ['created', 'deleted']:
                # on create and delete operation, refresh entire fsmap
                self._traverse()
                self._save()
            elif event_type in ['moved'] and dst_path is not None:
                # moved event includes 'rename' and 'relocate a folder'
                cp_key = ['db', 'file', 'fixed', 'group', 'last_sync', 'sep']

                self._traverse(True)
                if src_path in self._old_fsmap and dst_path in self.fsMap:
                    old_item = self._old_fsmap[src_path]
                    new_item = self.fsMap[dst_path]
                    for k, v in old_item.items():
                        if k in cp_key:
                            new_item[k] = v
                    del self._old_fsmap[src_path]
                else:
                    print('Error in handling DirMovedEvent: ', src_path, dst_path)

    def _db_key(self, _db, _col, _fs):
        _key = '{:s}::{:s}::{:s}'.format(_db, _col, _fs)
        return _key

    def _db_key_list(self, path, recursive, isUnique=False):
        _key_list = []
        def __recursive_db(_path, fsmap):
            if _path not in fsmap: return

            _db = fsmap[_path]['db']
            if _db is None: return

            _key = self._db_key(_db[0], _db[1], _db[2])
            if not isUnique:
                _key_list.append((_path, _key))
            else:
                if _key not in _key_list:
                    _key_list.append(_key)

            if recursive:
                for _c_path in fsmap[_path]['children']:
                    __recursive_db(_c_path, fsmap)
        __recursive_db(path, self.fsMap)
        return _key_list

    def _get_db_handler(self, db_col_fs):
        _db, _col, _fs = db_col_fs
        _key = self._db_key(_db, _col, _fs)
        if _key in self.clientPool:
            return self.clientPool[_key]
        else:
            _h = MultiViewMongo(
                connection=self.client,
                db_name=_db,
                collection_name=_col,
                fs_name=_fs
            )
            self.clientPool[_key] = _h
            return _h

    def _get_db_handler_by_key(self, key:str):
        if key in self.clientPool:
            return self.clientPool[key]
        else:
            tokens = key.split('::')
            _h = MultiViewMongo(
                connection=self.client,
                db_name=tokens[0],
                collection_name=tokens[1],
                fs_name=tokens[2]
            )
            self.clientPool[key] = _h
            return _h

    def _update_file(self, event_type, src_path, dst_path):
        """Invoked when files change
            By watchdog:
            By syncer:
        """
        if self.parser is None:
            print('parser is not set.')
            return None
        if dst_path is None:
            _path = src_path
            path, filename = os.path.split(src_path)
        else:
            _path = dst_path
            path, filename = os.path.split(dst_path)

        if len(filename) == 0:
            print('fail to detect filename.')
            return None

        ext = os.path.splitext(filename)[1]
        if len(ext) == 0 or ext not in self.extensions:
            print('Unsupported extension type. {:s}'.format(ext))
            return None

        if path not in self.fsMap:
            print("Path is not in fsmap. {:s}".format(path))
            return None
        if self.fsMap[path]['db'] is None:
            print("DB is not set on this path. {:s}".format(path))
            return None
        if self.fsMap[path]['group'] is None:
            print("Group name is not set to this path. {:s}".format(path))
            return None
        db = self.fsMap[path]['db']
        group = self.fsMap[path]['group']

        if event_type in ['created', 'modified', 'syncing', 'moved']:
            doc = self.parser.run(_path, ext, group)
            if doc is None:
                return None

            h = self._get_db_handler(db)
            if h.save_one(doc, ext) == 0:
                return None

            if ext == '.xml':
                query = {"sample": group, "item": doc['item']}
                res = h.load(query=query, fields={}, getarrays=False)
                res = self.after_query(res)
                return json.dumps(res)

        elif event_type in ['deleted']:
            # currently we do not delete any document in the db (should we?)
            pass
        else:
            # unknown event_type
            pass

        return None

    def _add_fs_event(self, what, event_type, src_path, dst_path):
        """Invoked by observer and syncers"""
        self.fs_event_q.put((what, event_type, src_path, dst_path))

    def get_fsmap_as_list(self):
        """
        Used to return the lastes file system information.
        Always, first scan file system itself to detect any changes made in
        the file system by someone else.
        """
        with self.fsmap_lock:
            self._traverse()
            fsmap_list = [[key, value] for key, value in self.fsMap.items() if value['valid']]
        return fsmap_list

    def set_fsmap(self, fsmap_list):
        """Used to set db config by a client"""
        with self.fsmap_lock:
            for path, value in fsmap_list:
                # path is not found
                # (can happen when file system is manually changed)
                if path not in self.fsMap: continue

                # db is already set by other clients, ignore this.
                # Only administrator can change this manually.
                if self.fsMap[path]['fixed']: continue

                # check db config a client set
                if value['db'] is None: continue         # db is not set
                if len(value['db']) != 3: continue      # must be 3-D array

                new_db = value['db'][0]
                new_col = value['db'][1]
                if len(new_db) == 0 or len(new_col) == 0: continue # in-complete setting
                if new_db == 'null' or new_col == 'null': continue # in-complete setting

                # update db config
                item = self.fsMap[path]
                item['db'] = [new_db, new_col, 'fs']
                item['fixed'] = True

            self._save()



    # def get_sync_samples(self, path, recursive):
    #     """
    #     This is called to initiate syncing operation.
    #     Args:
    #         path:
    #         recursive:
    #
    #     Returns:
    #
    #     """
    #     if path not in self.fsMap: return []
    #     if not os.path.exists(path): return []
    #
    #     sample_files = {}
    #     for dirpath, _, files in os.walk(path, followlinks=True):
    #         for f in files:
    #             name, ext = os.path.splitext(f)
    #             if ext in self.extensions:
    #                 sample_files[dirpath] = name
    #                 break
    #
    #         if not recursive: break
    #     return sample_files

    # def set_sync_info(self, info:dict):
    #     """update `inSync` and `sep` fields in fsmap"""
    #
    #     with self.fsmap_lock:
    #         responses = {}
    #         for path, sep in info.items():
    #             resp = {
    #                 'valid': Syncer.CAN_SYNC
    #             }
    #             if path in self.fsMap:
    #                 item = self.fsMap[path]
    #                 if item['inSync']:
    #                     resp['valid'] = Syncer.CANNOT_SYNC
    #                 elif item['db'] is None or len(item['db']) != 3:
    #                     resp['valid'] = Syncer.NO_DB
    #                 else:
    #                     item['inSync'] = True
    #                     item['sep'] = sep
    #             else:
    #                 resp['valid'] = Syncer.NO_PATH
    #             responses[path] = resp
    #
    #         self._save()
    #
    #     return responses

    # def run_syncer(self, resp:dict):
    #     """run syncer, some information will be added to resp"""
    #
    #     files_to_sync = []
    #     for path, info in resp.items():
    #         if info['valid']:
    #             item = {
    #                 'path': path,
    #                 'files': [],
    #                 'client': self.get_client(self.get_db(path))
    #             }
    #             for _, _, files in os.walk(path):
    #                 item['files'] = [f for f in files
    #                                  if os.path.splitext(f)[1] in self.extensions]
    #                 break
    #             files_to_sync.append(item)
    #             info['total'] = len(item['files'])
    #         else:
    #             info['total'] = 0
    #         info['progressed'] = 0
    #
    #     # create syncer
    #     syncer_id = Syncer.generate_syncer_id()
    #     #syncer = Syncer(items_to_sync=files_to_sync)
    #
    #     # update pool
    #     #self.syncerPool[syncer_id] = syncer
    #
    #     # run syncer
    #     #syncer.start()
    #
    #     return syncer_id, resp

    # def get_client(self, db_collection_fs):
    #     if db_collection_fs is None or len(db_collection_fs) != 3:
    #         return None
    #
    #     db = db_collection_fs[0]
    #     col = db_collection_fs[1]
    #     fs = db_collection_fs[2]
    #     key = '{}:{}:{}'.format(db, col, fs)
    #     if key in self.clientPool:
    #         h = self.clientPool[key]
    #     else:
    #         h = MultiViewMongo(
    #             connection=self.client,
    #             db_name=db,
    #             collection_name=col,
    #             fs_name=fs
    #         )
    #         self.clientPool[key] = h
    #     return h

    # def set_db(self, path, db, col):
    #     if path not in self.fsMap:
    #         return False
    #
    #     def __recursive_update(key: str, fsmap: dict):
    #         item = fsmap[key]
    #         if item['db'] is None: item['db'] = [db, col, 'fs']
    #         for child in item['children']:
    #             __recursive_update(child, fsmap)
    #
    #     # update db setting recursively
    #     # If a path is already set before (or maybe by other client),
    #     # it didn't modify it. Given path may be not set as a client wants.
    #     with self.fsmap_lock:
    #         __recursive_update(path, self.fsMap)
    #         self._save()
    #
    #     return True

    # def get_db(self, path):
    #     db = None
    #     with self.fsmap_lock:
    #         if path in self.fsMap:
    #             db = self.fsMap[path]['db']
    #     return db

    def after_query(self, res):
        """Post processor on queried results"""
        if not isinstance(res, list):
            res = [res]

        res = [replace_objid_to_str(doc) for doc in res]
        res = [flatten_dict(doc) for doc in res]
        # for doc in res:
        #     doc['sample'] = '[{:s}][{:s}]{:s}'.format(db, col, doc['sample'])
        #     doc['_id'] = '[{:s}][{:s}]{:s}'.format(db, col, doc['_id'])

        return res

    def get_samplelist(self, path, recursive):
        if path not in self.fsMap:
            return []

        samplelist = {}

        db_key_list = self._db_key_list(path, recursive)
        _db_list = self.client.list_database_names()
        for _path, _key in db_key_list:
            _db, _col, _fs = _key.split("::")

            if _db not in _db_list:
                continue

            _col_list = self.client[_db].collection_names()
            if _col not in _col_list:
                continue

            h = self._get_db_handler_by_key(_key)
            pipeline = [
                {"$match": {"path": _path}},
                {"$match": {"sample": {"$exists": True, "$ne": None}}},
                {"$group": {"_id": "$sample", "count": {"$sum": 1}}}
            ]
            res = list(h.collection.aggregate(pipeline))

            for r in res:
                _id = r['_id']
                _count = r['count']

                if _id in samplelist:
                    samplelist[_id] += _count
                else:
                    samplelist[_id] = _count

        return samplelist

    def get_samples(self, names, path, recursive):
        if path not in self.fsMap:
            return {}

        sampleData = {}
        db_key_list = self._db_key_list(path, recursive, False)
        _db_list = self.client.list_database_names()
        for _path, _key in db_key_list:
            _db, _col, _fs = _key.split("::")

            if _db not in _db_list:
                continue

            _col_list = self.client[_db].collection_names()
            if _col not in _col_list:
                continue

            h = self._get_db_handler_by_key(_key)
            for name in names:
                query = {"sample": name, "path": _path}
                res = h.load(query=query, fields={}, getarrays=False)

                if res is None:
                    continue

                res = self.after_query(res)

                if name in sampleData:
                    sampleData[name].append(res)
                else:
                    sampleData[name] = res
        return sampleData

    def get_tiff(self, id, path):
        if path not in self.fsMap:
            return []

        if self.fsMap[path]['db'] is None:
            return []

        db = self.fsMap[path]['db']
        h = self._get_db_handler(db)

        try:
            _id = ObjectId(id)
        except InvalidId:
            return []

        query = {'_id': _id, 'tiff': {'$exists': True}}
        fields = {'tiff': 1, '_id': 0}
        res = h.load(query, fields, getarrays=True)

        if res is None:
            return []

        data = res['tiff']['data']
        res['tiff']['data'] = data.tolist()
        return res['tiff']


class DBHandlerWithSyncer(DBHandler):
    """
    On top of DBHandler, implement syncer handler here.

    The communication between Web Server and a client is through this syncer
    handler. For the communication, following structured information is used.

    {
          'path/to/sync': {
              'status': int, index of one of [RUNNING, QUEUED, COMPLETED, ERROR],
              'path_name': str, path name to display,
              'file_name': str, sample file name to determine separator,
              'group_name': str, group name,
              'sep': str, separator (keyword),
              'total': int, the number of files to be synced,
              'processed': int, the number of files prossessed,
              'timestamp': date, last time this folder is synced
          }
    }
    """

    def __init__(self, rootDir, fsmapFn,
                 db_host='localhost', db_port=27017, xml_config=None):
        super().__init__(rootDir, fsmapFn, db_host, db_port, xml_config)
        self.syncerPool = {}

    def __del__(self):
        super().__del__()

    def _sync_files(self, path):
        """Return list of filenames under `path` (not recursive)"""
        if not os.path.exists(path):
            return []

        files_to_sync = []
        for dirpath, _, files in os.walk(path, followlinks=True):
            for f in files:
                name, ext = os.path.splitext(f)
                if ext in self.extensions:
                    files_to_sync.append(f)
            break

        return files_to_sync

    def _sync_file_sample(self, path):
        """Return a filename under `path' (not recursive)"""
        files = self._sync_files(path)
        if len(files):
            return files[0]
        return None

    def _update_sync_timestamp(self, path, timestamp):
        """Invoked as upon finishing syncing to update timestamp in fsmap"""
        with self.fsmap_lock:
            if path in self.fsMap:
                self.fsMap[path]['last_sync'] = timestamp
                self._save()

    def _sync_on_finished(self, path, info):
        """Call back function to update timestamp by a syncer"""
        dateFormat = "%Y-%m-%d %H:%M:%S"
        tstamp = datetime.datetime.now().strftime(dateFormat)
        info['timestamp'] = tstamp
        info['status'] = 'COMPLETED'
        self._update_sync_timestamp(path, tstamp)
        return info

    def _sync_request(self, path, item):
        """On request on syncing..."""
        files_to_sync = self._sync_files(path)
        h = Syncer(files_to_sync, path, item,
                   self._sync_on_finished,
                   self._add_fs_event)
        # before starting the syncer, update fsmap
        with self.fsmap_lock:
            fs = self.fsMap[path]
            fs['file'] = item['file_name']
            fs['sep'] = item['sep']
            fs['group'] = item['group_name']
            self._save()
        h.start()
        item['total'] = h.get_total()
        item['processed'] = h.get_processed()
        item['status'] = 'RUNNING'

        self.syncerPool[path] = h
        return item

    def _sync_running(self, path, item):
        """syncing monitoring"""
        if path in self.syncerPool:
            h = self.syncerPool[path]
            for k, v in h.get_info().items():
                item[k] = v
            if not h.isRunning:
                del self.syncerPool[path]
        else:
            item['status'] = 'COMPLETED'
        return item

    def _update_info_item(self, path, item:dict):
        """update sync. information according to the status"""
        status = item['status']
        if status == 'INIT' or status == 'COMPLETED':
            item = self._sync_request(path, item)
        elif status == 'RUNNING':
            item = self._sync_running(path, item)
        else:
            print('Unknown status: ', status)
        return item

    def get_sync_info(self, path, recursive):
        """
        Parse sync info from fsmap and syncer pool
        It only read information from them, no lock. (thread safe)
        """
        if path not in self.fsMap: return {}

        syncInfo = {}
        q = Queue()
        q.put(path)

        while not q.empty():
            _path = q.get()
            if _path not in self.fsMap: continue

            _fs = self.fsMap[_path]
            if _path in self.syncerPool:
                syncInfo[_path] = self.syncerPool[_path].get_info()
            else:
                db = _fs['db']
                file = _fs['file']
                if file is None: file = self._sync_file_sample(_path)

                if db is not None and file is not None:
                    syncInfo[_path] = {
                        'status': 'INIT',
                        'path_name': _fs['name'],
                        'file_name': file,
                        'group_name': _fs['group'],
                        'sep': _fs['sep'],
                        'total': 0,
                        'processed': 0,
                        'timestamp': _fs['last_sync']
                    }

            if recursive:
                for c_path in _fs['children']:
                    q.put(c_path)

        return syncInfo

    def update_sync_info(self, syncInfo:dict):
        for path, info in syncInfo.items():
            self._update_info_item(path, info)
        return syncInfo

class DataHandler(DBHandlerWithSyncer):
    """
    DataHandler
    """
    def __init__(
            self,
            rootDir,
            fsMapFn='./fsmap.json',
            db_host='localhost',
            db_port=27017,
            xml_config=None
    ):
        super().__init__(
            os.path.realpath(os.path.abspath(rootDir)),
            os.path.abspath(fsMapFn),
            db_host,
            db_port,
            xml_config
        )

        # watchdog
        self.observer = Observer()
        self.observer.schedule(
            FSHandler(
                callback=self._add_fs_event,
                extensions=self.extensions,
                skip_dirs=[],
                skip_filenames=[]
            ),
            self.rootDir, recursive=True)
        self.observer.start()

        # thread to handle fs event
        self.fs_thread = threading.Thread(target=self._fs_process, name='fs_thread')
        self.fs_thread.daemon = True
        self.fs_thread.start()


    def __del__(self):
        super().__del__()
        self.observer.stop()
        self.observer.join()

    def _fs_process(self):
        """target function of self.fs_thread (daemon, background thread)"""
        while True:
            e = self.fs_event_q.get()
            what, event_type, src_path, dst_path = e

            # based on the event,
            if what == 'dir':
                # If directory event... update fsmap...
                # No need for streaming...
                self._update_fsmap(event_type, src_path, dst_path)
            elif what == 'file' or what == 'sync':
                # If file event... update database...
                # add to streaming queue
                resp = self._update_file(event_type, src_path, dst_path)
                if resp is not None and len(resp):
                    self.stream_q.put(resp)
            else:
                pass
            # if file event... update database...
            # [NOTE]: symlink comes with the absolute path!
            time.sleep(0.001)

    def get_dataframe(self):
        class DataFrame(BaseDataFrame):
            @staticmethod
            def frames():
                while True:
                    # need to fix
                    f = self.stream_q.get()
                    yield f
        return DataFrame()






























