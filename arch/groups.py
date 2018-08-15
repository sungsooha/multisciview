import os
import pymongo
from pymongo.errors import ConnectionFailure
from db.multiviewmongo import MultiViewMongo
from db.watcher import Watcher
from db.syncer import Syncer

from queue import Queue
from uuid import uuid4


def list_files(startpath):
    summary = {}
    count = 0
    for dirpath, dirnames, filenames in os.walk(startpath, followlinks=True):
        dirpath = dirpath.replace(startpath,'')

        xml_count = 0
        jpg_count = 0
        tif_count = 0
        for f in filenames:
            if f.endswith('.xml'):
                xml_count += 1
            elif f.endswith('.jpg'):
                jpg_count += 1
            elif f.endswith('.tiff'):
                tif_count += 1

        tokens = dirpath.split(os.sep)[1:]
        parent_path = '/root'
        if len(tokens) > 1:
            parent_path = '/'
            for token in tokens[:-1]:
                parent_path += token + '/'
            parent_path = parent_path[:-1]

        node_id = 'N{:d}'.format(count)
        if len(dirpath) == 0:
            dirpath = '/root'
            summary[dirpath] = {
                'id': node_id,
                'name': os.path.basename(dirpath),
                'children': [],
                'count': [xml_count, jpg_count, tif_count],
                'path': dirpath
            }
        else:
            summary[parent_path]['children'].append(node_id)
            summary[dirpath] = {
                'id': node_id,
                'name': os.path.basename(dirpath),
                'children': [],
                'parent': summary[parent_path]['id'],
                'count': [xml_count, jpg_count, tif_count],
                'path': '/root' + dirpath
            }
        count += 1
    return summary

class dbGroup(object):
    """
    Handle multiple connection to a MongoDB Server

    - Create a global connection to the MongoDB server.
    - Keep cursors to db.collection based on (db, collection, fs)
    - Manage mapping from database to folders such that
        db_name:collection_name --> [list of folder name]

    [RULE 1]
    For example, if parent folder is mapped to db1:col1,
    its children must be mapped to same databse (db1:col1).
    This is because of the mechanism how it monitors (or syncronizing) filesystem
    (or data), recursively.
    """
    def __init__(
            self,
            rootDir: str,
            dirTreeMapFn='./dirTreeMap.json',
            hostname='localhost',
            port=27017
    ):
        self.rootDir = os.path.abspath(rootDir)
        self.dirTreeMapFn = os.path.abspath(dirTreeMapFn)
        self.hostname = hostname
        self.port = port

        self.connection = pymongo.MongoClient(self.hostname, self.port)
        try:
            self.connection.list_database_names()
        except ConnectionFailure:
            exit("MongoDB server is not available.")
        finally:
            print("Get connection to MongoDB server @ {}:{}".format(self.hostname, self.port))

        self.connectionPool = {}
        self.rootDirMap = {}

        # initialize treemap
        # 1. load from the file (flattened version)
        # 2. traverse directory (flattened version)
        # 3. merge
        self._traverse_rootDir()
        self._load_treemap()
        self._save_treemap()

    def __del__(self):
        """
        Before destroying..
        1. close the connection to the MongoDB server
        2. save treemap
        """
        self._close()

    def _close(self):
        print('Close MongoDB server connection')
        self.connection.close()

    def _traverse_rootDir(self):
        """Recursively traverse root directory to create initial mapping between directory to database"""
        rootDir = self.rootDir
        dirMap = self.rootDirMap

        for dirpath, _, _ in os.walk(rootDir):

            path = dirpath.replace(rootDir, '')
            tokens = path.split(os.sep)[1:]

            parent_path = os.path.join(rootDir, *tokens[:-1])

            if len(path) == 0:
                dirMap[dirpath] = {
                    'path': dirpath,
                    'name': dirpath,
                    'children': [],
                    'parent': None,
                    '__db': [None, None, None],
                }
            else:
                dirMap[parent_path]['children'].append(dirpath)
                dirMap[dirpath] = {
                    'path': dirpath,
                    'name': os.path.basename(path),
                    'children': [],
                    'parent': dirMap[parent_path]['path'],
                    '__db': [None, None, None]
                }

    def _load_treemap(self):
        """
        Load treemap and combine with current one

        Note
            1. treemap in the file is in a hierarchical format, and thus it first needs to be flattened.
        """
        import json
        try:
            with open(self.dirTreeMapFn) as f:
                data = json.load(f)
        except (FileNotFoundError, TypeError) as e:
            print('[WARN] Ignore previous treemap: ', e)
            return
        except json.decoder.JSONDecodeError as e:
            print('[WARN] Ignore previous treemap: ', e)
            return

        def __recursive_flatten_treemap(treemap:dict, flattened:dict):
            item = dict(treemap)
            key = treemap['path']
            children = []
            for child in item['children']:
                __recursive_flatten_treemap(child, flattened)
                children.append(child['path'])

            item['children'] = children
            flattened[key] = item

        t = {}
        for key, value in data.items():
            __recursive_flatten_treemap(value, t)

        # merge map from file to current one
        # Note that it cannot detect any changes in the filesystem at this moment.
        # Thus, if filesystem has been changed manually before launching the app,
        # previous information will be ignored.
        for key, value in t.items():
            self.rootDirMap[key] = value

    def _save_treemap(self):
        """
        Save treemap
        - convert to hierarchical format
        """
        t = {}

        parent_keys = [key for key, value in self.rootDirMap.items() if value['parent'] is None]

        def __convert_to_hierarchical_format(key: str, dirMap: dict):

            item = dict(dirMap[key])
            children_keys = item['children']

            children = []
            for key in children_keys:
                children.append(__convert_to_hierarchical_format(key, dirMap))

            item['children'] = children
            return item

        for p_key in parent_keys:
            t[p_key] = __convert_to_hierarchical_format(p_key, self.rootDirMap)

        import json
        with open(self.dirTreeMapFn, 'w') as f:
            json.dump(t, f, indent=4, sort_keys=True)

    def _update_treemap(self, path, db):
        if path not in self.rootDirMap:
            return

        q = Queue()
        q.put(path)
        while not q.empty():
            _path = q.get()
            item = self.rootDirMap[_path]
            __db = item['__db']
            if __db[0] is None:
                item['__db'] = list(db)

            for child in item['children']:
                q.put(child)

        self._save_treemap()

    def get_treemap(self):
        return self.rootDirMap

    def set_db_col(self, path, db, col, fs='fs'):
        if path not in self.rootDirMap:
            return None

        item = self.rootDirMap[path]
        __db = item['__db']
        if __db[0] is not None and __db[1] is not None and __db[2] is not None:
            return __db

        all_db_names = self.get_db_names()
        new_db_name = db
        count = 1
        while new_db_name in all_db_names:
            new_db_name = '{:s}_{:d}'.format(db, count)

        all_col_names = self.get_col_names(new_db_name)
        new_col_name = col
        count = 1
        while new_col_name in all_col_names:
            new_col_name = '{:s}_{:d}'.format(col, count)

        self._update_treemap(path, [new_db_name, new_col_name, fs])
        #self._update_log(path)
        #self._save_treemap()
        return [new_db_name, new_col_name, fs]


    def get_db_names(self):
        """Get a list of DB names used in the database"""
        return [name for name in self.connection.list_database_names()]

    def get_col_names(self, db):
        """Get a list of Collection names used in the database"""
        return [name for name in self.connection[db].collection_names() if 'fs.' not in name]

    def get_sample_names(self, db, col):
        pipeline = [
            {
                "$match": {
                    "sample": {"$exists": True, "$ne": None}
                }
            },
            {
                "$group": {
                    "_id": "$sample",
                    "count": {"$sum": 1}
                }
            }
        ]
        res = list(self.connection[db][col].aggregate(pipeline))
        return res

    def open(self, db, col, fs="fs"):
        key = '{}:{}:{}'.format(db, col, fs)
        if key in self.connectionPool:
            h = self.connectionPool[key]
        else:
            h = MultiViewMongo(
                connection=self.connection,
                db_name=db,
                collection_name=col,
                fs_name=fs
            )
            self.connectionPool[key] = h
        return h


class watcherGroup(object):
    """
    Watcher group based on working directy to monitor.

    """
    def __init__(self, root_dir, parser):
        self.root_dir = root_dir
        self.parser = parser
        self.watcherPool = {}

    def stop(self):
        for _, h in self.watcherPool.items():
            h.stop()

    def get_dir_list(self, wdir='/root'):
        key = str(wdir).replace('/root', '')
        if len(key) == 0:
            key = '/root'

        # as the structure can change at any time,
        # we call list_files as upon calling this function.
        dirDict = list_files(self.root_dir)
        nodeid = dirDict[key]['id']
        dirList = [[value['id'], value] for _, value in dirDict.items()]
        return {
            'dirList': dirList,
            'nodeid': nodeid
        }

    def get_watcher(self, wdir, h_db):
        db = h_db.db_name
        col = h_db.collection_name

        key = '{}:{}:{}'.format(db, col, wdir)

        if key in self.watcherPool:
            h = self.watcherPool[key]
        else:
            dir_to_watch = str(wdir).replace('/root', self.root_dir)
            h = Watcher(dir_to_watch, h_db, self.parser)
            self.watcherPool[key] = h
        return h

    def get_watcher_only(self, wdir, db, col):
        key = '{}:{}:{}'.format(db, col, wdir)
        if key in self.watcherPool:
            return self.watcherPool[key]
        return None

    def delete_watcher(self, wdir, db, col):
        key = '{}:{}:{}'.format(db, col, wdir)
        if key in self.watcherPool:
            del self.watcherPool[key]

class syncerGroup(object):
    """syncer group"""
    def __init__(self, root_dir, parser):
        self.root_dir = root_dir
        self.parser = parser
        self.syncerPool = {}

    def get_syncer(self, wdir, h_db):
        db = h_db.db_name
        col = h_db.collection_name
        key = '{}:{}:{}'.format(db, col, wdir)

        # syncer may be in running although it is not sure
        # who initiate the syncer.
        if key in self.syncerPool:
            return self.syncerPool[key]

        dir_to_sync = str(wdir).replace('/root', self.root_dir)
        h = Syncer(dir_to_sync, h_db, self.parser)
        self.syncerPool[key] = h
        return h

    def get_syncer_only(self, wdir, db, col):
        key = '{}:{}:{}'.format(db, col, wdir)

        if key in self.syncerPool:
            return self.syncerPool[key]
        return None

    def delete_syncer(self, wdir, db, col):
        key = '{}:{}:{}'.format(db, col, wdir)
        if key in self.syncerPool:
            del self.syncerPool[key]


if __name__ == '__main__':
    from db.db_config import MONGODB_CONFIG

    g_dbs = dbGroup(
        rootDir=MONGODB_CONFIG['DATA_DIR'],
        dirTreeMapFn= './testTreeMap.json',#MONGODB_CONFIG['DIR_TREEMAP'],
        hostname=MONGODB_CONFIG['DB']['HOST'],
        port=MONGODB_CONFIG['DB']['PORT']
    )








