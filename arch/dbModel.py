import os
import json
import pymongo
from pymongo.errors import ConnectionFailure
from db.multiviewmongo import MultiViewMongo

class dbModel(object):
    def __init__(
            self,
            rootDir: str,
            filesystemMapFn='./fsMap.json',
            hostname='localhost',
            port=27017
    ):
        self.rootDir = os.path.abspath(rootDir)
        self.fsMapFn = os.path.abspath(filesystemMapFn)
        self.host = hostname
        self.port = port
        self.supported_ext = ['.tiff', '.jpg', '.xml']

        # establish MongoDB connection
        self.client = None  # MongoDB client
        self._connect()

        # initialize fsMap (map from filesystem to db)
        self.fsMap = self._load_map()
        self._traverse_root()
        self._save_map()

        # db client pool,
        # key = db_name:collection_name:fs_name
        # value = db cursor
        self.clientPool = {}

    def __del__(self):
        if self.client is not None:
            self.client.close()

    def _connect(self):
        self.client = pymongo.MongoClient(self.host, self.port)
        try:
            self.client.list_database_names()
        except ConnectionFailure:
            exit("MongoDB server is not available")
        finally:
            print("MongoDB server @ {}:{}".format(self.host, self.port))

    def _load_map(self):
        if not os.path.exists(self.fsMapFn): return {}

        try:
            with open(self.fsMapFn) as f:
                data = json.load(f)
        except (FileNotFoundError, TypeError, json.decoder.JSONDecodeError) as e:
            print('[WARN] Fail to load saved fsmap!')
            print('[WARN] Previous fsmap will be ignored, if there are.')
            return {}

        def __recursive_flatten_fsmap(fsmap:dict, flattened:dict):
            item = dict(fsmap)
            item['children'] = [__recursive_flatten_fsmap(child, flattened) for child in item['children']]
            flattened[item['path']] = item

        t = {}
        for key, value in data.items():
            __recursive_flatten_fsmap(value, t)

        return t

    def _save_map(self):
        t = {}
        p_keys = [key for key, value in self.fsMap.items() if value['parent'] is None]

        def __convert_to_hierarchical_format(key: str, fsmap: dict):
            item = dict(fsmap[key])
            item['children'] = [__convert_to_hierarchical_format(c, fsmap) for c in item['children']]
            return item
        for key in p_keys: t[key] = __convert_to_hierarchical_format(key, self.fsMap)

        with open(self.fsMapFn, 'w') as f:
            json.dump(t, f, indent=2, sort_keys=True)

    def _traverse_root(self):
        fsmap = {}
        for dirpath, _, _ in os.walk(self.rootDir):
            path = dirpath.replace(self.rootDir, '')
            tokens = path.split(os.sep)[1:]
            parent_path = os.path.join(self.rootDir, *tokens[:-1])
            if len(path) == 0:
                fsmap[dirpath] = {
                    'path': dirpath, # absolute path to current directory
                    'name': dirpath, # name of current directory for display
                    'children': [],  # list of absolute pathes of direct children directories
                    'parent': None,  # absolute path to direct parent directory
                    'db': None,      # related database (db, collection)
                    'valid': True,
                    'inSync': False,
                }
            else:
                fsmap[parent_path]['children'].append(dirpath)
                fsmap[dirpath] = {
                    'path': dirpath,
                    'name': os.path.basename(path),
                    'children': [],
                    'parent': fsmap[parent_path]['path'],
                    'db': None,
                    'valid': True,
                    'inSync': False
                }

        def __merge_fsmap(dstMap:dict, srcMap:dict):
            for key, srcItem in srcMap.items():
                if key in dstMap:
                    # Is parent same? yes, it must be same as key is the absolute path.
                    # But children could be different. For example, one might delete/move/add
                    # sub-directories. But, we do not care, here.
                    dstItem = dstMap[key]
                    dstItem['db'] = srcItem['db']
                    dstItem['valid'] = srcItem['valid']
                    dstItem['inSync'] = srcItem['inSync']
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

    def refresh_map(self):
        self._traverse_root()
        self._save_map()

    def get_fsmap_as_list(self):
        return [[key, value] for key, value in self.fsMap.items() if value['valid']]

    def update_db(self, path, db, col):
        if path not in self.fsMap:
            return False

        def __recursive_update(key:str, fsmap:dict):
            item = fsmap[key]
            if item['db'] is None: item['db'] = [db, col, 'fs']
            for child in item['children']:
                __recursive_update(child, fsmap)
        __recursive_update(path, self.fsMap)
        self._save_map()
        return True

    def get_sample_list(self, db, col):
        pipeline = [
            {"$match": {"sample": {"$exists": True, "$ne": None}}},
            {"$group": {"_id": "$sample", "count": {"$sum": 1}}}
        ]
        return list(self.client[db][col].aggregate(pipeline))

    def get_client(self, db_collection_fs):
        if db_collection_fs is None:
            return None

        db = db_collection_fs[0]
        col = db_collection_fs[1]
        fs = db_collection_fs[2]
        key = '{}:{}:{}'.format(db, col, fs)
        if key in self.clientPool:
            h = self.clientPool[key]
        else:
            h = MultiViewMongo(
                connection=self.client,
                db_name=db,
                collection_name=col,
                fs_name=fs
            )
            self.clientPool[key] = h
        return h

    def get_files_to_sync(self, wdir):
        """
        Return list of files (fullpath) to be processed by a syncer.
        Files considered here includes `wdir` and all sub-directories, recursively.
        It will skip folders where `db` is not defined.
        """
        if wdir not in self.fsMap:
            return []

        f = []
        for dirpath, _, files in os.walk(wdir):
            if dirpath not in self.fsMap or \
                self.fsMap[dirpath]['db'] is None or \
                self.fsMap[dirpath]['inSync']:
                continue

            self.fsMap[dirpath]['inSync'] = True

            item = {
                'path': dirpath,
                'files': [file for file in files if os.path.splitext(file)[1] in self.supported_ext],
                'client': self.get_client(self.fsMap[dirpath]['db'])
            }
            f.append(item)
        self._save_map()
        return f

    def unlock_files_to_sync(self, files_to_sync):
        for item in files_to_sync:
            dirpath = item['path']

            if dirpath not in self.fsMap:
                continue

            self.fsMap[dirpath]['inSync'] = False
        self._save_map()



if __name__ == '__main__':
    m = dbModel(
        rootDir='/Users/scott/Desktop/test2'
    )

    f = m.get_files_to_sync('/Users/scott/Desktop/test2/a')

    import pprint
    pp = pprint.PrettyPrinter(indent=2)
    pp.pprint(f)










