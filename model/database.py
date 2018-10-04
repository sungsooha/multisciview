import pymongo
import gridfs
import numpy as np
import pickle
from bson.errors import InvalidId
from bson.binary import Binary
from bson.objectid import ObjectId

class DataBase(object):
    def __init__(self, host='localhost', port=27017):
        # MongoDB host name
        self.host = host
        # MongoDB port number
        self.port = port
        # MongoDB connection
        self.conn = pymongo.MongoClient(host, port)

    def __del__(self):
        self.conn.close()

    def get_db(self, db, col, fs='fs'):
        """Get collection cursor and associated gridfs cursor"""
        _db = self.conn[db]
        _col = _db[col]
        _fs = gridfs.GridFS(_db, fs)
        return _col, _fs

def save_document(colCursor, doc:dict):
    """
    Insert new document.
    If it exsits in the database, replace existing fields.
    Args:
        colCursor: cursor to a collection
        doc: document

    Returns:
        previous document (None if there is no previous one)
    """
    item = doc['item']
    res = colCursor.find_one_and_update(
        {'item': item},
        {'$set': doc},
        upsert=True,
        return_document=pymongo.ReturnDocument.BEFORE
    )
    return res

def save_image_document(colCursor, fsCursor, doc:dict, type:str):
    def _npArray2Binary(_arr):
        return Binary(pickle.dumps(_arr, protocol=2), subtype=128)

    def _stashNPArrays(_doc:dict):
        """stash np array, in-place modification"""
        for (key, value) in _doc.items():
            if isinstance(value, np.ndarray):
                id = fsCursor.put(_npArray2Binary(value))
                _doc[key] = id

            elif isinstance(value, dict):
                _doc[key] = _stashNPArrays(value)

        return _doc

    # stash np arrays
    _stashNPArrays(doc)

    # add to db
    prev = save_document(colCursor, doc)

    # delete old image data, if any
    if prev is not None:
        try:
            old_img_doc = prev[type]
        except KeyError:
            old_img_doc = None
        if old_img_doc is not None:
            fsCursor.delete(old_img_doc['data'])

    return prev

def load(colCursor, query, fields=None, fsCursor=None):
    def _binary2NPArray(_binary):
        return pickle.loads(_binary)

    def _loadNPArrays(_doc:dict):
        for (key, value) in _doc.items():
            if isinstance(value, ObjectId) and key != '_id':
                _doc[key] = _binary2NPArray(fsCursor.get(value).read())
            elif isinstance(value, dict):
                _doc[key] = _loadNPArrays(value)
        return _doc

    if fields is None:
        results = colCursor.find(query)
    else:
        results = colCursor.find(query, fields)

    if fsCursor is None:
        results = [doc for doc in results]
    else:
        results = [_loadNPArrays(doc) for doc in results]

    return results

def load_xml(colCursor, sample_name, project_name=None):
    if project_name is None:
        query = {"sample": sample_name}
    else:
        query = {"sample": sample_name, "project": project_name}

    fields = {'tiff': 0, 'jpg': 0}
    results = load(colCursor, query, fields)
    return results

def load_image(colCursor, fsCursor, id, type):
    try:
        _id = ObjectId(id)
    except InvalidId:
        return []

    query = {'_id': _id, type: {'$exists': True}}
    fields = {type: 1, '_id': 0}
    result = load(colCursor, query, fields, fsCursor)

    return result

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

def after_query(res):
    """Post processing on queried results"""
    if not isinstance(res, list):
        res = [res]

    res = [replace_objid_to_str(doc) for doc in res]
    res = [flatten_dict(doc) for doc in res]

    return res


if __name__ == '__main__':
    from config import CONFIG
    from model.parser import Parser
    import pprint
    import os

    parser = Parser(config=CONFIG['XML'])
    DB = DataBase(
        host=CONFIG['DB']['HOST'],
        port=CONFIG['DB']['PORT']
    )
    pp = pprint.PrettyPrinter(indent=4)

    colCursor, fsCursor = DB.get_db('test_db', 'test_col')

    data_dir = [
        '/Users/scott/Desktop/data/saxs/analysis_proper/results/',
        '/Users/scott/Desktop/data/saxs/analysis_proper/thumbnails',
        '/Users/scott/Desktop/data/saxs/tiff'
    ]

    test_files = [
        'C67_GD2-69-6_th0.110_1929.1s_T200.006C_5.00s_61288_saxs.xml',
        'C67_GD2-69-6_th0.110_1929.1s_T200.006C_5.00s_61288_saxs.jpg',
        'C67_GD2-69-6_th0.110_1929.1s_T200.006C_5.00s_61288_saxs.tiff'
    ]

    print('Add data to DB')
    for dir, file in zip(data_dir, test_files):
        print(file)

        ext = os.path.splitext(file)[1][1:]

        doc = parser.run(
            os.path.join(dir, file),
            kind=ext,
            sample_name='test_sample',
            project_name='test_project'
        )

        if ext == 'xml':
            save_document(colCursor, doc)
        else:
            save_image_document(colCursor, fsCursor, doc, ext)

    print('Retrieve xml data')
    xml = load_xml(colCursor, 'test_sample', 'test_project')
    pp.pprint(xml)

    print('Retrieve jpg data')
    jpg = load_image(colCursor, fsCursor, "5bad2bdcd511eb8fef5ccd3f", 'jpg')
    pp.pprint(jpg)

    print('Retrieve tiff data')
    tiff = load_image(colCursor, fsCursor, "5bad2bdcd511eb8fef5ccd3f", 'tiff')
    pp.pprint(tiff)








