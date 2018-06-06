from bson.binary import Binary
from bson.objectid import ObjectId
import gridfs
import pymongo
from pymongo import ReturnDocument
import pickle
import numpy as np
import datetime

import copy


__all__ = ['MultiViewMongo']


class MultiViewMongo(object):

    def __init__(self, db_name, collection_name, hostname='localhost', port=27017, username="", password=""):
        self.db_name = db_name
        self.collection_name = collection_name
        self.hostname = hostname
        self.port = port

        self.connection = pymongo.MongoClient(hostname, port)
        #if (username != ""):
        #    admin_db = self.connection["admin"]
        #    admin_db = admin_db.authenticate(username, password)

        self.db = self.connection[self.db_name]
        self.fs = gridfs.GridFS(self.db, 'fs')

        self.collection = self.db[collection_name]

        print('Run mongdo database')
        print('{}.{} @ {}:{}'.format(self.db_name, self.collection_name, self.hostname, self.port))

    def _close(self):
        print('Close mongdo database')
        print('{}.{} @ {}:{}'.format(self.db_name, self.collection_name, self.hostname, self.port))
        self.connection.close()

    def __del__(self):
        self._close()

    def update(self):
        self.collection.update()

    # core methods. load(), save(), delete()
    def save(self, document):

        # simplify thins below by making even a single document a list
        if not isinstance(document, list):
            document = [document]

        id_values = []
        for doc in document:
            docCopy = copy.deepcopy(doc)

            # make a list of any existing referenced gridfs files
            try:
                self.temp_oldNpObjectIDs = docCopy['_npObjectIDs']
            except KeyError:
                self.temp_oldNpObjectIDs = []

            self.temp_newNpObjectIds = []
            # replace np arrays with either a new gridfs file or a reference to the old gridfs file
            docCopy = self._stashNPArrays(docCopy)

            docCopy['_npObjectIDs'] = self.temp_newNpObjectIds
            doc['_npObjectIDs'] = self.temp_newNpObjectIds

            # cleanup any remaining gridfs files (these used to be pointed to by document, but no longer match any
            # np.array that was in the db
            for id in self.temp_oldNpObjectIDs:
                self.fs.delete(id)
            self.temp_oldNpObjectIDs = []

            # add insertion date field to every document
            docCopy['insertion_date'] = datetime.datetime.now()
            doc['insertion_date'] = datetime.datetime.now()

            # insert into the collection and restore full data into original document object
            new_id = self.collection.save(docCopy)
            doc['_id'] = new_id
            id_values.append(new_id)

        return id_values

    def save_doc_one(self, doc, type='doc'):
        """
        insert new document.
        if it exsits in the database, replace existing fields.
        :param doc: document
        :param type: one of ['doc', 'tiff', 'jpg']
        :return: previous document (None if there is no previous one)
        """
        item = doc['item']
        r = self.collection.find_one_and_update(
            {'item': item},
            {'$set': doc},
            upsert=True,
            return_document=ReturnDocument.BEFORE
        )
        #print('[save_doc_one]: ', r, item)
        return r

    def save_img_one(self, doc, type='tiff'):
        """
        insert new image document
        :param doc: image document
        :param type: one of ['tiff', 'jpg']
        :return: previous document (None if there is no previous one)
        """
        docCopy = copy.deepcopy(doc)

        # make a list of any existing referenced gridfs files
        # there are no old IDs... always treat it as new
        self.temp_oldNpObjectIDs = []
        self.temp_newNpObjectIds = []
        # replace np arrays with either a new gridfs file or
        # a reference to the old gridfs file
        docCopy = self._stashNPArrays(docCopy)

        # cleanup any remaining gridfs files
        # (these used to be pointed to by document,
        # but no longer match any np.array that was in the db)
        for id in self.temp_oldNpObjectIDs:
            self.fs.delete(id)
        self.temp_oldNpObjectIDs = []

        r = self.save_doc_one(docCopy, type)
        # delete old image data, if there is
        if r is not None:
            try:
                old_img_doc = r[type]
            except KeyError:
                old_img_doc = None
            if old_img_doc is not None:
                self.fs.delete(old_img_doc['data'])
        return r


    def loadFromIds(self, Ids):

        if type(Ids) is not list:
            Ids = [Ids]

        out = []

        for id in Ids:
            obj_id = id
            if type(id) is ObjectId:
                obj_id = id
            elif type(id) is str:
                obj_id = ObjectId(id)
            out.append(self.load({'_id': obj_id}))

        return out

    def load(self, query, fields, getarrays=False):
        if not fields:
            results = self.collection.find(query)
        else:
            results = self.collection.find(query, fields)

        if getarrays:
            allResults = [self._loadNPArrays(doc) for doc in results]
        else:
            allResults = [doc for doc in results]

        if allResults:
            if len(allResults) > 1:
                return allResults
            elif len(allResults) == 1:
                return allResults[0]

        return None

    def distinct(self, key, doc_filter={}):
        if not isinstance(key, str):
            key = str(key)

        return self.collection.distinct(key, doc_filter)

    def delete(self, objectId):
        documentToDelete = self.collection.find_one({"_id": objectId})

        npObjectIdsToDelete = []
        if 'jpg' in documentToDelete:
            npObjectIdsToDelete.append(documentToDelete['jpg']['data'])
        if 'tiff' in documentToDelete:
            npObjectIdsToDelete.append(documentToDelete['tiff']['data'])

        for npObjectID in npObjectIdsToDelete:
            self.fs.delete(npObjectID)
        self.collection.remove(objectId)

    # utility functions
    def _npArray2Binary(self, npArray):
        return Binary(pickle.dumps(npArray, protocol=2), subtype=128)

    def _binary2npArray(self, binary):
        return pickle.loads(binary)

    def _loadNPArrays(self, document):
        for (key, value) in document.items():
            if isinstance(value, ObjectId) and key != '_id':
                document[key] = self._binary2npArray(self.fs.get(value).read())
            elif isinstance(value, dict):
                document[key] = self._loadNPArrays(value)

        return document

    # modifies in place
    def _stashNPArrays(self, document):
        for (key, value) in document.items():
            if isinstance(value, np.ndarray):
                dataBSON = self._npArray2Binary(value)
                #dataMD5 = hashlib.md5(dataBSON).hexdigest()

                match = False
                for obj in self.temp_oldNpObjectIDs:
                    match = True
                    document[key] = obj
                    self.temp_oldNpObjectIDs.remove(obj)
                    self.temp_newNpObjectIds.append(obj)

                if not match:
                    obj = self.fs.put(self._npArray2Binary(value))
                    document[key] = obj
                    self.temp_newNpObjectIds.append(obj)

            elif isinstance(value, dict):
                document[key] = self._stashNPArrays(value)

            elif isinstance(value, (int, float)):
                if isinstance(value, int):
                    document[key] = int(value)
                elif isinstance(value, float):
                    document[key] = float(value)

            elif isinstance(value, ObjectId):
                document[key] = value

        return document


# if __name__ == "__main__":
#     db = MultiViewMongo('test', 'test_collection')
#
#     test_doc = dict()
#
#     proto_doc = dict()
#     proto_doc['a'] = 1
#     proto_doc['b'] = 2
#
#     proto_doc2 = dict()
#     proto_doc2['a'] = 1
#     proto_doc2['b'] = 2
#
#     image_doc = dict()
#     image_doc['width'] = 10
#     image_doc['height'] = 10
#     image_doc['data'] = np.array([2,3,1,0])
#
#
#
#     test_doc['DataFile'] = 'file name'
#     test_doc['proto_a'] = proto_doc
#     test_doc['proto_b'] = proto_doc2
#     test_doc['image'] = image_doc
#
#     db.save(test_doc)




































