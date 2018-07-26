import os
from threading import Thread
from time import sleep
from uuid import uuid4

class Syncer(object):
    def __init__(self, parser, items_to_sync):
        self.items_to_sync = items_to_sync
        self.parser = parser

        self.processed = 0
        self.total = 0
        self.isRunning = False
        self.isFinished = False
        self.t = None

    def start(self):
        self.isRunning = True
        self.isFinished = False
        self.processed = 0
        self.total = 0

        for item in self.items_to_sync:
            self.total += len(item['files'])

        self.t = Thread(target=self._process)
        self.t.start()

    def stop(self):
        if self.isRunning:
            self.isRunning = False
            if self.t is not None:
                while self.t.is_alive():
                    sleep(0.001)
                self.t = None

    def get_stat(self):
        return self.total, self.processed, self.isFinished

    def get_items_to_sync(self):
        return self.items_to_sync

    def _process(self):
        """
        item = {
            'path': path to directory,
            'files': list of filenames in the directory
            'client': MongoDB client
        }
        :return:
        """
        for item in self.items_to_sync:
            #print(self.isRunning, item)
            if not self.isRunning:
                break

            path_to_dir = item['path']
            filenames = item['files']
            db = item['client']

            for f in filenames:

                if not self.isRunning:
                    break

                fileExt = os.path.splitext(f)[1]
                fullpath = os.path.join(path_to_dir, f)

                if fileExt == '.xml':
                    doc = self.parser.xml_to_doc(fullpath)
                    db.save_doc_one(doc)
                elif fileExt == '.tiff':
                    doc = self.parser.tiff_to_doc(fullpath)
                    db.save_img_one(doc, 'tiff')
                elif fileExt == '.jpg':
                    doc = self.parser.jpg_to_doc(fullpath)
                    db.save_img_one(doc, 'jpg')
                else:
                    print('Unsupported file: ', fullpath)
                #print('SYNC: ', fullpath)

                self.processed += 1

                if self.processed%100 == 0:
                    sleep(0.001)

        self.isRunning = False
        self.isFinished = True


class syncerGroup(object):
    def __init__(self, parser):
        self.parser = parser
        self.syncerPool = {}

    def _generate_syncer_id(self):
        syncer_id = uuid4()
        while syncer_id in self.syncerPool:
            syncer_id = uuid4()
        return str(syncer_id)

    def init_syncer(self, items_to_sync):
        syncer_id = self._generate_syncer_id()
        h = Syncer(self.parser, items_to_sync)
        self.syncerPool[syncer_id] = h
        return syncer_id, h

    def get_syncer(self, syncer_id):
        if syncer_id is None or syncer_id not in self.syncerPool:
            return None
        return self.syncerPool[syncer_id]

    def delete_syncer(self, syncer_id):
        h = self.get_syncer(syncer_id)
        if h is not None:
            h.stop()
            del self.syncerPool[syncer_id]