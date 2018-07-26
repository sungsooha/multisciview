import os
from threading import Thread
from time import sleep

class Syncer(object):
    def __init__(self, wdir, db, parser):
        self.wdir = wdir
        self.db = db
        self.parser = parser

        self.all_files = []
        self.processed = 0
        self.finished = False
        self.isRunning = False
        self.t = None

    def get_progress(self):
        return len(self.all_files), self.processed

    def _get_file_to_process(self):
        all_files = []
        for dirpath, dirnames, filenames in os.walk(self.wdir):
            for f in filenames:
                if f.endswith('.xml') or f.endswith('.tiff') or f.endswith('.jpg'):
                    all_files.append(os.path.join(dirpath, f))
        return all_files

    def start(self):
        self.isRunning = True
        self.finished = False
        self.processed = 0
        self.all_files = self._get_file_to_process()
        self.t = Thread(target=self._process)
        self.t.start()

    def stop(self):
        if self.isRunning:
            print('stop syncer')
            self.isRunning = False
            self.finished = True
            if self.t is not None:
                while self.t.is_alive():
                    sleep(0.001)
                print('syncer thread died')
                self.t = None


    def _process(self):
        for f in self.all_files:
            if not self.isRunning:
                break

            if f.endswith('.xml'):
                doc = self.parser.xml_to_doc(f)
                self.db.save_doc_one(doc)
            elif f.endswith('.tiff'):
                doc = self.parser.tiff_to_doc(f)
                self.db.save_img_one(doc, 'tiff')
            elif f.endswith('.jpg'):
                doc = self.parser.jpg_to_doc(f)
                self.db.save_img_one(doc, 'jpg')
                
            self.processed += 1
        self.finished = True
        self.isRunning = False
