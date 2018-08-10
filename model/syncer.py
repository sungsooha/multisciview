import threading
import time

class Syncer(object):
    def __init__(self,
                 files_to_sync,
                 path,
                 info,
                 onFinished):
        self.files_to_sync = files_to_sync
        self.path = path
        self.info = info
        self.onFinished = onFinished

        self.processed = 0
        self.total = len(files_to_sync)
        self.isRunning = False
        self.t = None

    def start(self):
        self.isRunning = True
        self.t = threading.Thread(target=self._process)
        self.t.start()

    def get_total(self):
        """Return the total number of files to be synced"""
        return self.total

    def get_processed(self):
        """Return the number of files synced so far..."""
        return self.processed

    def get_info(self):
        info = dict(self.info)
        info['total'] = self.total
        info['processed'] = self.processed
        return info

    def _process(self):
        """
        item = {
            'path': path to directory,
            'files': list of filenames in the directory
            'client': MongoDB client
        }
        :return:
        """
        for file in self.files_to_sync:
            if not self.isRunning:
                break

            time.sleep(0.1) # process on the file

            self.processed += 1

            # path_to_dir = item['path']
            # filenames = item['files']
            # db = item['client']
            #
            # for f in filenames:
            #
            #     if not self.isRunning:
            #         break
            #
            #     fileExt = os.path.splitext(f)[1]
            #     fullpath = os.path.join(path_to_dir, f)
            #
            #     if fileExt == '.xml':
            #         doc = self.parser.xml_to_doc(fullpath)
            #         db.save_doc_one(doc)
            #     elif fileExt == '.tiff':
            #         doc = self.parser.tiff_to_doc(fullpath)
            #         db.save_img_one(doc, 'tiff')
            #     elif fileExt == '.jpg':
            #         doc = self.parser.jpg_to_doc(fullpath)
            #         db.save_img_one(doc, 'jpg')
            #     else:
            #         print('Unsupported file: ', fullpath)
            #     #print('SYNC: ', fullpath)
            #
            #     self.processed += 1
            #
            #     if self.processed%100 == 0:
            #         sleep(0.001)

        self.isRunning = False
        self.info = self.onFinished(self.path, self.info)
