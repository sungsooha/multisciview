import os
import threading

class Syncer(object):
    def __init__(self,
                 files_to_sync,
                 path,
                 info,
                 onFinished,
                 onSync):
        self.files_to_sync = files_to_sync
        self.path = path
        self.info = info
        self.onFinished = onFinished
        self.onSync = onSync

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

            self.onSync(
                'sync',
                'syncing',
                os.path.join(self.path, file),
                None
            )

            self.processed += 1

        self.isRunning = False
        self.info = self.onFinished(self.path, self.info)
