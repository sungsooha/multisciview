import time
import threading
from watchdog.events import FileSystemEventHandler

class FSEventHandler(FileSystemEventHandler):
    """
    Filesystem handler
    - process on the files detected by watchdog.observer
    - watchdog.observer will populates a queue (or array) storing events (modification, creatation).
    - Then, an internal thread processes events in the queue such that
        - keep the latest events only for the same files or directories
        - (need attention to create, modify, move and delete)
    - Event for the internal thread needs to be implemented and used such as
        - pause by another thread to fetch the events.
    """
    thread = None # background thread to process on the observed events
    thread_event = None # background thread event
    task_list = []

    def __init__(self):
        pass

    def on_any_event(self, event):
        """
        event:
            is_directory: True | False
            src_path: path to observed file
            event_type: created, modified, moved, deleted
        """
        if event.is_directory:
            pass
        else:
            pass

    def _process(self):
        pass

    def get_task(self):
        """Called by other threads to get a task to process"""
        # lock the background thread
        # get a task
        # unlock the background thread
        # return the task
        pass

class FSWatcher(object):
    def __init__(self):
        fsHandler = FSEventHandler()





if __name__ == '__main__':
    pass







