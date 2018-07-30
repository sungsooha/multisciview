from watchdog.events import FileSystemEventHandler

class FSHandler(FileSystemEventHandler):
    def __init__(self):
        pass

    def on_modified(self, event):
        """add modification event into queue"""
        pass

    def on_created(self, event):
        """add creation event into queue"""
        pass

    def on_deleted(self, event):
        """add deletion event into queue"""
        pass

    def _process_event(self):
        """
        process events
        """
        pass