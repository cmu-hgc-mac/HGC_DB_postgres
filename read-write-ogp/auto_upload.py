import time, os, subprocess
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class NewFileHandler(FileSystemEventHandler):
    def __init__(self, test_file_path):
        self.test_file_path = test_file_path

    def on_created(self, event):
        if not event.is_directory:
            new_file_path = event.src_path
            print("New file created:", new_file_path)
            self.run_test_file(new_file_path)

    def run_test_file(self, new_file_path):
        subprocess.Popen(['python3', self.test_file_path])#, new_file_path])

def monitor_directory(path, test_file_path):
    event_handler = NewFileHandler(test_file_path)
    observer = Observer()
    observer.schedule(event_handler, path, recursive=False)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        print("\nKeyboard interrupt received. Exiting gracefully.")
    observer.join()

if __name__ == "__main__":
    directory_to_watch = "/Users/sindhu/Downloads/test_trig"
    test_file_path = "/Users/sindhu/Downloads/test.py"
    print(f'Watching directory: {directory_to_watch}')
    monitor_directory(directory_to_watch, test_file_path)
