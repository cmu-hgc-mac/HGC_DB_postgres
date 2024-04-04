import time, os, subprocess
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class NewFileHandler(FileSystemEventHandler):
    def __init__(self, process_file_path):
        self.process_file_path = process_file_path

    def on_created(self, event):
        if not event.is_directory:
            print(event)
            new_file_path = os.path.normpath(event.src_path).replace('\\','/')
            print("\nNew file created:", new_file_path)
            self.run_test_file(new_file_path)

    def run_test_file(self, new_file_path):
        subprocess.Popen(['python', self.process_file_path, new_file_path])

def monitor_directory(path, process_file_path):
    event_handler = NewFileHandler(process_file_path)
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        print("\nKeyboard interrupt received. Exiting gracefully.")
    observer.join()

if __name__ == "__main__":
    directory_to_watch = "C:/Users/Admin/Desktop/module_assembly_surveys/offsets/OGP_results"
    process_file_path = "C:/Users/Admin/Desktop/module_assembly_surveys/offsets/data_processing_and_plotting/process_im.py"
    print(f'Watching directory: {directory_to_watch}')
    monitor_directory(directory_to_watch, process_file_path)