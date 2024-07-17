import os
import time
import subprocess
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

local_xml_dir = "converted_xml/baseplate/"
remote_xml_dir = "username@remote_host:/path/to/remote_xml_dir/"

class XMLFileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        elif event.src_path.endswith(".xml"):
            start_time = time.time()  # Record the start time
            self.transfer_file(event.src_path)
            end_time = time.time()  # Record the end time
            transfer_time = end_time - start_time
            print(f"Time taken to transfer {event.src_path}: {transfer_time:.2f} seconds")

    def transfer_file(self, file_path):
        command = f"scp {file_path} {remote_xml_dir}"
        try:
            subprocess.run(command, shell=True, check=True)
            print(f"Transferred: {file_path}")
        except subprocess.CalledProcessError as e:
            print(f"Error transferring {file_path}: {e}")

if __name__ == "__main__":
    event_handler = XMLFileHandler()
    observer = Observer()
    observer.schedule(event_handler, path=local_xml_dir, recursive=False)
    observer.start()
    print(f"Monitoring {local_xml_dir} for new XML files...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()