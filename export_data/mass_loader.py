# Dorukhan Boncukçu
# last update: 2025-03-14
# dorukhan.boncukcu@cern.ch
# main repository: https://gitlab.cern.ch/dboncukc/mass-loader
# file source: https://gitlab.cern.ch/hgcal-database/usefull-scripts/-/blob/master/mass_loader.py

import argparse
import csv
import os
import shutil
import sys
import time
import logging
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Optional, Union, Dict, Any, Set, Iterator, Callable, TypeVar, cast, Iterable

logging.basicConfig(handlers=[logging.NullHandler()])
logger = logging.getLogger(__name__)

try:
    from tqdm import tqdm
    has_tqdm = True
except ImportError:
    has_tqdm = False
    logger.warning("tqdm library not found. Progress will be shown through logging.")

SPOOL_BASEDIR = "/home/dbspool"
SPOOL_DIR = os.path.join(SPOOL_BASEDIR, "spool", "hgc")
STATE_DIR = os.path.join(SPOOL_BASEDIR, "state", "hgc")
LOG_DIR = os.path.join(SPOOL_BASEDIR, "logs", "hgc")

DEFAULT_CHUNK_SIZE = 10
DEFAULT_DB = 'int2r'


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Batch upload XML files using dbloader_uploader.sh configuration.'
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--cmsr',
        nargs='+',
        help='Upload to CMSR database. Provide XML file paths separated by space or comma.'
    )
    group.add_argument(
        '--int2r',
        nargs='+',
        help='Upload to INT2R database. Provide XML file paths separated by space or comma, or use wildcards.'
    )
    parser.add_argument(
        '-c', '--chunk_size',
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help=f'Number of parallel uploads (default: {DEFAULT_CHUNK_SIZE})'
    )
    parser.add_argument(
        '-s', '--csv_path',
        type=str,
        help='Path to save CSV results (optional)'
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Show detailed progress with logger instead of progress bar'
    )
    parser.add_argument(
        '-f', '--force',
        action='store_true',
        help='Force upload even if file was already uploaded successfully'
    )
    return parser.parse_args()


def split_paths(path_list: List[str]) -> List[str]:
    split_files = []
    for path in path_list:
        split_files.extend([p.strip() for p in path.split(',') if p.strip()])
    return split_files


def gather_xml_files(paths: List[str]) -> List[Path]:
    xml_files: List[Path] = []
    for pattern in paths:
        path = Path(pattern)
        
        if path.is_dir():
            logger.info(f"Directory provided: {path}, gathering all XML files")
            xml_files.extend([
                p for p in path.glob("*.xml") 
                if p.is_file() and p.suffix.lower() == '.xml'
            ])
            continue
            
        if '*' in pattern:
            try:
                parent = path.parent
                expanded = list(parent.glob(path.name))
                xml_files.extend([
                    p for p in expanded 
                    if p.is_file() and p.suffix.lower() == '.xml'
                ])
            except Exception as e:
                logger.error(f"Error processing glob pattern {pattern}: {e}")
            continue
            
        if path.is_file() and path.suffix.lower() == '.xml':
            xml_files.append(path)
        else:
            logger.error(f"No files matched or invalid XML file: {pattern}")
    
    xml_files = list(dict.fromkeys(xml_files))
    
    if not xml_files:
        logger.warning("No XML files found in the specified paths")
    else:
        logger.info(f"Found {len(xml_files)} XML files")
        if logger.isEnabledFor(logging.INFO):
            for xml_file in xml_files:
                logger.info(f"  - {xml_file}")
    
    return xml_files


def read_state_file(state_path: str) -> Optional[int]:
    try:
        with open(state_path, 'r') as f:
            state = int(f.read().strip())
        return state
    except Exception as e:
        logger.error(f"Failed to read state file {state_path}: {e}")
        return None


def wait_for_state_file(state_path: str, timeout: int = 900, check_interval: float = 0.85) -> bool:
    start_time = time.time()
    while not Path(state_path).exists():
        if time.time() - start_time > timeout:
            logger.error(f"Timeout waiting for state file {state_path}")
            return False
        time.sleep(check_interval)
    return True


def get_state_path(file_name: str, db: str) -> str:
    return os.path.join(STATE_DIR, db, file_name)


def get_log_path(file_name: str, db: str) -> str:
    return os.path.join(LOG_DIR, db, file_name)

def analyze_log_status(log_path: str) -> Tuple[str, str]:
    """
    Analyze the log file and return a tuple: (status, last_line).
    Status is determined by the log content based on key phrases.
    """
    try:
        print(f'checking the log status... {log_path}')
        if not Path(log_path).exists():
            return ("Log Missing", "Log file not found")

        # Read the entire log safely
        with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()

        if not lines:
            return ("Log Empty", "Log file is empty")

        last_line = lines[-1].strip()

        log_text = " ".join(lines)

        # ---- Decision logic ----
        if "commit transaction" in last_line.lower():
            return ("Success", last_line)

        if "dbloader.java:274" in log_text.lower():
            if "dataset already exists" in log_text.lower():
                return ("Already Exists", last_line)
            else:
                return ("XML Parse Error", last_line)

        if "... 20 more" in log_text:
            return ("Missing/Wrong Variable", last_line)

        # Default
        return ("Error", last_line)

    except Exception as e:
        return (f"Error Reading Log: {e}", "")


def process_file(file_path: Path, db: str, force: bool = False) -> Tuple[str, str, str, str, str]:
    """
    Process a single XML file: copy it to spool, wait for state file, and determine upload status from the log.
    """
    src = str(file_path)
    dest_dir = os.path.join(SPOOL_DIR, db)
    dest = os.path.join(dest_dir, file_path.name)
    
    state_path = get_state_path(file_path.name, db)

    print(f"\n[DEBUG] Processing file: {src}")
    print(f"[DEBUG]  dest_dir={dest_dir}")
    print(f"[DEBUG]  state_path={state_path}")
    print(f"[DEBUG]  log_path={log_path}")

    # If already uploaded and not forcing re-upload
    if not force and Path(state_path).exists():
        print(f"[DEBUG] State file already exists for {file_path.name}")
        state = read_state_file(state_path)
        if state is not None:
            if state == 0:
                logger.info(f"File {file_path.name} already successfully uploaded (state=0), skipping copy")
                log_path = get_log_path(file_path.name, db)
                print(f"[DEBUG] State=0 → treating as already uploaded")
                status, last_line = analyze_log_status(log_path)
                return (src, status, last_line, state_path, log_path)
            else:
                print(f"[DEBUG] Non-zero state found ({state}), waiting 3s for possible update")
                logger.info(f"Found existing state file with non-zero state, waiting 3 seconds for potential update")
                time.sleep(3)
    
    # Copy to spool
    try:
        shutil.copy(src, dest)
        print(f"[DEBUG] Copied {src} → {dest}")
        logger.info(f"Copied {src} to {dest}")
    except Exception as e:
        print(f"[DEBUG][ERROR] Failed to copy {src} to {dest}: {e}")
        logger.error(f"Failed to copy {src} to {dest}: {e}")
        return (src, f'Copy Failed: {e}', '', '', '')
    
    # Wait for state file to appear
    if not wait_for_state_file(state_path):
        status, last_line = analyze_log_status(log_path)
        logger.warning(f"State file did not appear for {file_path.name}")
        return (src, f'State Timeout ({status})', last_line, state_path, log_path)
    
    # Read upload result from state
    state = read_state_file(state_path)
    print(f"[DEBUG] Read state={state} for {file_path.name}")
    status, last_line = analyze_log_status(log_path)
    if state is None:
        if status in ['Success', 'Already Exists']:
            state = 0
        else:
            state = 'State Read Error'
            status = 'Error'
    else:
        # Determine finer-grained status based on log file
        status, last_line = analyze_log_status(log_path)
        print(f"[DEBUG] Log analysis → status={status}, last_line={last_line}")
        if state == 0 and status != "Success":
            # Inconsistent but log says something else
            status = f"State=0 but Log={status}"
    return (src, status, last_line, state_path, log_path)



# def process_future_result(future: Any, file_path: Path, csv_writer: csv.writer, verbose: bool = False) -> bool:
#     try:
#         result = future.result()
#         if result:
#             csv_writer.writerow(result)
#             return result[1] == 'Success'
#     except Exception as exc:
#         if verbose:
#             logger.error(f"Error processing {file_path}: {exc}")
#         csv_writer.writerow([str(file_path), 'Processing Exception', '', '', ''])
#     return False

def process_future_result(future: Any, file_path: Path, csv_writer: csv.writer, verbose: bool = False) -> bool:
    print(f"[DEBUG] Collecting future result for {file_path}", flush=True)
    try:
        result = future.result()
        print(f"[DEBUG] Future result received for {file_path}: {result}", flush=True)
        if result:
            csv_writer.writerow(result)
            print(f"[DEBUG] CSV write complete for {file_path}", flush=True)
            return result[1] == 'Success'
        else:
            print(f"[DEBUG][WARN] Empty result returned for {file_path}", flush=True)
    except Exception as exc:
        print(f"[DEBUG][ERROR] Exception in process_future_result for {file_path}: {exc}", flush=True)
        if verbose:
            logger.error(f"Error processing {file_path}: {exc}")
        csv_writer.writerow([str(file_path), 'Processing Exception', '', '', ''])
    print(f"[DEBUG] process_future_result() returning False for {file_path}", flush=True)
    return False

def setup_logging(verbose: bool, timestamp: str) -> None:
    logger.handlers.clear()
    
    if verbose:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    else:
        log_file = f"dbloader_batch_uploader_{timestamp}.log"
        handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(handler)
        error_handler = logging.StreamHandler(sys.stdout)
        error_handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
        error_handler.setLevel(logging.ERROR)
        logger.addHandler(error_handler)
        logger.setLevel(logging.INFO)
        logger.info(f"Detailed logs will be written to: {log_file}")


def main() -> None:
    args = parse_arguments()

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    setup_logging(args.verbose, timestamp)

    if not has_tqdm and not args.verbose:
        logger.info("tqdm not available, enabling verbose mode automatically")
        args.verbose = True
        setup_logging(True, timestamp)

    db = 'cmsr' if args.cmsr else 'int2r'
    raw_paths = split_paths(args.cmsr if args.cmsr else args.int2r)
    xml_files = gather_xml_files(raw_paths)

    if not xml_files:
        logger.error("No valid XML files to process.")
        sys.exit(1)

    if args.csv_path:
        csv_file = args.csv_path
    else:
        csv_file = f"dbloader_batch_uploader_{timestamp}.csv"

    csv_dir = os.path.dirname(csv_file)
    if csv_dir:
        os.makedirs(csv_dir, exist_ok=True)

    total_files = len(xml_files)
    chunk_size = args.chunk_size

    # Counters
    stats = {
        "Success": 0,
        "Already Exists": 0,
        "XML Parse Error": 0,
        "Missing/Wrong Variable": 0,
        "Other Error": 0,
    }

    if args.verbose:
        logger.info(f"Processing {total_files} files with {chunk_size} parallel uploads")
        logger.info(f"Results will be saved to: {csv_file}")
        if args.force:
            logger.info("Force mode enabled: will upload files even if they were already uploaded")

    try:
        with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(['xml_path', 'upload_status', 'log_last_line', 'upload_state_path', 'upload_log_path'])

            with ThreadPoolExecutor(max_workers=chunk_size) as executor:
                future_to_file = {
                    executor.submit(process_file, file_path, db, args.force): file_path
                    for file_path in xml_files
                }

                if args.verbose:
                    for future in as_completed(future_to_file):
                        file_path = future_to_file[future]
                        result = future.result()
                        csv_writer.writerow(result)
                        status = result[1]

                        # Update counters
                        if status == "Success":
                            stats["Success"] += 1
                        elif status == "Already Exists":
                            stats["Already Exists"] += 1
                        elif status == "XML Parse Error":
                            stats["XML Parse Error"] += 1
                        elif status == "Missing/Wrong Variable":
                            stats["Missing/Wrong Variable"] += 1
                        else:
                            stats["Other Error"] += 1

                        processed = sum(stats.values())
                        logger.info(
                            f"Progress: [{processed}/{total_files}] "
                            f"(Success: {stats['Success']}, Already Exists: {stats['Already Exists']}, "
                            f"Errors: {stats['XML Parse Error'] + stats['Missing/Wrong Variable'] + stats['Other Error']})"
                        )
                else:
                    with tqdm(total=total_files, desc="Uploading files",
                              bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]') as pbar:
                        for future in as_completed(future_to_file):
                            file_path = future_to_file[future]
                            result = future.result()
                            csv_writer.writerow(result)
                            status = result[1]

                            if status == "Success":
                                stats["Success"] += 1
                            elif status == "Already Exists":
                                stats["Already Exists"] += 1
                            elif status == "XML Parse Error":
                                stats["XML Parse Error"] += 1
                            elif status == "Missing/Wrong Variable":
                                stats["Missing/Wrong Variable"] += 1
                            else:
                                stats["Other Error"] += 1

                            pbar.update(1)

    except Exception as e:
        logger.error(f"Error during processing: {e}")
        sys.exit(1)

    # ---- Final Summary ----
    total_processed = sum(stats.values())

    logger.info("\nUpload Statistics:")
    logger.info("----------------")
    logger.info(f"Successful uploads        : {stats['Success']}")
    logger.info(f"Already existing datasets : {stats['Already Exists']}")
    logger.info(f"XML Parse errors          : {stats['XML Parse Error']}")
    logger.info(f"Missing/Wrong variables   : {stats['Missing/Wrong Variable']}")
    logger.info(f"Other errors              : {stats['Other Error']}")
    logger.info(f"Total processed           : {total_processed}")
    logger.info(f"Results saved to: {csv_file}")


if __name__ == "__main__":
    main() 
