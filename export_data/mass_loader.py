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


def process_file(file_path: Path, db: str, force: bool = False) -> Tuple[str, str, Union[int, str], str, str]:
    src = str(file_path)
    dest_dir = os.path.join(SPOOL_DIR, db)
    dest = os.path.join(dest_dir, file_path.name)
    
    state_path = get_state_path(file_path.name, db)
    
    if not force and Path(state_path).exists():
        state = read_state_file(state_path)
        if state is not None:
            if state == 0:
                logger.info(f"File {file_path.name} already successfully uploaded (state=0), skipping copy")
                log_path = get_log_path(file_path.name, db)
                return (src, 'Already Uploaded', state, state_path, log_path)
            else:
                logger.info(f"Found existing state file with non-zero state, waiting 3 seconds for potential update")
                time.sleep(3)
    
    try:
        shutil.copy(src, dest)
        logger.info(f"Copied {src} to {dest}")
    except Exception as e:
        logger.error(f"Failed to copy {src} to {dest}: {e}")
        return (src, f'Copy Failed: {e}', '', '', '')
    
    if not wait_for_state_file(state_path):
        return (src, 'State Timeout', '', state_path, '')
    
    state = read_state_file(state_path)
    if state is None:
        state = 'State Read Error'
        status = 'Error'
    else:
        status = 'Success' if state == 0 else 'Error'
    
    log_path = get_log_path(file_path.name, db)
    logger.info(f"Log path for {file_path.name}: {log_path}")
    
    return (src, status, state, state_path, log_path)


def process_future_result(future: Any, file_path: Path, csv_writer: csv.writer, verbose: bool = False) -> bool:
    try:
        result = future.result()
        if result:
            csv_writer.writerow(result)
            return result[1] == 'Success'
    except Exception as exc:
        if verbose:
            logger.error(f"Error processing {file_path}: {exc}")
        csv_writer.writerow([str(file_path), 'Processing Exception', '', '', ''])
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
    successful = 0
    failed = 0

    if args.verbose:
        logger.info(f"Processing {total_files} files with {chunk_size} parallel uploads")
        logger.info(f"Results will be saved to: {csv_file}")
        if args.force:
            logger.info("Force mode enabled: will upload files even if they were already uploaded")

    try:
        with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(['xml_path', 'upload_status', 'upload_state_value', 'upload_state_path', 'upload_log_path'])

            with ThreadPoolExecutor(max_workers=chunk_size) as executor:
                future_to_file = {
                    executor.submit(process_file, file_path, db, args.force): file_path 
                    for file_path in xml_files
                }

                if args.verbose:
                    for future in as_completed(future_to_file):
                        file_path = future_to_file[future]
                        is_success = process_future_result(future, file_path, csv_writer, args.verbose)
                        successful += 1 if is_success else 0
                        failed += 1 if not is_success else 0
                        logger.info(f"Progress: [{successful + failed}/{total_files}] "
                                  f"(Success: {successful}, Failed: {failed})")
                else:
                    with tqdm(total=total_files, desc="Uploading files", 
                            bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]') as pbar:
                        for future in as_completed(future_to_file):
                            file_path = future_to_file[future]
                            is_success = process_future_result(future, file_path, csv_writer)
                            successful += 1 if is_success else 0
                            failed += 1 if not is_success else 0
                            pbar.update(1)

    except Exception as e:
        logger.error(f"Error during processing: {e}")
        sys.exit(1)

    logger.info("\nUpload Statistics:")
    logger.info("----------------")
    logger.info(f"Successful uploads: {successful}")
    logger.info(f"Failed uploads   : {failed}")
    logger.info(f"Total processed  : {successful + failed}")
    logger.info(f"Results saved to: {csv_file}")


if __name__ == "__main__":
    main() 
