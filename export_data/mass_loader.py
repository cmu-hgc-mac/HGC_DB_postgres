"""
Mass Loader - Batch XML File Upload Tool
Author: Dorukhan Boncukçu
Email: dorukhan.boncukcu@cern.ch
Last Update: 2025-10-07

This tool uploads XML files to CMSR or INT2R databases in parallel batches,
monitoring the upload state and providing detailed progress reporting.

main repository: https://gitlab.cern.ch/dboncukc/mass-loader
new file source: https://gitlab.cern.ch/dboncukc/mass-loader/-/blob/master/mass_loader.py?ref_type=heads
old file source: https://gitlab.cern.ch/hgcal-database/usefull-scripts/-/blob/master/mass_loader.py
"""

import argparse
import csv
import logging
import os
import shutil
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import List, Optional, Union

# Configure logging
logging.basicConfig(handlers=[logging.NullHandler()])
logger = logging.getLogger(__name__)

# Optional tqdm support
try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False
    logger.warning("tqdm not found. Progress shown via logging.")


# =============================================================================
# Configuration
# =============================================================================

class Config:
    """Application configuration."""

    def __init__(
        self,
        spool_basedir: str = "/home/dbspool",
        default_chunk_size: int = 10,
        default_timeout: int = 10,
        state_check_interval: float = 0.85
    ):
        self.spool_basedir = spool_basedir
        self.default_chunk_size = default_chunk_size
        self.default_timeout = default_timeout
        self.state_check_interval = state_check_interval

    @property
    def spool_dir(self) -> str:
        return os.path.join(self.spool_basedir, "spool", "hgc")

    @property
    def state_dir(self) -> str:
        return os.path.join(self.spool_basedir, "state", "hgc")

    @property
    def log_dir(self) -> str:
        return os.path.join(self.spool_basedir, "logs", "hgc")


class UploadStatus(Enum):
    """Upload status codes."""
    SUCCESS = "Success"
    ERROR = "Error"
    TIMEOUT = "State Timeout"
    ALREADY_UPLOADED = "Already Uploaded"
    COPY_FAILED = "Copy Failed"
    PROCESSING_EXCEPTION = "Processing Exception"


class UploadResult:
    """Result of a file upload operation."""

    def __init__(
        self,
        source_path: str,
        status: str,
        state_value: Union[str, int],
        state_path: str,
        log_path: str
    ):
        self.source_path = source_path
        self.status = status
        self.state_value = state_value
        self.state_path = state_path
        self.log_path = log_path

    def to_csv_row(self) -> List[str]:
        """Convert to CSV row format."""
        return [
            self.source_path,
            self.status,
            str(self.state_value),
            self.state_path,
            self.log_path
        ]


class Database(Enum):
    """Supported database targets."""
    CMSR = "cmsr"
    INT2R = "int2r"


# =============================================================================
# File Operations
# =============================================================================

class FileHandler:
    """Handles file operations and path management."""

    def __init__(self, config: Config):
        self.config = config

    def get_state_path(self, filename: str, db: Database) -> str:
        """Get state file path for a given file and database."""
        return os.path.join(self.config.state_dir, db.value, filename)

    def get_log_path(self, filename: str, db: Database) -> str:
        """Get log file path for a given file and database."""
        return os.path.join(self.config.log_dir, db.value, filename)

    def read_state_file(self, state_path: str) -> Optional[int]:
        """Read and parse state file content."""
        try:
            with open(state_path, 'r') as f:
                return int(f.read().strip())
        except (FileNotFoundError, ValueError, IOError) as e:
            logger.error(f"Failed to read state file {state_path}: {e}")
            return None

    def wait_for_state_file(
        self,
        state_path: str,
        timeout: int,
        check_interval: float = None,
        initial_mtime: float = None
    ) -> bool:
        """
        Wait for state file to appear or be updated within timeout period.

        Args:
            state_path: Path to state file
            timeout: Maximum time to wait in seconds
            check_interval: How often to check (seconds)
            initial_mtime: Initial modification time (if file exists before copy)

        Returns:
            True if file appears/updates, False if timeout
        """
        if check_interval is None:
            check_interval = self.config.state_check_interval

        start_time = time.time()
        state_file = Path(state_path)

        while True:
            if time.time() - start_time > timeout:
                logger.error(
                    f"Timeout ({timeout}s) reached while waiting for "
                    f"state file {state_path}"
                )
                return False

            if state_file.exists():
                # If we have initial mtime, wait for file to be newer
                if initial_mtime is not None:
                    current_mtime = state_file.stat().st_mtime
                    if current_mtime > initial_mtime:
                        return True
                else:
                    # File didn't exist before, now it does
                    return True

            time.sleep(check_interval)

        return True

    def gather_xml_files(self, paths: List[str]) -> List[Path]:
        """Collect all XML files from provided paths/patterns."""
        xml_files: List[Path] = []

        for pattern in paths:
            path = Path(pattern)

            # Handle directories
            if path.is_dir():
                logger.info(f"Directory provided: {path}, gathering all XML files")
                xml_files.extend(
                    p for p in path.glob("*.xml")
                    if p.is_file() and p.suffix.lower() == '.xml'
                )
                continue

            # Handle glob patterns
            if '*' in pattern:
                try:
                    parent = path.parent
                    expanded = list(parent.glob(path.name))
                    xml_files.extend(
                        p for p in expanded
                        if p.is_file() and p.suffix.lower() == '.xml'
                    )
                except Exception as e:
                    logger.error(f"Error processing glob pattern {pattern}: {e}")
                continue

            # Handle individual files
            if path.is_file() and path.suffix.lower() == '.xml':
                xml_files.append(path)
            else:
                logger.error(f"No files matched or invalid XML file: {pattern}")

        # Remove duplicates while preserving order
        xml_files = list(dict.fromkeys(xml_files))

        if not xml_files:
            logger.warning("No XML files found in the specified paths")
        else:
            logger.info(f"Found {len(xml_files)} XML files")
            if logger.isEnabledFor(logging.INFO):
                for xml_file in xml_files:
                    logger.info(f"  - {xml_file}")

        return xml_files


# =============================================================================
# Upload Processing
# =============================================================================

class UploadProcessor:
    """Processes file uploads to the database."""

    def __init__(self, config: Config, file_handler: FileHandler):
        self.config = config
        self.file_handler = file_handler

    def process_file(
        self,
        file_path: Path,
        db: Database,
        force: bool = False,
        timeout: int = None
    ) -> UploadResult:
        """
        Process a single file upload.

        Args:
            file_path: Path to XML file to upload
            db: Target database
            force: Force upload even if already uploaded
            timeout: Timeout in seconds for state file operations

        Returns:
            UploadResult with status and paths
        """
        if timeout is None:
            timeout = self.config.default_timeout

        src = str(file_path)
        dest_dir = os.path.join(self.config.spool_dir, db.value)
        dest = os.path.join(dest_dir, file_path.name)
        state_path = self.file_handler.get_state_path(file_path.name, db)

        # Check if already uploaded successfully
        if not force and Path(state_path).exists():
            state = self.file_handler.read_state_file(state_path)
            if state is not None and state == 0:
                logger.info(
                    f"File {file_path.name} already uploaded (state=0), "
                    "skipping"
                )
                log_path = self.file_handler.get_log_path(file_path.name, db)
                return UploadResult(
                    src,
                    UploadStatus.ALREADY_UPLOADED.value,
                    state,
                    state_path,
                    log_path
                )

        # Record state file mtime before copy (to detect updates)
        state_path_obj = Path(state_path)
        initial_mtime = state_path_obj.stat().st_mtime if state_path_obj.exists() else None

        # Copy file to spool directory
        try:
            shutil.copy(src, dest)
            logger.info(f"Copied {src} to {dest}")
        except Exception as e:
            logger.error(f"Failed to copy {src} to {dest}: {e}")
            return UploadResult(
                src,
                f'{UploadStatus.COPY_FAILED.value}: {e}',
                '',
                '',
                ''
            )

        # Wait for state file to appear or be updated
        if not self.file_handler.wait_for_state_file(state_path, timeout, initial_mtime=initial_mtime):
            logger.error(
                f"State file not found within {timeout}s timeout "
                f"for {file_path.name}"
            )
            return UploadResult(
                src,
                UploadStatus.TIMEOUT.value,
                '',
                state_path,
                ''
            )

        # Read final state
        state = self.file_handler.read_state_file(state_path)
        log_path = self.file_handler.get_log_path(file_path.name, db)

        # Default interpretation from state file
        if state is None:
            status = UploadStatus.ERROR.value
            state = 'State Read Error'
        elif state == 0:
            status = UploadStatus.SUCCESS.value
        else:
            status = UploadStatus.ERROR.value

        # --- NEW SECTION: check log content for special cases ---
        try:
            if os.path.exists(log_path):
                with open(log_path, 'r', encoding='utf-8', errors='ignore') as logf:
                    log_text = logf.read().lower()
                    if "dataset already exists" in log_text or "already uploaded" in log_text:
                        status = UploadStatus.ALREADY_UPLOADED.value
                    elif "timeout" in log_text and status != UploadStatus.SUCCESS.value:
                        status = UploadStatus.TIMEOUT.value
        except Exception as log_err:
            logger.warning(f"Could not read log file {log_path}: {log_err}")
        # --------------------------------------------------------

        logger.info(f"Final status for {file_path.name}: {status}")
        return UploadResult(src, status, state, state_path, log_path)



# =============================================================================
# Statistics and Progress Tracking
# =============================================================================

class UploadStatistics:
    """Track upload statistics."""

    def __init__(
        self,
        successful: int = 0,
        failed: int = 0,
        timeout: int = 0,
        already_uploaded: int = 0
    ):
        self.successful = successful
        self.failed = failed
        self.timeout = timeout
        self.already_uploaded = already_uploaded

    @property
    def total(self) -> int:
        return self.successful + self.failed + self.timeout + self.already_uploaded

    def update_from_result(self, result: UploadResult) -> None:
        """Update statistics from an upload result."""
        if result.status == UploadStatus.SUCCESS.value:
            self.successful += 1
        elif result.status == UploadStatus.ALREADY_UPLOADED.value:
            self.already_uploaded += 1
        elif result.status == UploadStatus.TIMEOUT.value:
            self.timeout += 1
        else:
            self.failed += 1

    def log_summary(self) -> None:
        """Log statistics summary."""
        logger.info("\nUpload Statistics:")
        logger.info("----------------")
        logger.info(f"Successful uploads : {self.successful}")
        logger.info(f"Already uploaded   : {self.already_uploaded}")
        logger.info(f"Failed uploads     : {self.failed}")
        logger.info(f"Timeout uploads    : {self.timeout}")
        logger.info(f"Total processed    : {self.total}")


class ProgressTracker:
    """Track and display upload progress."""

    def __init__(self, total: int, verbose: bool):
        self.total = total
        self.verbose = verbose
        self.stats = UploadStatistics()

    def log_progress(self) -> None:
        """Log current progress."""
        if self.verbose:
            logger.info(
                f"Progress: [{self.stats.total}/{self.total}] "
                f"(Success: {self.stats.successful}, "
                f"Already: {self.stats.already_uploaded}, "
                f"Failed: {self.stats.failed}, "
                f"Timeout: {self.stats.timeout})"
            )


# =============================================================================
# CSV Writer
# =============================================================================

class CSVResultWriter:
    """Handles writing results to CSV file."""

    HEADERS = [
        'xml_path',
        'upload_status',
        'upload_state_value',
        'upload_state_path',
        'upload_log_path'
    ]

    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self._ensure_directory_exists()

    def _ensure_directory_exists(self) -> None:
        """Ensure CSV directory exists."""
        csv_dir = os.path.dirname(self.csv_path)
        if csv_dir:
            os.makedirs(csv_dir, exist_ok=True)

    def write_result(
        self,
        writer: csv.writer,
        file_handle,
        result: UploadResult
    ) -> None:
        """Write a single result and flush immediately."""
        writer.writerow(result.to_csv_row())
        file_handle.flush()


# =============================================================================
# Main Application
# =============================================================================

class MassLoader:
    """Main application class for batch XML uploads."""

    def __init__(self, config: Config = None):
        self.config = config or Config()
        self.file_handler = FileHandler(self.config)
        self.processor = UploadProcessor(self.config, self.file_handler)

    def setup_logging(self, verbose: bool, timestamp: str) -> None:
        """Configure logging handlers."""
        logger.handlers.clear()

        if verbose:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(
                logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            )
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        else:
            # File handler for detailed logs
            log_file = f"mass_loader_{timestamp}.log"
            file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
            file_handler.setFormatter(
                logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            )
            logger.addHandler(file_handler)

            # Console handler for errors only
            error_handler = logging.StreamHandler(sys.stdout)
            error_handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
            error_handler.setLevel(logging.ERROR)
            logger.addHandler(error_handler)

            logger.setLevel(logging.INFO)
            logger.info(f"Detailed logs will be written to: {log_file}")

    def run(
        self,
        db: Database,
        file_paths: List[str],
        chunk_size: int = None,
        timeout: int = None,
        csv_path: str = None,
        verbose: bool = False,
        force: bool = False
    ) -> UploadStatistics:
        """
        Run the batch upload process.

        Args:
            db: Target database
            file_paths: List of file paths/patterns to upload
            chunk_size: Number of parallel uploads
            timeout: Timeout for state file operations
            csv_path: Path to save CSV results
            verbose: Show detailed progress
            force: Force upload even if already uploaded

        Returns:
            UploadStatistics with final counts
        """
        if chunk_size is None:
            chunk_size = self.config.default_chunk_size
        if timeout is None:
            timeout = self.config.default_timeout

        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        self.setup_logging(verbose, timestamp)

        # Auto-enable verbose if tqdm unavailable
        if not HAS_TQDM and not verbose:
            logger.info("tqdm not available, enabling verbose mode")
            verbose = True
            self.setup_logging(True, timestamp)

        # Gather XML files
        xml_files = self.file_handler.gather_xml_files(file_paths)
        if not xml_files:
            logger.error("No valid XML files to process")
            sys.exit(1)

        # Setup CSV output
        if csv_path is None:
            csv_path = f"mass_loader_{timestamp}.csv"
        csv_writer = CSVResultWriter(csv_path)

        total_files = len(xml_files)
        tracker = ProgressTracker(total_files, verbose)

        if verbose:
            logger.info(f"Processing {total_files} files with {chunk_size} parallel uploads")
            logger.info(f"Results will be saved to: {csv_path}")
            if force:
                logger.info("Force mode: will upload files even if already uploaded")

        # Process uploads
        try:
            with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(CSVResultWriter.HEADERS)
                csvfile.flush()

                with ThreadPoolExecutor(max_workers=chunk_size) as executor:
                    # Submit all tasks
                    future_to_file = {
                        executor.submit(
                            self.processor.process_file,
                            file_path,
                            db,
                            force,
                            timeout
                        ): file_path
                        for file_path in xml_files
                    }

                    # Process results as they complete
                    if verbose:
                        self._process_results_verbose(
                            future_to_file,
                            writer,
                            csvfile,
                            csv_writer,
                            tracker
                        )
                    else:
                        self._process_results_with_progress_bar(
                            future_to_file,
                            writer,
                            csvfile,
                            csv_writer,
                            tracker
                        )

        except Exception as e:
            logger.error(f"Error during processing: {e}")
            sys.exit(1)

        # Log final statistics
        tracker.stats.log_summary()
        logger.info(f"Results saved to: {csv_path}")

        return tracker.stats

    def _process_results_verbose(
        self,
        future_to_file,
        writer,
        csvfile,
        csv_writer: CSVResultWriter,
        tracker: ProgressTracker
    ) -> None:
        """Process results with verbose logging."""
        for future in as_completed(future_to_file):
            file_path = future_to_file[future]
            try:
                result = future.result()
                csv_writer.write_result(writer, csvfile, result)
                tracker.stats.update_from_result(result)
                tracker.log_progress()
            except Exception as exc:
                logger.error(f"Error processing {file_path}: {exc}")
                error_result = UploadResult(
                    str(file_path),
                    UploadStatus.PROCESSING_EXCEPTION.value,
                    '',
                    '',
                    ''
                )
                csv_writer.write_result(writer, csvfile, error_result)
                tracker.stats.update_from_result(error_result)
                tracker.log_progress()

    def _process_results_with_progress_bar(
        self,
        future_to_file,
        writer,
        csvfile,
        csv_writer: CSVResultWriter,
        tracker: ProgressTracker
    ) -> None:
        """Process results with tqdm progress bar."""
        with tqdm(
            total=tracker.total,
            desc="Uploading files",
            bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]'
        ) as pbar:
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    result = future.result()
                    csv_writer.write_result(writer, csvfile, result)
                    tracker.stats.update_from_result(result)
                except Exception as exc:
                    logger.error(f"Error processing {file_path}: {exc}")
                    error_result = UploadResult(
                        str(file_path),
                        UploadStatus.PROCESSING_EXCEPTION.value,
                        '',
                        '',
                        ''
                    )
                    csv_writer.write_result(writer, csvfile, error_result)
                    tracker.stats.update_from_result(error_result)

                pbar.update(1)


# =============================================================================
# CLI Interface
# =============================================================================

def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Batch upload XML files to CMSR or INT2R database',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    # Database selection (mutually exclusive)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--cmsr',
        nargs='+',
        help='Upload to CMSR database. Provide XML file paths separated by space or comma.'
    )
    group.add_argument(
        '--int2r',
        nargs='+',
        help='Upload to INT2R database. Provide XML file paths separated by space or comma.'
    )

    # Optional arguments
    parser.add_argument(
        '-c', '--chunk_size',
        type=int,
        default=10,
        help='Number of parallel uploads (default: 10)'
    )
    parser.add_argument(
        '-t', '--timeout',
        type=int,
        default=10,
        help='Timeout in seconds for state file waiting (default: 10)'
    )
    parser.add_argument(
        '-s', '--csv_path',
        type=str,
        help='Path to save CSV results (optional)'
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Show detailed progress with logging instead of progress bar'
    )
    parser.add_argument(
        '-f', '--force',
        action='store_true',
        help='Force upload even if file was already uploaded successfully'
    )

    return parser.parse_args()


def split_paths(path_list: List[str]) -> List[str]:
    """Split comma-separated paths into individual paths."""
    split_files = []
    for path in path_list:
        split_files.extend([p.strip() for p in path.split(',') if p.strip()])
    return split_files


def main() -> None:
    """Main entry point."""
    args = parse_arguments()

    # Determine database and paths
    db = Database.CMSR if args.cmsr else Database.INT2R
    raw_paths = split_paths(args.cmsr if args.cmsr else args.int2r)

    # Create and run mass loader
    loader = MassLoader()
    loader.run(
        db=db,
        file_paths=raw_paths,
        chunk_size=args.chunk_size,
        timeout=args.timeout,
        csv_path=args.csv_path,
        verbose=args.verbose,
        force=args.force
    )


if __name__ == "__main__":
    main()
