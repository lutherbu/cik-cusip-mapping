#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import threading
from pathlib import Path
import requests
from typing import List, Dict, Callable
import tarfile
from datetime import datetime
import logging

from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse
import os

class EfficientDownloader:
    """
    A class for efficiently downloading and processing a large number of files from a single site.
    """

    def __init__(
        self,
        urls: List[str],
        download_dir: Path,
        archive_prefix: str,
        process_func: Callable = lambda x: x,   # default process_func is Identity
        user_agent: Dict = None,
        rate_limit: int = 10
    ):
        """
        Initialize the EfficientDownloader class.

        :param urls: List of URLs to download.
        :param download_dir: Directory to store downloaded files.
        :param archive_prefix: Filepath prefix of the tar.gz archive to create.
        :param processor_fun: User-supplied function to process downloaded files.
        :param user_agent: Optional user agent dictionary.
        :param rate_limit: Rate limit in requests per second (default: 10).
        """
        self.urls = urls
        self.process_func = process_func
        self.download_dir = download_dir
        self.archive_prefix = archive_prefix
        self.common_prefix = os.path.commonprefix(self.urls)
        if not self.common_prefix.endswith('/'):
            self.common_prefix = self.common_prefix.rsplit('/', 1)[0] + '/'
        self.user_agent = user_agent or {"User-Agent": "EfficientDownloader/1.0"}
        self.rate_limit = rate_limit
        self.lock = threading.Lock()
        self.last_request_time = 0
        self.failed_downloads = []
        self.successful_downloads = []
        self.stats = {
            "total_files": len(urls),
            "successful_downloads": 0,
            "failed_downloads": 0,
            "total_download_size": 0,
            "total_processed_size": 0,
            "total_download_time": 0,
            "total_processing_time": 0
        }

        # Create download directory if it doesn't exist
        self.download_dir.mkdir(parents=True, exist_ok=True)

        # Set up logging
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)


    def download_all(self):
        """
        Download and process all files in the URL list.
        """
        threads = []
        for url in self.urls:
            thread = threading.Thread(target=self._download_and_process_file, args=(url,))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

    # # This is an asynchronous alternative
    # def download_all(self):
    #     """
    #     Download and process all files in the URL list.
    #     """
    #     with ThreadPoolExecutor() as executor:
    #         executor.map(self._download_and_process_file, self.urls)


    def download_and_process(self):
        """
        Download and process all URLs while respecting the rate limit.
        """
        print("Starting download process...")
        self.download_all()
        print("Download process completed.")

        print("Creating archive...")
        self.create_archive()
        print("Logging failed downloads...")
        self.log_failures()
        self._print_summary_stats()


    def _get_filename(self, url: str) -> str:
        """
        Generate a filename based on the URL, stripping the common prefix
        and replacing '/' with '_' to ensure a valid filename.

        :param url: URL of the file.
        :return: Generated filename.
        """
        filename = url[len(self.common_prefix):] or url.rsplit('/', 1)[-1]
        return filename.replace('/', '_')


    def _download_and_process_file(self, url: str):
        """
        Download a file, process it, and save the result.

        :param url: URL of the file to download.
        """
        file_name = self._get_filename(url)
        file_path = self.download_dir / file_name

        retries = 0
        max_retries = 3
        backoff_factor = 2

        while retries < max_retries:
            try:
                self._respect_rate_limit()
                start_time = time.time()
                response = requests.get(url, headers=self.user_agent)
                response.raise_for_status()
                download_time = time.time() - start_time

                content = response.content

                download_size = len(content)

                start_time = time.time()
                processed_content = self.process_func(content)
                processing_time = time.time() - start_time

                with file_path.open("wb") as f:
                    f.write(processed_content)

                with self.lock:
                    self.stats["successful_downloads"] += 1
                    self.stats["total_download_size"] += download_size
                    self.stats["total_processed_size"] += len(processed_content)
                    self.stats["total_download_time"] += download_time
                    self.stats["total_processing_time"] += processing_time
                    self.successful_downloads.append(file_path)

                self.logger.info(f"Successfully downloaded and processed: {url}")
                break

            except requests.exceptions.RequestException as e:
                retries += 1
                if retries == max_retries:
                    with self.lock:
                        self.stats["failed_downloads"] += 1
                        self.failed_downloads.append((url, str(e), retries))
                    self.logger.error(f"Failed to download {url} after {retries} attempts: {e}")
                else:
                    wait_time = backoff_factor ** retries
                    self.logger.warning(f"Attempt {retries} failed for {url}. Retrying in {wait_time} seconds.")
                    time.sleep(wait_time)

    def _respect_rate_limit(self):
        """
        Ensure the global rate limit is respected across all threads.
        """
        with self.lock:
            current_time = time.time()
            elapsed = current_time - self.last_request_time
            if elapsed < 1 / self.rate_limit:
                time.sleep(1 / self.rate_limit - elapsed)
            self.last_request_time = time.time()


    def log_failures(self):
        """
        Write failed downloads to a log file.
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        log_file = self.download_dir / f"failed_downloads_{timestamp}.log"

        if self.failed_downloads:
            with log_file.open("w") as f:
                for url, error, attempts in self.failed_downloads:
                    f.write(f"URL: {url}\nError: {error}\nAttempts: {attempts}\n\n")

            print(f"Failed downloads logged to: {log_file}")


    def create_archive(self):
        """
        Compress successfully downloaded and processed files into a tar.gz archive.
        """

        # archive_file = self.download_dir / f"{self.archive_name}.tar.gz"
        archive_file = (self.download_dir / self.archive_prefix).with_suffix(".tar.gz")
        with tarfile.open(archive_file, "w:gz") as tar:
            for file_path in self.successful_downloads:
                tar.add(file_path, arcname=file_path.name)
                file_path.unlink()  # Remove the original file after adding to archive

        print(f"Created archive: {archive_file}")

    def _print_summary_stats(self):
        """
        Print summary statistics of the download process.
        """
        print("\nDownload Summary:")
        self.logger.info(f"Total files: {self.stats['total_files']}")
        self.logger.info(f"Successful downloads: {self.stats['successful_downloads']}")
        self.logger.info(f"Failed downloads: {self.stats['failed_downloads']}")
        self.logger.info(f"Total download size: {self.stats['total_download_size'] / 1024 / 1024:.2f} MB")
        self.logger.info(f"Total processed size: {self.stats['total_processed_size'] / 1024 / 1024:.2f} MB")
        self.logger.info(f"Total download time: {self.stats['total_download_time']:.2f} seconds")
        self.logger.info(f"Total processing time: {self.stats['total_processing_time']:.2f} seconds")



# Example usage:
if __name__ == "__main__":
    from main_parameters import(
        SEC_USER_AGENT,
        SEC_MASTER_URLS, SEC_RATE_LIMIT,
    )

    def example_processor(content: bytes) -> bytes:
        # This is a placeholder processor function
        return content.upper()

    DATA_RAW_FOLDER = Path("temp_downloads")
    MASTER_INDEX_PREFIX = "master_index"

    sec_index_downloader = EfficientDownloader(
        urls=SEC_MASTER_URLS,
        user_agent=SEC_USER_AGENT,
        rate_limit=SEC_RATE_LIMIT,
        download_dir=DATA_RAW_FOLDER,
        archive_prefix=MASTER_INDEX_PREFIX,
        # process_func=example_processor,
    )

    sec_index_downloader.download_and_process()