#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
dl.py

This module downloads an explicit list of SEC filings, sourced from the master
index reference data.

"""
from pathlib import Path    # For handling and manipulating filesystem paths
import csv                  # For reading from and writing to CSV files
import time                 # For time-related functions such as sleeping or measuring durations
import requests             # For making HTTP requests, useful for downloading data from the web

import threading
import time
from concurrent.futures import ThreadPoolExecutor

from main_parameters import(
    SEC_USER_AGENT,
    SEC_FILINGS_URL,
    SEC_RATE_LIMIT, MAX_THREADS,
    FILING_TYPES, FILINGS_DIRECTORIES,
    FILTERED_INDEX_FILE,
)


class RateLimiter:
    """A thread-safe rate limiter to ensure adherence to SEC's rate limit."""
    def __init__(self, rate_limit):
        self.rate_limit = rate_limit
        self.last_request_time = 0
        self.lock = threading.Lock()


    def wait(self):
        """Wait if necessary to comply with the rate limit."""
        with self.lock:
            current_time = time.time()
            time_since_last_request = current_time - self.last_request_time
            if time_since_last_request < 1 / self.rate_limit:
                time.sleep((1 / self.rate_limit) - time_since_last_request)
            self.last_request_time = time.time()


rate_limiter = RateLimiter(SEC_RATE_LIMIT)  # No more than 10 requests per second


def download_file(row, base_directory):
    cik = row["cik"].strip()
    date = row["date"].strip()
    year, month = date.split("-")[:2]
    filename = row["filename"].strip()
    accession_stub = filename.split(".")[0].split("-")[-1]      # accession number -> unique document identifier... this takes the last segment

    download_directory = base_directory / f"{year}_{month}"
    download_directory.mkdir(parents=True, exist_ok=True)
    download_file = download_directory / f"{cik}_{date}_{accession_stub}.txt"

    if download_file.exists():
        return

    rate_limiter.wait()  # Wait for rate limit compliance

    try:
        response = requests.get(
            SEC_FILINGS_URL.format(filename=filename),
            headers=SEC_USER_AGENT,
            timeout=5.0
        )
        if response.status_code == 200:
            with download_file.open(mode="w", errors="ignore") as file:
                file.write(response.text)
        else:
            print(f"Failed to download: {cik}, {date}")
    except Exception as e:
        print(f"Error downloading {cik}, {date}: {str(e)}")

    # TODO: output errors to csv

def download_manager(download_filings_list, base_directory):
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = [executor.submit(download_file, row, base_directory) for row in download_filings_list]
        for i, future in enumerate(futures, 1):
            future.result()  # Wait for the future to complete
            print(f"Completed {i+1} out of {len(download_filings_list)+1}")


def download_indexed_filing_types():

    for idx, filing_type in enumerate(FILING_TYPES):

        base_directory = Path(FILINGS_DIRECTORIES[idx])

        download_filings_list = []
        # Read MASTER INDEX FILE 
        with FILTERED_INDEX_FILE.open(mode="r") as file:
            reader = csv.DictReader(file)
            for row in reader:
                if filing_type in row["form"]:
                    download_filings_list.append(row)

        num_filings = len(download_filings_list)
        print(num_filings)
        print("Begin download of SEC filings...")

        # simple time-checks to respect the SEC's required rate-limit of 10 requests per second
        last_check_time = time.time()

        download_manager(download_filings_list, base_directory)

if __name__ == "__main__":
    download_indexed_filing_types()