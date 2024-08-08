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


from main_parameters import(
    EDGAR_USER_AGENT,
    SEC_FILINGS_URL,
    FILING_TYPES, FILINGS_DIRECTORIES,
    FILTERED_INDEX_FILE,
)


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

        for n, row in enumerate(download_filings_list):
            print(f"{n} out of {num_filings}")

            cik = row["cik"].strip()
            date = row["date"].strip()
            year = row["date"].split("-")[0].strip()
            month = row["date"].split("-")[1].strip()
            filename = row["filename"].strip()
            accession = filename.split(".")[0].split("-")[-1]      # accession number -> unique document identifier...

            # Create download folder
            download_directory = base_directory / f"{year}_{month}"
            download_directory.mkdir(parents=True, exist_ok=True)

            FILING_DOCUMENT = download_directory / f"{cik}_{date}_{accession}.txt"

            if FILING_DOCUMENT.exists():
                continue

            try:
                current_time = time.time()

                while (current_time - last_check_time) < 0.1:
                    current_time = time.time()
                    pass

                txt = requests.get(
                    SEC_FILINGS_URL.format(filename=filename), headers=EDGAR_USER_AGENT, timeout=3.0
                ).text

                last_check_time = time.time()

                with FILING_DOCUMENT.open(mode="w", errors="ignore") as file:
                    file.write(txt)
            except:
                print(f"{cik}, {date} failed to download")


if __name__ == "__main__":
    download_indexed_filing_types()