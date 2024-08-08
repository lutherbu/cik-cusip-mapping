#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import csv
from pathlib import Path
import requests


from main_parameters import(
    EDGAR_USER_AGENT,
    SEC_FILINGS_URL,
    FILING_TYPES, FILINGS_DIRECTORIES,
    FILTERED_INDEX_FILE,
)


def download_indexed_filing_types():

    for idx, filing_type in enumerate(FILING_TYPES):

        BASE_DIRECTORY = Path(FILINGS_DIRECTORIES[idx])

        to_dl = []
        # Read MASTER INDEX FILE (choose formatted or filtered)
        with FILTERED_INDEX_FILE.open(mode="r") as file:
            reader = csv.DictReader(file)
            for row in reader:
                if filing_type in row["form"]:
                    to_dl.append(row)

        len_ = len(to_dl)
        print(len_)
        print("Begin download of SEC filings...")

        # simple time-checks to respect the SEC's required rate-limit of 10 requests per second
        last_check_time = time.time()

        for n, row in enumerate(to_dl):
            print(f"{n} out of {len_}")

            cik = row["cik"].strip()
            date = row["date"].strip()
            year = row["date"].split("-")[0].strip()
            month = row["date"].split("-")[1].strip()
            filename = row["filename"].strip()
            accession = filename.split(".")[0].split("-")[-1]      # accession number -> unique document identifier...

            # Create download folder
            DOWNLOAD_DIRECTORY = BASE_DIRECTORY / f"{year}_{month}"
            DOWNLOAD_DIRECTORY.mkdir(parents=True, exist_ok=True)

            DOWNLOAD_FILE = DOWNLOAD_DIRECTORY / f"{cik}_{date}_{accession}.txt"

            if DOWNLOAD_FILE.exists():
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

                with DOWNLOAD_FILE.open(mode="w", errors="ignore") as file:
                    file.write(txt)
            except:
                print(f"{cik}, {date} failed to download")


if __name__ == "__main__":
    download_indexed_filing_types()