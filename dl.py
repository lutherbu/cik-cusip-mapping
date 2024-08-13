#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
dl.py

This module downloads an explicit list of SEC filings, sourced from the master
index reference data.

"""
from pathlib import Path
import csv
import asyncio
from typing import List

from utils_internet import EfficientDownloader

from main_parameters import (
    SEC_USER_AGENT,
    SEC_FILINGS_URL,
    SEC_RATE_LIMIT,
    FILING_TYPES,
    FILINGS_DIRECTORIES,
    FILTERED_INDEX_FILE,
)

def generate_filepath(row, base_directory):
    cik = row["cik"].strip()
    date = row["date"].strip()
    year, month = date.split("-")[:2]
    sec_file_name = row["filename"].strip()
    accession_stub = sec_file_name.split(".")[0].split("-")[-1]      # accession number -> unique document identifier... this takes the last segment

    download_directory = base_directory / f"{year}_{month}"
    download_file_path = download_directory / f"{cik}_{date}_{accession_stub}.txt"
    return sec_file_name, download_file_path


def process_filing(content: bytes) -> bytes:
    # This is a placeholder for the light processing mentioned
    # You can implement your regex pattern matching here
    return content

async def download_indexed_filing_types():
    for idx, filing_type in enumerate(FILING_TYPES):
        base_directory = Path(FILINGS_DIRECTORIES[idx])
        
        urls = []
        filepaths = []
        
        # Read filtered SEC index file, a CSV, to gather remote urls and local filepaths
        with FILTERED_INDEX_FILE.open(mode="r") as file:
            reader = csv.DictReader(file)
            for row in reader:
                if filing_type in row["form"]:
                    sec_file_name, download_file_path = generate_filepath(row, base_directory)
                    urls.append(SEC_FILINGS_URL.format(filename=sec_file_name))
                    filepaths.append(download_file_path)

        num_filings = len(urls)
        print(f"Downloading {num_filings} {filing_type} filings...")

        downloader = EfficientDownloader(
            urls=urls,
            download_dir=base_directory,
            archive_prefix=f"{filing_type}_filings",
            process_func=process_filing,
            user_agent=SEC_USER_AGENT,
            rate_limit=SEC_RATE_LIMIT,
        )
        
        await downloader.download_all()
        downloader.create_archive()
        downloader.log_failures()
        downloader._print_summary_stats()

if __name__ == "__main__":
    asyncio.run(download_indexed_filing_types())