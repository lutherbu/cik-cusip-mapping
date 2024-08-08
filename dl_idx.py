#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import datetime
import csv
import re
import requests

from main_parameters import(
    EDGAR_USER_AGENT,
    SEC_MASTER_URL, SEC_RATE_LIMIT,

    FILING_TYPES, MASTER_INDEX_FILE, FILTERED_INDEX_FILE,

    START_YEAR, START_QUARTER,
)

current_year = datetime.datetime.now().year
current_month = datetime.datetime.now().month
current_quarter = (current_month - 1) // 3 + 1


def download_master_index_of_filings():

    # save MASTER INDEX FILE
    with MASTER_INDEX_FILE.open("wb") as idxfile:

        # time-checks to respect the SEC's required rate-limit of 10 requests per second
        last_check_time = time.time()

        for year in range(START_YEAR, current_year+1):      # <-- test period. Widest should be (1994, current_year+1)
            start_qrt = START_QUARTER if year == START_YEAR else 1
            for quarter in range(start_qrt, 5):           # <-- test period. Otherwise range(1, 5)
                print(year, quarter)

                current_time = time.time()
                while (current_time - last_check_time) < 1/SEC_RATE_LIMIT:  # respect the SEC rate limit
                    current_time = time.time()
                    pass

                if (year == current_year and quarter > current_quarter) or (year > current_year):
                    break

                content = requests.get(
                    SEC_MASTER_URL.format(year=year, quarter=quarter),
                    headers=EDGAR_USER_AGENT,
                ).content

                last_check_time = time.time()
                
                idxfile.write(content)


def filter_master_index_of_filings():
    with FILTERED_INDEX_FILE.open(mode="w", errors="ignore") as csvfile:
        wr = csv.writer(csvfile)
        wr.writerow(["cik", "comnam", "form", "date", "filename"])

        # RegEx pattern to select for all partial-matches of the "form" field with selected filing types
        pattern = re.compile("|".join(FILING_TYPES), re.IGNORECASE)

        with MASTER_INDEX_FILE.open(mode="r", encoding="latin1") as idxfile:
            for this_row in idxfile:

                if ".txt" in this_row:
                    fields = this_row.strip().split("|")

                    if pattern.search(fields[2]):
                        wr.writerow(fields)


if __name__ == "__main__":
    download_master_index_of_filings()
    filter_master_index_of_filings()