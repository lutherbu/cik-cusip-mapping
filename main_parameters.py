#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
main_parameters.py

This module contains the main parameters required for the execution of the program. 
It serves as a configuration file where various constants and parameters are defined 
to be used across different modules.

NOTE:
With an original objective to find unique CIK-CUSIP pairs for common stock issuances,
we filter for 13D and 13G filings only. There are over 300 types of SEC filings of which
8 filings match type '13D' and '13G'.

This was taken as given without need for other issuances.

Hence, the starting point is SEC filings filtered for 13D and 13G.

"""
from pathlib import Path    # For handling and manipulating filesystem paths
import re                   # For regular expressions, useful in pattern matching and text processing

#########################
#### USER PARAMETERS ####
#########################
# USER CHOICE: SEC filing types
FILING_TYPES = ["13D", "13G"]               # SEC filing types for common stock

# Initial calendar quarter for downloads of master-filings-index and filings
START_YEAR, START_QUARTER = (2024, 3)       # can be as early as (1994, 1)

############################
#### SOURCE INFORMATION ####
############################
EDGAR_USER_AGENT = {
    'User-Agent': 'ACME Co jane.smith@acme.co',         # <-- Your info here. Required by SEC's EDGAR system
    'Accept-Encoding': 'deflate',
    'Host': 'www.sec.gov'
}

# URL template for quarterly master index archives
SEC_MASTER_URL = """https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/master.idx"""     # dl_idx.py
# SEC_MASTER_URL.format(year=2024, quarter=3)

# URL template for SEC filings, where filename has the format CIK/
SEC_FILINGS_URL = """https://www.sec.gov/Archives/{filename}"""                                         # dl.py
# SEC_FILINGS_URL.format(filename="edgar/data/1000694/0000093751-24-000650.txt")

# SEC's EDGAR system prohibits http requests faster than 10-per-second
SEC_RATE_LIMIT = 10

#####################################
#### DATA PROCESSING INFORMATION ####
#####################################
# Define helper RegEx patterns
html_tag_rx     = re.compile(r'<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});')
html_junk_rx    = re.compile(r'''["].*["]|=#.*\d+''')
wordchar_rx     = re.compile(r'\w+')
cusip_rx        = re.compile(r'[\( >]*[0-9A-Z]{1}[0-9]{3}[0-9A-Za-z]{2}[- ]*[0-9]{0,2}[- ]*[0-9]{0,1}[\) \n<]*')

#################################
#### DESTINATION INFORMATION ####
#################################
# Get the directory where the current script is located
SCRIPT_DIR = Path(__file__).resolve().parent

DATA_DIRECTORY = SCRIPT_DIR / "data_dir"
DATA_DIRECTORY.mkdir(parents=True, exist_ok=True)

# Processed filings will be saved subdirectories by filing type
FILINGS_DIRECTORIES = [DATA_DIRECTORY / f"{x}_filings" for x in FILING_TYPES]  # e.g. ['13D_filings', '13G_filings', ...]

# Helper filepaths for processing the EDGAR's MASTER INDEX OF SEC FILINGS
MASTER_INDEX_FILE       = DATA_DIRECTORY / "master_index_1_raw.idx"             # concatenated historical archive (write: dl_idx.py)
FILTERED_INDEX_FILE     = DATA_DIRECTORY / "master_index_2_filtered.csv"        # historical archive, filtered for chosen filing types (write: dl_idx.py, read: dl2.py)

# Consolidated mapping file for selected filing types 
FINAL_OUTPUT_CSV = DATA_DIRECTORY / 'cik-cusip-maps.csv'    # (write: post_proc.py)
FINAL_OUTPUT_JSON = DATA_DIRECTORY / 'cik-cusip-maps.json'  # (write: post_proc.py)