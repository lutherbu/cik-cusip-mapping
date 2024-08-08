# cik-cusip-mapping
Forked from [leoliu0/cik-cusip-mapping](https://github.com/leoliu0/cik-cusip-mapping), with originally stated purpose:

This repository produces the link between cik and cusip using EDGAR 13D and 13G fillings, that is more robust than Compustat (due to backward filling of new cusip to old records). It is a competitor to WRDS SEC platform while this one is free.

# This project...
Aims to streamline original project: to download, parse, and process SEC filings archives to extract CIK-CUSIP mappings.

## Modules

#### `main_parameters.py`
- Defines global configuration settings

#### `main.py`
- Entry point for program execution
- Orchestrates workflow across modules

#### `dl_idx.py`
- Downloads _reference data_ (SEC's master index archive) to archived SEC filing documents

#### `dl.py`
- Handles downloads of _SEC filings_, based on references sourced from the master index archive. 

#### `parse_cusip_html.py`
- Extracts CIK-CUSIP pairs (mappings) from the SEC filings by scraping their HTML-like stucture.

#### `post_proc.py`
- Performs light data cleaning and pruning of CIK-CUSIP mappings. Exports as CSV and JSON.

## Workflow
1) Set parameters in `main_parameters.py`
2) Execute `main.py` which will:
  - Download data using `dl_idx.py` (master index) and `dl.py` (SEC filings, type 13D and 13G)
  - Parse CIK-CUSIP mappings with `parse_cusip_html.py` from SEC filings
  - Lightly prune mappings results using `post_proc.py`

### Dependencies
- pathlib's Path  # For handling and manipulating filesystem paths
- re  # For regular expressions, useful in pattern matching and text processing
- time  # For time-related functions such as sleeping or measuring durations
- datetime  # For handling dates and times
- csv  # For reading from and writing to CSV files
- requests  # For making HTTP requests, useful for downloading data from the web
- collections  # For specialized container datatypes (e.g., namedtuple, deque)
- multiprocessing's Pool  # For parallel processing using multiple processes
- pandas  # For data manipulation and analysis

MIT License
Copyright (c) [2024] [@flatly1140]

