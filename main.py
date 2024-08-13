#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
main.py

This is the main entry point of the program. It orchestrates the execution of various 
tasks by utilizing different modules such as downloading master index data from the SEC,
filtering the index for weblinks to 13D and 13F filings, parsing those filings to extract
a mapping between CIK and CUSIP, and post-processing that data.

Usage:
    Run this script directly to start the program.
"""
import asyncio
import dl_idx
from dl import download_indexed_filing_types
import parse_cusip_html
import post_proc

def main():

    dl_idx.download_sec_index_of_filings()          # Download master index of SEC filings from the EDGAR system
    dl_idx.filter_sec_index_of_filings_to_csv()     # Filter master index of SEC filings for selected filing types

    asyncio.run(download_indexed_filing_types())    # Download SEC filings based on urls sourced the master index archive

    # parse_cusip_html.parse_filings_type_list()          # Parse SEC filings to create mapping between CIK and CUSIP for each filing type

    # post_proc.consolidate_and_clean_cik_cusip_map()     # Consolidate and clean CIK-CUSIP

if __name__ == '__main__':
    main()