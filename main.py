#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import dl_idx
import dl
import parse_cusip_html

def main():

    dl_idx.download_master_index_of_filings()           # Download master index of SEC filings from the EDGAR system (~2GB archive for full history)
    dl_idx.filter_master_index_of_filings()             # Filter master index of SEC filings for selected filing types

    dl.download_indexed_filing_types()                 # Download SEC filings based on urls sourced the master index archive

    parse_cusip_html.parse_filings_type_list()          # Parse SEC filings to create mapping between CIK and CUSIP for each filing type

if __name__ == '__main__':
    main()