#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
from collections import *
from multiprocessing import Pool

from main_parameters import(
    FILING_TYPES,
    DATA_DIRECTORY,
    html_tag_rx, html_junk_rx, cusip_rx, wordchar_rx,
)


def parse_filing_type(file):
    with file.open('r') as f:
        raw = f.read().replace("<DOCUMENT>", "***BEGIN SEARCH HERE***")
        lines = html_tag_rx.sub('\n', raw).split('\n')

    record = 0
    cik = None
    for line in lines:
        if 'SUBJECT COMPANY' in line:
            record = 1
        if 'CENTRAL INDEX KEY' in line and record == 1:
            cik = line.split('\t\t\t')[-1].strip()
            break

    cusips = []
    record = 0
    for line in lines:

        if '***BEGIN SEARCH HERE***' in line:  # lines are after the document preamble
            record = 1

        if record == 1:
            line = html_junk_rx.sub('', line)
            line = html_tag_rx.sub('', line)

            if 'IRS' not in line and 'I.R.S' not in line:
                fd = cusip_rx.findall(line)

                if fd:
                    cusip = fd[0].strip().strip('<>')
                    cusips.append(cusip)

    if len(cusips) == 0:
        cusip = None
    else:
        cusip = Counter(cusips).most_common()[0][0]
        cusip = ''.join(wordchar_rx.findall(cusip))

    return [file.name, cik, cusip]


def parse_filings_type_list():

    for filing_type in FILING_TYPES:
        output_file = DATA_DIRECTORY / f"{filing_type}-cik-cusip.csv"           # intermediate mappings by filing type

        # Get filepaths of downloaded filings
        filings_list = DATA_DIRECTORY.glob(f"{filing_type}_filings/*/*.txt")

        with Pool(30) as p:
            with output_file.open('w') as w:
                wr = csv.writer(w)
                for res in p.imap(parse_filing_type, filings_list, chunksize=100):
                    wr.writerow(res)


if __name__ == '__main__':
    parse_filings_type_list()