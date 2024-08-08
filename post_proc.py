#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd

from main_parameters import(
    DATA_DIRECTORY,
    FINAL_OUTPUT_CSV, FINAL_OUTPUT_JSON,
)

# Grab list of intermediate cik-cusip maps (one for each filing type)
cik_cusip_maps_by_type = DATA_DIRECTORY.glob(f"*-cik-cusip.csv")

def consolidate_and_clean_cik_cusip_map(cik_cusip_maps=cik_cusip_maps_by_type):

    df = [pd.read_csv(cik_cusip_map, names=['f', 'cik', 'cusip']).dropna() for cik_cusip_map in cik_cusip_maps]
    df = pd.concat(df)

    df['leng'] = df.cusip.map(len)

    df = df[(df.leng == 6) | (df.leng == 8) | (df.leng == 9)]

    df['cusip6'] = df.cusip.str[:6]

    df = df[df.cusip6 != '000000']
    df = df[df.cusip6 != '0001pt']

    df['cusip8'] = df.cusip.str[:8]

    df.cik = pd.to_numeric(df.cik)

    df.drop_duplicates().reset_index(drop=True, inplace=True)

    # Write-out to CSV
    df[['cik', 'cusip6', 'cusip8']].to_csv(FINAL_OUTPUT_CSV, index=False)

    # Write-out to JSON
    df[['cik', 'cusip6', 'cusip8']].to_json(FINAL_OUTPUT_JSON)


if __name__ == "__main__":
    consolidate_and_clean_cik_cusip_map()