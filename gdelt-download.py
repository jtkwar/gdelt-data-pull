"""
This is a .py file for downloading files from the GDELT project
Processing of the files can be found in other .py files

Approach:
- Go to master text file for GDELT Project v2 (Our Global World in Realtime)
- Download and process all lines in the file.
    - Should have a start date file of the master list to compare against the latest pull and update the master file so you could easily tell what is available to download (if you do not have the whole archive)
    - Master file is how you actually go and download the data files in the project
- View files available on local machine
- Choose files you want to download
    - Grab latest files
    - Grab all missing files, if updating a large local repo
    - Grab all files for a single day
    - Grab files for a date range
- For the Chosen Files
    - Download the files
    - Open the files into a DataFrame
        - Drop bad lines and record in a log file
        - Add column names to file
        - Save data as a .parquet file
        - Delete original csv file
    - File Structure:
        - /data
            - /YYYY-MM-DD
                - /exports
                - /mentions
                - /gkg
TODO: Conduct more cleaning of the data, if needed.  Will require some deeper dives into the data to determine need.

"""

import os
import sys
import urllib
from urllib.request import urlopen
import numpy as np
import pandas as pd
import requests
from tqdm import tqdm
import typing
import wget
import zipfile
from datetime import date, timedelta, datetime
import sys
import csv
# Incorporated to load the proper sized 
maxInt = sys.maxsize
while True:
    # decrease the maxInt value by factor 10 
    # as long as the OverflowError occurs.

    try:
        csv.field_size_limit(maxInt)
        break
    except OverflowError:
        maxInt = int(maxInt/10)

# url variable - master list of all data files collected and stored by GDELT Project
master_url="http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"

def download_master_file(url: str) -> pd.DataFrame:
    try:
        print(f"Scraping Master File List from: {url}")
        text = requests.get(url).text.splitlines()
    except: Exception as e: print(e)
    split_text = []
    print("Starting Line Splitting of Master Archive File...")
    for i in tqdm(range(len(text)), desc="Line Splitting Progress"):
        split_line = text[i].split(' ')
        split_text.append(split_line)
    master_file_df = pd.DataFrame(split_text)
    return master_file_df

def save_master_file(master_file_df: pd.DataFrame):
    save_path = os.path.join(dir_path, "master_archive", "master_archive.csv")
    master_file_df.to_csv(save_path)
    print(f"Master Archive File Saved to: {save_path}")

def check_update_master_file(dir_path: str, url: str):
    master_archive_path = os.path.join(dir_path, "master_archive", "master_archive.csv")
    og_master = pd.read_csv(master_archive_path)
    new_master = download_master_file(url)

    if new_master.shape[0] > og_master.shape[0]:
        save_master_file(new_master)
    else:
        print("There are presently no additional files in the repository.")



def download_single_day(date: str, master_archive: pd.DataFrame, type: str, dir_path: str):
    date_string = "gdeltv2/" + date
    mask = (master_archive[2].str.contains(date_string, na=False) & master_archive[2].str.contains(type, na=False))
    single_day = master_archive[mask]
    urls = single_day[2].to_list()
    date_path = os.path.join(dir_path, f"data\{date}")
    if not os.path.exists(date_path):
        os.makedirs(date_path)
    type_single_day_path = os.path.join(date_path, f"{type}")
    if not os.path.exists(type_single_day_path):
        os.makedirs(type_single_day_path)
    out_path = type_single_day_path
    for i in tqdm(range(len(urls)), desc=f"{date} {type} download progress"):
        wget.download(urls[i], type_single_day_path)
    print(f"Download of {date} {type} data complete")
    return out_path

def extract_zip(path: str):
    
    _EXPORT_COLUMN_NAMES = [
                            'GlobalEventID',
                            'Day',
                            'MonthYear',
                            'Year',
                            'FractionDate',
                            'Actor1Code',
                            'Actor1Name',
                            'Actor1CountryCode',
                            'Actor1KnownGroupCode',
                            'Actor1EthnicCode',
                            'Actor1Religion1Code',
                            'Actor1Religion2Code',
                            'Actor1Type1Code',
                            'Actor1Type2Code',
                            'Actor1Type3Code',
                            'Actor2Code',
                            'Actor2Name',
                            'Actor2CountryCode',
                            'Actor2KnownGroupCode',
                            'Actor2EthnicCode',
                            'Actor2Religion1Code',
                            'Actor2Religion2Code',
                            'Actor2Type1Code',
                            'Actor2Type2Code',
                            'Actor2Type3Code',
                            'IsRootEvent',
                            'EventCode',
                            'EventBaseCode',
                            'EventRotCode',
                            'QuadClass',
                            'GoldsteinScale',
                            'NumMenions',
                            'NumSources',
                            'NumArticles',
                            'AvgTone',
                            'Actor1Geo_Type',
                            'Actor1Geo_FullName',
                            'Actor1Geo_CountryCode',
                            'Actor1Geo_ADM1Code',
                            'Actor1Geo_ADM2Code',
                            'Actor1Geo_Lat',
                            'Actor1Geo_Lon',
                            'Actor1Geo_FeatureID',
                            'Actor2Geo_Type',
                            'Actor2Geo_FullName',
                            'Actor2Geo_CountryCode',
                            'Actor2Geo_ADM1Code',
                            'Actor2Geo_ADM2Code',
                            'Actor2Geo_Lat',
                            'Actor2Geo_Lon',
                            'Actor2Geo_FeatureID',
                            'DATEADDED',
                            'SOURCEURL'
                           ]
    _MENTIONS_COLUMN_NAMES = [
                              'GlobalEventID',
                              'EventTimeDate',
                              'MentionTimeDate',
                              'MentionType',
                              'MentionSourceName',
                              'MentionIdentifer',
                              'SentenceID',
                              'Actor1CharOffset',
                              'Actor2CharOffset',
                              'ActionCharOffset',
                              'InRawText',
                              'Confidence',
                              'MentionDocLen',
                              'MentionDocTone',
                              'MentionTranslationInfo',
                              'Extras'
                             ]
    _GKG_COLUMN_NAMES = [
                            'GlobalEventID',
                            'Day',
                            'MonthYear',
                            'Year',
                            'FractionDate',
                            'Actor1Code',
                            'Actor1Name',
                            'Actor1CountryCode',
                            'Actor1KnownGroupCode',
                            'Actor1EthnicCode',
                            'Actor1Religion1Code',
                            'Actor1Religion2Code',
                            'Actor1Type1Code',
                            'Actor1Type2Code',
                            'Actor1Type3Code',
                            'Actor2Code',
                            'Actor2Name',
                            'Actor2CountryCode',
                            'Actor2KnownGroupCode',
                            'Actor2EthnicCode',
                            'Actor2Religion1Code',
                            'Actor2Religion2Code',
                            'Actor2Type1Code',
                            'Actor2Type2Code',
                            'Actor2Type3Code',
                            'IsRootEvent',
                            'EventCode',
                            'EventBaseCode',
                            'EventRotCode',
                            'QuadClass',
                            'GoldsteinScale',
                            'NumMenions',
                            'NumSources',
                            'NumArticles',
                            'AvgTone',
                            'Actor1Geo_Type',
                            'Actor1Geo_FullName',
                            'Actor1Geo_CountryCode',
                            'Actor1Geo_ADM1Code',
                            'Actor1Geo_ADM2Code',
                            'Actor1Geo_Lat',
                            'Actor1Geo_Lon',
                            'Actor1Geo_FeatureID',
                            'Actor2Geo_Type',
                            'Actor2Geo_FullName',
                            'Actor2Geo_CountryCode',
                            'Actor2Geo_ADM1Code',
                            'Actor2Geo_ADM2Code',
                            'Actor2Geo_Lat',
                            'Actor2Geo_Lon',
                            'Actor2Geo_FeatureID',
                            'DATEADDED',
                            'SOURCEURL'
    ]
    files = os.listdir(path)
    print(f"Unzipping and saving files as parquet in {path}")
    for i in tqdm(range(len(files)), desc='Unzipping/Saving Progress'):
        file = files[i]
        file_path = os.path.join(path, file)
        save_name = "-".join(file.split(".")[:2]) + ".parquet"
        extracted_data = pd.read_csv(file_path,
                                     compression="zip",
                                     header=None,
                                     delimiter="\t",
                                     engine='python',
                                     encoding_errors='ignore'
                                    )
        extracted_data.columns = [str(x) for x in extracted_data.columns.to_list()]
        extracted_data.to_parquet(os.path.join(path, save_name))
    print("Unziping and saving completed.")
    print(f"Removing zip files from {path}")
    for i in tqdm(range(len(files)), desc="Removing zipfiles progress"):
        file_to_be_removed_path = os.path.join(path, files[i])
        os.remove(file_to_be_removed_path)
    print(f"Zipped files removed in {path}")

def download_date_range(start_date: str, end_date: str, master_archive: pd.DataFrame, dir_path: str):
    
    start_dt = datetime.strptime(start_date, "%Y%m%d").date()
    end_dt = datetime.strptime(end_date, "%Y%m%d").date()
    t_delta = end_dt - start_dt
    days = []
    for i in range(delta.days + 1):
        day = start_dt + timedelta(days=i)
        day = day.strftime("%Y%m%d")
        days.append(day)
    print(days)
    for day in days:
        day_export_path = download_single_day(day, master_archive, 'export', dir_path)
        extract_zip(path=day_export_path)
        day_mentions_path = download_single_day(day, master_archive, 'mentions', dir_path)
        extract_zip(path=day_mentions_path)
        day_gkg_path = download_single_day(day, master_archive, 'gkg', dir_path)
        extract_zip(path=day_gkg_path)


class GdeltDataDownloader:
    
    _VALID_DOWNLOAD_METHODS = ['all', 'single_day', 'date_range', 'update_all']
    
    def __init__(
        self,
        download_method: str,
        master_archive: pd.DataFrame,
        ) -> None:
        pass


