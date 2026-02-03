import os
import json
import datetime
import io
import pandas as pd
from garminconnect import Garmin
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError

def flatten_json(y):
    out = {}
    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            out[name[:-1]] = json.dumps(x)
        else:
            out[name[:-1]] = x
    flatten(y)
    return out

def get_garmin_data():
    api = Garmin(os.getenv("GARMIN_EMAIL"), os.getenv("GARMIN_PASSWORD"))
    api.login()
    target_date = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
    print(f"Fetching data for {target_date}...")

    # Fetch Data Buckets
    try: daily_summary = api.get_stats(target_date)
    except: daily_summary = {}
    try: sleep_data = api.get_sleep_data(target_date)
    except: sleep_data = {}
    try: hrv_data = api.get_hrv_data(target_date)
    except: hrv_data = {}
    
    # Flatten Data
    row = {"Date": target_date}
    row.update(flatten_json(daily_summary))
    row.update({f"sleep_{k}": v for k, v in flatten_json(sleep_data).items()})
    row.update({f"hrv_{k}": v for k, v in flatten_json(hrv_data).items()})

    try:
        activities = api.get_activities(0, 20)
        daily_acts = [a for a in activities if a['startTimeLocal'].startswith(target_date)]
        row["All_Activities_Raw"] = json.dumps(daily_acts)
    except:
        row["All_Activities_Raw"] = "[]"

    return row

def sync_to_drive(new_entry):
    file_id = os.getenv("DRIVE_FILE_ID")
    service_account_info = json.loads(os.getenv("GDRIVE_JSON_KEY"))
    
    creds = service_account.Credentials.from_service_account_info(
        service_account_info, scopes=['https://www.googleapis.com/auth/drive']
    )
    service = build('drive', 'v3', credentials=creds)

    print("Downloading existing history...")
    df_existing = pd.DataFrame()
    
    # --- SAFETY LOGIC START ---
    try:
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
        fh.seek(0)

        # Only try to read if we actually got bytes back
        if fh.getbuffer().nbytes > 0:
            try:
                df_existing = pd.read_csv(fh)
                print(f"Found {len(df_existing)} previous records.")
            except pd.errors.EmptyDataError:
                print("File exists but is empty. Starting fresh.")
                df_existing = pd.DataFrame()
        else:
            print("File is 0 bytes. Starting fresh.")
            df_existing = pd.DataFrame()

    except HttpError as e:
        print(f"CRITICAL DOWNLOAD ERROR: {e}")
        print("STOPPING SYNC TO PROTECT DATA.")
        # If we can't read the file (e.g., it's a Google Sheet), we abort here.
        return 
    # --- SAFETY LOGIC END ---

    # Append & Deduplicate
    df_new = pd.DataFrame([new_entry])
    df_combined = pd.concat([df_existing, df_new], sort=False).drop_duplicates(subset=['Date'], keep='last')
    
    # Sort
    if 'Date' in df_combined.columns:
        df_combined['Date'] = pd.to_datetime(df_combined['Date'])
        df_combined = df_combined.sort_values('Date')
    
    print(f"Uploading new history ({len(df_combined)} rows)...")
    df_combined.to_csv("sync.csv", index=False)
    
    media = MediaFileUpload("sync.csv", mimetype='text/csv', resumable=False)
    service.files().update(fileId=file_id, media_body=media).execute()
    print("Success.")

if __name__ == "__main__":
    try:
        entry = get_garmin_data()
        sync_to_drive(entry)
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
