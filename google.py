from __future__ import print_function

import os.path

from apscheduler.schedulers.background import BackgroundScheduler
from pytz import HOUR, timezone
from datetime import datetime
import time
import os
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import gspread
import pandas as pd
import gspread_dataframe as gd
from pandas.core.indexing import maybe_convert_ix

gc = gspread.service_account(filename='creds.json')

def main():
    try:
        service = build('drive', 'v3', credentials=creds)

        # Call the Drive v3 API
        results = service.files().list(q="'1DfhAaRUFycIUz4F0ocimRFWwuMQdJEeZ' in parents",
            pageSize=10, fields="nextPageToken, files(id, name)").execute()
        items = results.get('files', [])

        count = 0
        response = service.files().list(q="'1uj27Kwq14fWOCzUQi1bD53scf7O2LFud' in parents", pageSize=10, fields="nextPageToken, files(id, name)").execute()
        backup_files = response.get('files', [])

        for item in items:
            # print(u'{0} ({1})'.format(item['name'], item['id']))
            wb = gc.open('trial2').sheet1
            master_file_id = '1eHX5Ls8RNDhuaFd8h4HHCcE66K4FgjSeEarKZ9nbMfk'
            backup_folder_id = '1uj27Kwq14fWOCzUQi1bD53scf7O2LFud'

            
            for i in range(len(backup_files)-1, -1, -1):
                if backup_files[i]['name'] == str(count):
                    count = count + 1
                
            file_metadata = {
                'name': str(count),
                'parents': [backup_folder_id],
                'starred': True,
            }

            service.files().copy(
                fileId=master_file_id,
                body=file_metadata
            ).execute()

            rows = wb.get_all_values()
            if (rows != []):
                df_old = pd.DataFrame.from_records(rows[1:],columns=rows[0])
                df_old[df_old.columns] = df_old.apply(lambda x: x.str.strip())
                df_old['HPNO'] = df_old['HPNO'].map(lambda x: x.lstrip('+-'))
            new_file = gc.open_by_key(item['id'])
            new_rows = new_file.get_worksheet(0).get_all_values()
            df = pd.DataFrame.from_records(new_rows[1:],columns=new_rows[0])
            df[df.columns] = df.apply(lambda x: x.str.strip())
            df['HPNO'] = df['HPNO'].map(lambda x: x.lstrip('+-'))
            if (rows == []):
                df_old = df
                merged = df_old
            else:
                if (df_old.equals(df)):
                    merged = df_old
                else:
                    merged = df_old.merge(df,how='outer',on=['IC','Nama'])
            merged.fillna('',inplace=True)
            for column in merged:
                if ("_x" in column):
                    name = column.replace("_x","")
                    merged.rename(columns={column:name},inplace=True)
                    del merged[name+"_y"]
            merged.fillna('',inplace=True)
            if ('å§“å' in merged.columns):
                del merged['å§“å']
            if ('Tel No' in merged.columns):
                del merged['Tel No']
            wb.clear()
            merged.drop_duplicates(subset=['IC'],inplace=True)
            gd.set_with_dataframe(wb,merged)
            # print('Done merging')

        move_folder_id = '1DfhAaRUFycIUz4F0ocimRFWwuMQdJEeZ'
        target_folder_id = '1TPPQ86IXxbeyKaHRZaMMNIbCLo3ovXQq'
        response2 = service.files().list(q=f"parents = '{move_folder_id}'", fields="nextPageToken, files(id, name)").execute()
        move_files = response2.get('files', [])

        for f in move_files:
            service.files().update(
                fileId=f.get('id'),
                addParents=target_folder_id,
                removeParents=move_folder_id
            ).execute()
        
        print("All done")
        
    except HttpError as error:
        # TODO(developer) - Handle errors from drive API.
        print(f'An error occurred: {error}')

if __name__ == '__main__':
    main()
    scheduler = BackgroundScheduler(timezone="Asia/Kuala_Lumpur")
    scheduler.add_job(main, 'interval', hours=24)
    scheduler.start()

    try:
        # This is here to simulate application activity (which keeps the main thread alive).
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        # Not strictly necessary if daemonic mode is enabled but should be done if possible
        scheduler.shutdown()
