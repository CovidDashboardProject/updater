
import gspread

from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen

from oauth2client import client
from oauth2client.service_account import ServiceAccountCredentials
import gspread_dataframe as gd

from datetime import date
import datetime

import pandas as pd
import requests



def RI_data_updater():

 def connect_to_db(credentials):

    scope = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials,scope)
    client = gspread.authorize(creds)

    return client

 def get_data_sheet(client,sheet_name):

        return client.open(sheet_name).sheet1

 def get_sheet(sheet_name):

        credentials = 'credentials/creds.json'

        client = connect_to_db(credentials)

        sheet = get_data_sheet(client,sheet_name)

        return sheet

 def get_dataframe(sheet_name):
    sheet = get_sheet(sheet_name)
    df = pd.DataFrame(sheet.get_all_records())
    df['date'] = df['Date']
    df.set_index('Date', inplace=True)
    return df

 #get all the data
 cessi_df = get_dataframe(sheet_name = 'cessi_data_file')
 pred_df = get_dataframe(sheet_name = 'Predictions')

 sheet = get_sheet(sheet_name = 'RI_Calculation')
 RI_df = pd.DataFrame(sheet.get_all_records())
 RI_df['date'] = RI_df['Date']
 RI_df.set_index('Date', inplace=True)

 # check if there is something to update
 last_RI_date = datetime.datetime.strptime(RI_df['date'][-1],'%Y-%m-%d')
 last_cessi_date = datetime.datetime.strptime(cessi_df['date'][-1],'%Y-%m-%d')

 if last_RI_date < last_cessi_date:

    i = 1

    #append all the record 
    while True:
        Date = str(last_RI_date + datetime.timedelta(days=i)).split(' ')[0]

        row = [Date,str(cessi_df.loc[Date]['dailyconfirmed']),str(pred_df.loc[Date]['Pred'])]

        sheet.append_row(row)

        if Date == str(last_cessi_date).split(' ')[0]:
            break

        i+=1

 else:
    print("nothing to do here (@ _ @)")

  
if __name__ == '__main__':

    #start the process
    RI_data_updater()