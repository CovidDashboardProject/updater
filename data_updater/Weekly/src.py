import os
import datetime

import matplotlib as mpl
import matplotlib.pyplot as plt

import numpy as np
import pandas as pd

import seaborn as sns
import tensorflow as tf

from datetime import date
import datetime

import gspread

from oauth2client import client
from oauth2client.service_account import ServiceAccountCredentials

# from cessi.download_data import get_data
import pandas as pd
import requests

def download_from_google_drive(id, destination):

      def get_confirm_token(response):
          for key, value in response.cookies.items():
              if key.startswith('download_warning'):
                  return value

          return None


      URL = "https://docs.google.com/uc?export=download"


      def save_response_content(response, destination):
          CHUNK_SIZE = 32768

          with open(destination, "wb") as f:
              for chunk in response.iter_content(CHUNK_SIZE):
                  if chunk: # filter out keep-alive new chunks
                      f.write(chunk)

      session = requests.Session()

      response = session.get(URL, params = { 'id' : id }, stream = True)
      token = get_confirm_token(response)

      if token:
          params = { 'id' : id, 'confirm' : token }
          response = session.get(URL, params = params, stream = True)

      save_response_content(response, destination)



def connect_to_db(credentials):

    scope = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials,scope)
    client = gspread.authorize(creds)

    return client
  
def connect():

  credentials = 'creds.json'
  client = connect_to_db(credentials)

  return client

def get_data_sheet(client,sheet_name):

    return client.open(sheet_name).sheet1


def get_pred_sheet():

    sheet_name = 'Predictions'

    client = connect()

    sheet = get_data_sheet(client,sheet_name)

    return sheet


def get_cessi_data(client):

  cessi_data_sheet = client.open('cessi_data_file').sheet1
  cessi_data = pd.DataFrame(cessi_data_sheet.get_all_records())

  cessi_data['Date'] = pd.to_datetime(cessi_data['Date'],format = '%Y-%m-%d')
  cessi_data = cessi_data.iloc[12:] # to match both of the data ( some of the data in cessi is missing (24-4-2020 and 25-4-2020))
  
  return cessi_data

def get_mobility_data(client):

  mobility_sheet = client.open('MH_mobility').worksheets()

  mobility_data_sheet_2020 = mobility_sheet[1]
  mobility_data_sheet_2021 = mobility_sheet[0]

  mobility_data_2020 = pd.DataFrame(mobility_data_sheet_2020.get_all_records())
  mobility_data_2021 = pd.DataFrame(mobility_data_sheet_2021.get_all_records())

  mobility_data = pd.concat([mobility_data_2020,mobility_data_2021],axis=0)
  mobility_data = mobility_data[mobility_data['sub_region_2'] == 'Pune']
  mobility_data = mobility_data.drop(columns = ['sub_region_1','sub_region_2'])
  
  mobility_data = mobility_data.iloc[71:]
  mobility_data['date'] = pd.to_datetime(mobility_data['date'],format = '%Y-%m-%d')

  return mobility_data


def get_data():

  client = connect()

  cessi_data = get_cessi_data(client)
  mobility_data = get_mobility_data(client)
  
  columns = list(cessi_data.columns) + list(mobility_data.columns)
  final_data = pd.DataFrame(columns = columns)

  for ( _ , row1 ) , (_ , row2) in zip(cessi_data.iterrows(),mobility_data.iterrows()):
    row = list(row1) + list(row2)
    final_data.loc[len(final_data.index)] = row

  final_data.drop(columns = ['date'],inplace = True)

  return final_data