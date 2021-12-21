
import gspread

from oauth2client import client
from oauth2client.service_account import ServiceAccountCredentials

import pandas as pd
import requests
from datetime import datetime

def cessi_data_updater():

    date_to_month = {'01' : 'Jan',
                    '02' : 'Feb',
                    '03' : 'Mar',
                    '04' : 'Apr',
                    '05' : 'May',
                    '06' : 'Jun',
                    '07' : 'Jul',
                    '08' : 'Aug',
                    '09' : 'Sep',
                    '10' : 'Oct',
                    '11' : 'Nov',
                    '12' : 'Dec'}

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


    def trip(data):
        count = 0

        for date in list(data.Date):
            
            if date == '15.3.2021':
                break

            count+=1

        data  = data.tail(len(data)-335)

        return data 


    def drop_null_columns(data):

        def count_nan(Column): 
            count = 0
            for value in Column:
                if value == None:
                    count+=1
            
            return count


        drop_columns = []
        threshold = 100

        for column in data.columns:
            no_of_nan = count_nan(data[column])

            if no_of_nan > 100:
                drop_columns.append(column)

            
        data.drop(drop_columns,axis = 1,inplace = True)

        return data

    def fill_nan(data):
        columns = data.columns[:-1]

        for column in columns:
            data[column] = pd.to_numeric(data[column], errors='coerce')
        
        data.fillna(data.mean(),inplace = True)
        data = data.round(decimals=0)

        return data

    def download():

        URL = 'http://www.cessi.in/coronavirus/images/model_output/pmc_1.csv'
        req = requests.get(URL)

        url_content = req.content
        csv_file = open('pmc_1.csv', 'wb')

        csv_file.write(url_content)
        csv_file.close()

    def change(x):
        x = x.split('.')

        x = x[2] + '-' + x[1] + '-' + x[0]

        return x

    def get_data():
    
        download()

        df = pd.read_csv('pmc_1.csv',usecols = ['dailysamples', 'dailyconfirmed', 'dailyrecovered', 'dailydeceased',
            'totalcritical', 'totalsamples', 'totalconfirmed', 'totalhospital',
            'totalrecovered', 'totaldeceased','Date'])

        data =  df.where(pd.notnull(df), None)

        #data = trip(data)

        data = drop_null_columns(data)

        data = fill_nan(data)

        #data['Date'] = pd.to_datetime(data['Date'],format = "%d.%m.%Y")

        data['Date'] = data['Date'].apply(lambda x : change(x))


        #data['Date'] = data['Date'].apply(lambda x : str(x.split(' ')[0] ))

        return data 


    def get_sheet():

        credentials = 'credentials/creds.json'
        sheet_name = 'cessi_data_file'

        client = connect_to_db(credentials)

        sheet = get_data_sheet(client,sheet_name)

        return sheet

    def main(): 
        #download the data 

        data = get_data()

        #take the sheet
        sheet = get_sheet()

        #find the last date 
        old_df = pd.DataFrame(sheet.get_all_records())
        count = 1

        for x in list(old_df['dailyconfirmed']):
            count += 1
            if x == '':
                break
        

        #get the data from that date
        len_ = len(data) - count +1
        df = data.tail(len_)


        #add the data into the sheet
        for index, row in df.iterrows():
            sheet.append_row(list(row))

        #count+=1

        #done
        return 

    main()


if __name__ == '__main__':

    #start the process
    cessi_data_updater()