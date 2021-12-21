
import gspread


from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen


from oauth2client import client
from oauth2client.service_account import ServiceAccountCredentials
import gspread_dataframe as gd


import pandas as pd
import requests




def mobility_data_updater():

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


	def download():

		resp = urlopen('https://www.gstatic.com/covid19/mobility/Region_Mobility_Report_CSVs.zip')

		zipfile = ZipFile(BytesIO(resp.read()))

		with zipfile.open('2021_IN_Region_Mobility_Report.csv') as input:
			
			with open("MH_mobility_2021.csv", "w") as output:

				for line in input:
					output.write(line.decode('utf-8'))




	def get_data():
	
		download()

		df = pd.read_csv('MH_mobility_2021.csv').drop(columns = ['place_id','census_fips_code','iso_3166_2_code','metro_area','country_region_code','country_region'])

		#consider only maharashtra 
		df = df[df['sub_region_1'] == 'Maharashtra']

		#remove the rows with nan sub_region
		df['sub_region_2'] = df['sub_region_2'].fillna(0)
		df = df.drop(df[(df['sub_region_2'] == 0)].index)
		
		return df


	def get_sheet():

		credentials = 'credentials/creds.json'
		sheet_name = 'MH_mobility'

		client = connect_to_db(credentials)

		sheet = get_data_sheet(client,sheet_name)

		return sheet

	def main(): 

		#download the data 
		data = get_data()

		#take the sheet
		sheet = get_sheet()

		#clear the sheet 
		sheet.clear()

		#add new data to sheet
		gd.set_with_dataframe(sheet, data)

		#done
		return 
	
	main()


if __name__ == '__main__':

    #start process
    mobility_data_updater()