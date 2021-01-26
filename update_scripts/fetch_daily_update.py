'''This script retrieves daily updates on COVID-19 data from the repository of the CSSE, JHU [https://github.com/CSSEGISandData] and stores the latest tally in a CSV file with the corresponding date in the filename. It also updates the time-series dataset and regenerates the corresponding CSV file.'''

import pandas as data
import os.path
from datetime import timedelta
from datetime import datetime

base_dir = os.path.join(os.path.dirname(__file__), "../")		#Obtain the path to the base directory for absosulte addressing.
date = datetime.now().date() 				#Date of update.
day_delta = timedelta(days = 1)			#Add time delta to fix timezone mismatch.

def generate_dataset(record):
	'''Generate a dataframe from a record of tallies for a particular date.'''
	df = data.DataFrame({"Confirmed": record[0], "Recovered/Migrated": record[1], "Deceased": record[2]}).reset_index().rename(columns = {"index": "Region"})
	df = df[df.Confirmed != 0]		#Ignore regions with no cases.
	df = df.append(df.sum(numeric_only = True), ignore_index = True)
	df.iloc[-1, 0] = "Global Total"
	return(df)

if __name__ == "__main__":
	#Load historical data.
	raw_CNF = data.read_csv("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv")
	raw_RCV = data.read_csv("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv")
	raw_DCS = data.read_csv("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv")

	#Initialize the time-series.
	record = dict()
	time_series = data.read_csv(base_dir + "time-series/Global_aggregated.csv")		#Load the time-series.
	time_series = time_series[time_series["Date"] != date.strftime('%d-%m-%Y')]	#Discard other updates made today.

	#Clean and transform the data.
	raw_CNF = raw_CNF[["Country/Region", raw_CNF.keys()[-1]]]
	raw_CNF = raw_CNF.rename(columns = {"Country/Region": "Region"}).groupby(["Region"]).sum().T
	raw_CNF.index = data.to_datetime(raw_CNF.index)

	raw_RCV = raw_RCV[["Country/Region", raw_RCV.keys()[-1]]]
	raw_RCV = raw_RCV.rename(columns = {"Country/Region": "Region"}).groupby(["Region"]).sum().T
	raw_RCV.index = data.to_datetime(raw_RCV.index)

	raw_DCS = raw_DCS[["Country/Region", raw_DCS.keys()[-1]]]
	raw_DCS = raw_DCS.rename(columns = {"Country/Region": "Region"}).groupby(["Region"]).sum().T
	raw_DCS.index = data.to_datetime(raw_DCS.index)

	#Implement spelling corrections here:
	rename_dict = {'Taiwan*': 'Taiwan', 'Korea, South': 'South Korea', 'US': 'United States of America', 'Czechia': 'Czech Republic', 'Holy See': 'Vatican City'}
	raw_CNF = raw_CNF.rename(columns = rename_dict)
	raw_RCV = raw_RCV.rename(columns = rename_dict)
	raw_DCS = raw_DCS.rename(columns = rename_dict)

	#Implement corrections for recoveries:
	raw_RCV.loc[raw_RCV.index[0], "Serbia"] = 15564
	raw_RCV.loc[raw_RCV.index[0], "Belgium"] = 31130
	raw_RCV.loc[raw_RCV.index[0], "United States of America"] = 6298082

	#Generate a dataframe of date-wise aggregated data.
	assert (date == (raw_CNF.index[0] + day_delta)),"Date mismatch. New data is not contiguous with existing entries."

	#Prepare values.
	tally = [table.loc[data.to_datetime(date - day_delta)] for table in [raw_CNF, raw_RCV, raw_DCS]]
	#Generate a daily record for the particular date.
	updated_tally = generate_dataset(tally)

	updated_tally.to_csv(base_dir + "datasets/Global_aggregated_{}.csv".format(date.strftime('%d-%m-%Y')), index = False)
		
	#Add a date column and assimilate the records into a time_series dataframe with historical data.
	updated_tally.insert(0, "Date", date.strftime('%d-%m-%Y'))	
	time_series = time_series.append(updated_tally, ignore_index = True)	

	#Write the updated time-series to its CSV file.
	time_series.to_csv(base_dir + "time-series/Global_aggregated.csv", index = False)
