import pandas as data
import os.path
from datetime import timedelta
from datetime import datetime

cut_off = datetime.strptime('16-07-2020', '%d-%m-%Y').date()	#Extract historical data till this date.
base_dir = os.path.join(os.path.dirname(__file__), "../")		#Obtain the path to the base directory for absosulte addressing.
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
	time_series = data.DataFrame()

	#Clean and transform the data.
	raw_CNF = raw_CNF.rename(columns = {"Country/Region": "Region"}).drop(["Province/State", "Lat", "Long"], axis = 1).groupby(["Region"]).sum().T
	raw_CNF.index = data.to_datetime(raw_CNF.index)

	raw_RCV = raw_RCV.rename(columns = {"Country/Region": "Region"}).drop(["Province/State", "Lat", "Long"], axis = 1).groupby(["Region"]).sum().T
	raw_RCV.index = data.to_datetime(raw_RCV.index)

	raw_DCS = raw_DCS.rename(columns = {"Country/Region": "Region"}).drop(["Province/State", "Lat", "Long"], axis = 1).groupby(["Region"]).sum().T
	raw_DCS.index = data.to_datetime(raw_DCS.index)

	#Implement spelling corrections here:
	rename_dict = {'Taiwan*': 'Taiwan', 'Korea, South': 'South Korea', 'US': 'United States of America', 'Czechia': 'Czech Republic', 'Holy See': 'Vatican City'}
	raw_CNF = raw_CNF.rename(columns = rename_dict)
	raw_RCV = raw_RCV.rename(columns = rename_dict)
	raw_DCS = raw_DCS.rename(columns = rename_dict)

	#Generate a dataframe of date-wise aggregated data.
	for date in raw_CNF.index:
		#Prepare values.
		tally = [table.loc[date] for table in [raw_CNF, raw_RCV, raw_DCS]]
		#Generate a daily record for the particular date.
		daily_record = generate_dataset(tally)
		
		if(date + day_delta < cut_off):		#Do not process records after the cut-off date.
			#Write the updated time-series to its CSV file. Add time delta to fix time-zone mismatch.
			daily_record.to_csv(base_dir + "datasets/Global_aggregated_{}.csv".format((date + day_delta).strftime('%d-%m-%Y')), index = False)
			#Add a date column and assimilate the records into a time_series dataframe with historical data. Add time delta to fix time-zone mismatch.
			daily_record.insert(0, "Date", (date + day_delta).strftime('%d-%m-%Y'))	
			time_series = time_series.append(daily_record, ignore_index = True)	
	
	#Write the updated time-series to its CSV file.
	time_series.to_csv(base_dir + "time-series/Global_aggregated.csv", index = False)
