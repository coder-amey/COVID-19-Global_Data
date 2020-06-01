import pandas as data
import os.path
from datetime import datetime

cut_off = datetime.strptime('31-05-2020', '%d-%m-%Y').date()	#Extract historical data till this date.
base_dir = os.path.join(os.path.dirname(__file__), "../")		#Obtain the path to the base directory for absosulte addressing.

def generate_dataset(record):
	'''Generate a dataframe from a record of tallies for a particular date.'''
	df = data.DataFrame({"Confirmed": record[0], "Recovered/Migrated": record[1], "Deceased": record[2]}).reset_index()
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
	#Implement spelling corrections here:
	raw_CNF = raw_CNF.rename(columns = {'Taiwan*': 'Taiwan'})
	raw_CNF = raw_CNF.rename(columns = {'Korea, South': 'South Korea'})
	raw_CNF = raw_CNF.rename(columns = {'US': 'United States of America'})
	raw_CNF.index = data.to_datetime(raw_CNF.index)

	raw_RCV = raw_RCV.rename(columns = {"Country/Region": "Region"}).drop(["Province/State", "Lat", "Long"], axis = 1).groupby(["Region"]).sum().T
	raw_RCV = raw_RCV.rename(columns = {'Taiwan*': 'Taiwan'})
	raw_RCV.index = data.to_datetime(raw_RCV.index)

	raw_DCS = raw_DCS.rename(columns = {"Country/Region": "Region"}).drop(["Province/State", "Lat", "Long"], axis = 1).groupby(["Region"]).sum().T
	raw_DCS = raw_DCS.rename(columns = {'Taiwan*': 'Taiwan'})
	raw_DCS.index = data.to_datetime(raw_DCS.index)

	#Generate a dataframe of date-wise aggregated data.
	for date in raw_CNF.index:
		#Prepare values.
		tally = [table.loc[date] for table in [raw_CNF, raw_RCV, raw_DCS]]
		#Generate a daily record for the particular date.
		daily_record = generate_dataset(tally)

		daily_record.to_csv(base_dir + "datasets/India_regional_aggregated_{}.csv".format(date.strftime('%d-%m-%Y')), index = False)
		if(date < cut_off):		#Do not process records after the cut-off date.
			#Add a date column and assimilate the records into a time_series dataframe with historical data.
			daily_record.insert(0, "Date", date.strftime('%d-%m-%Y'))	
			time_series = time_series.append(daily_record, ignore_index = True)	
	
	#Write the updated time-series to its CSV file.
	time_series.to_csv(base_dir + "time-series/India_regional_aggregated.csv", index = False)
