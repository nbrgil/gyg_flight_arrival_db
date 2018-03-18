import pandas as pd
import time
import os

from util.utils import get_db_client, sum_lists_without_duplicates
from definitions import ROOT_DIR
import logging


class FlightArrivalFact():
	"""
		Load flight arrival fact table
	"""

	def __init__(self, year):
		self.year = year
		self.db_client = get_db_client()

	def file_to_df(self, chunksize=None):
		"""
			Load flight arrival to a dataframe
		:param chunksize: number of records
		:return: Dataframe iterator
		"""
		df = pd.read_csv(
			filepath_or_buffer=os.path.join(ROOT_DIR, "raw", "{}.csv.bz2".format(self.year)),
			sep=",", compression="bz2", encoding="utf-8", chunksize=chunksize
		)

		return df

	def simple_lookup(
			self, df: pd.DataFrame, dim_table_name: str, df_columns: list, dim_columns: list, sk_name: str = None,
			dimension_custom_query: str = None, drop_non_sk_after: bool = True):
		"""
			Lookup between main dataframe and a dimension, checking if any records were lost.
		:param df: Dataframe
		:param dim_table_name: Dimension dataframe
		:param df_columns: Dataframe column list
		:param dim_columns: Dimension column list
		:param sk_name: Surrogate key of the dimension
		:param dimension_custom_query: Custom query (to change the default sql)
		:param drop_non_sk_after: Indicates if all the columns added, except the sk, should be dropped.
		:return:
		"""
		start_time = time.time()

		if dimension_custom_query is None:
			sql = "Select {}, {} from {}".format(sk_name, ", ".join(dim_columns), dim_table_name)
		else:
			sql = dimension_custom_query

		df_dimensao = pd.read_sql_query(sql=sql, con=self.db_client.get_conn_engine())

		old_size = len(df)
		df = df.merge(right=df_dimensao, left_on=df_columns, right_on=dim_columns, how="inner", validate="m:1")
		if old_size != len(df):
			raise ValueError(
				"Missed records after dimension {} join. Before: {}, After: {}".format(
					dim_table_name, old_size, len(df)))

		if drop_non_sk_after:
			drop_columns = sum_lists_without_duplicates(df_columns, dim_columns)
			df.drop(drop_columns, axis=1, inplace=True)

		df[sk_name] = df[sk_name].fillna(0).astype(int)

		elapsed_time = time.time() - start_time
		logging.info("SimpleLookup - {} - {} s".format(dim_table_name, elapsed_time))

		return df

	def save(self, df):
		"""Save the table"""
		conn = self.db_client.get_conn_engine().raw_connection()
		cur = conn.cursor()

		try:
			self.db_client.copy_df_to_table(
				df=df,
				table_name="flight_arrival_fact",
				commit_connection=conn,
				columns=df.columns,
				df_columns=df.columns,
				cursor=cur,
				index=False
			)
		finally:
			if not conn.closed:
				conn.close()

	def apply_lookup(self, df):
		"""
			Calls all lookups, indicating the join columns
		:param df: Dataframe
		:return: Dataframe with new sk columns
		"""

		df = self.simple_lookup(
			df=df,
			dim_table_name="flight_dimension",
			sk_name="sk_flight",
			df_columns=["FlightNum", "TailNum"],
			dim_columns=["flight_number", "tail_number"]
		)

		df = self.simple_lookup(
			df=df,
			dim_table_name="date_dimension",
			sk_name="sk_date",
			df_columns=["Year", "Month", "DayofMonth", "DayOfWeek"],
			dim_columns=["year", "month", "day_of_month", "day_of_week"]
		)

		df = self.simple_lookup(
			df=df,
			dim_table_name="carrier_dimension",
			sk_name="sk_carrier",
			df_columns=["UniqueCarrier"],
			dim_columns=["code"]
		)

		df = self.simple_lookup(
			df=df,
			dim_table_name="travel_dimension",
			sk_name="sk_travel",
			df_columns=["Origin", "Dest"],
			dim_columns=["origin_airport_iata", "dest_airport_iata"]
		)

		df = self.simple_lookup(
			df=df,
			dim_table_name="cancel_dimension",
			sk_name="sk_cancel",
			df_columns=["Cancelled", "CancellationCode"],
			dim_columns=["is_cancelled", "cancellation_code"]
		)

		return df

	def transform(self, df):
		"""
			Transformations: Rename columns, change columns type and change the format of 'time' columns.
		:param df: Dataframe
		:return: Dataframe transformed
		"""

		df = df.rename(
			columns={
				"DepTime": "actual_departure_time",
				"CRSDepTime": "scheduled_departure_time",
				"ArrTime": "arrival_time",
				"CRSArrTime": "scheduled_arrival_time",
				"ActualElapsedTime": "actual_elapsed_time",
				"CRSElapsedTime": "estimated_elapsed_time",
				"AirTime": "air_time",
				"ArrDelay": "arrival_delay",
				"DepDelay": "departure_delay",
				"TaxiIn": "taxi_in_time",
				"TaxiOut": "taxi_out_time",
				"Diverted": "diverted",
				"CarrierDelay": "carrier_delay",
				"WeatherDelay": "weather_delay",
				"NASDelay": "nas_delay",
				"SecurityDelay": "security_delay",
				"LateAircraftDelay": "late_aircraft_delay"
			}
		)

		for col in [
			"actual_departure_time", "scheduled_departure_time", "arrival_time", "scheduled_arrival_time",
			"actual_elapsed_time", "estimated_elapsed_time", "air_time", "arrival_delay", "departure_delay",
			"taxi_in_time", "taxi_out_time", "diverted", "carrier_delay", "weather_delay", "nas_delay",
			"security_delay", "late_aircraft_delay"
		]:
			df[col] = df[col].fillna(0).astype(int)

		for col in [
			"actual_departure_time", "scheduled_departure_time", "arrival_time", "scheduled_arrival_time"
		]:
			df[col] = df[col].fillna(0).astype(str)
			df[col] = df[col].str[0:-2].replace("", "0") + ":" + df[col].str[-2:]

		df.drop("Distance", axis=1, inplace=True)

		return df

	def run(self):
		df_iter = self.file_to_df(50000)
		for df in df_iter:  # type: pd.DataFrame
			df = self.apply_lookup(df)
			df = self.transform(df)
			self.save(df)


if __name__ == "__main__":
	x = FlightArrivalFact(2008)
	x.run()
