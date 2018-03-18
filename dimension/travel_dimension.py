import os
from definitions import ROOT_DIR
import pandas as pd

from dimension.base_dimension import BaseDimension
from util.utils import get_db_client


class TravelDimension(BaseDimension):
	"""
		Loads data to the travel dimension.
		The data source is a file in raw/[year].csv.bz2 and airports.csv
	"""

	def __init__(self, year):
		"""
			Connects to the target postgres
		:param year: Year of the data
		"""
		super().__init__()
		self.year = year
		self.db_client = get_db_client()
		self.table_columns = [
			'distance', 'origin_airport_iata', 'origin_airport_name', 'origin_city',
			'origin_state', 'origin_country', 'origin_latitude', 'origin_longitude',
			'dest_airport_iata', 'dest_airport_name', 'dest_city', 'dest_state',
			'dest_country', 'dest_latitude', 'dest_longitude']
		self.join_columns = [
			"origin_airport_iata", "dest_airport_iata"
		]

	def query_from_db(self):
		"""
			Queries the full dimension table in to a pandas dataframe.
			This df is used to find out new records and avoid duplicate entries.
		:return:
		"""
		return pd.read_sql(
			sql="""
				SELECT origin_airport_iata, dest_airport_iata
				FROM travel_dimension
			""",
			con=self.db_client.get_conn_engine()
		)

	def file_to_df(self, chunksize=None):
		"""
			Load the flight arrival data into a dataframe iterator
		:param chunksize: Number of records of the chunk
		:return: Dataframe.
		"""
		df = pd.read_csv(
			filepath_or_buffer=os.path.join(ROOT_DIR, "raw", "{}.csv.bz2".format(self.year)),
			sep=",", compression="bz2", encoding="utf-8", usecols=["Origin", "Dest", "Distance"],
			chunksize=chunksize
		)

		return df

	def airport_file_to_df(self):
		"""
			Load whole airport data into a dataframe
		:return: Dataframe
		"""
		df = pd.read_csv(
			filepath_or_buffer=os.path.join(ROOT_DIR, "raw", "airports.csv".format(self.year)),
			sep=",", encoding="utf-8",
			usecols=["iata", "airport", "city", "state", "country", "lat", "long"]
		)

		return df

	def transform(self, df: pd.DataFrame, df_airport: pd.DataFrame):
		"""
			Add airport informations (origin and destination) and rename columns to match the target table.
		:param df: Arrival flight dataframe (chunk)
		:param df_airport: Airport dataframe
		:return: Dataframe with new columns
		"""
		df_res = df.merge(
			right=df_airport,
			how="inner",
			left_on="Origin",
			right_on="iata"
		)

		df_res = df_res.rename(columns={
			"iata": "origin_airport_iata",
			"airport": "origin_airport_name",
			"city": "origin_city",
			"state": "origin_state",
			"country": "origin_country",
			"lat": "origin_latitude",
			"long": "origin_longitude"
		})

		df_res.drop("Origin", axis=1, inplace=True)

		df_res = df_res.merge(
			right=df_airport,
			how="inner",
			left_on="Dest",
			right_on="iata"
		)

		df_res = df_res.rename(columns={
			"iata": "dest_airport_iata",
			"airport": "dest_airport_name",
			"city": "dest_city",
			"state": "dest_state",
			"country": "dest_country",
			"lat": "dest_latitude",
			"long": "dest_longitude"
		})

		df_res.drop("Dest", axis=1, inplace=True)

		df_res = df_res.rename(columns={
			"Distance": "distance"
		})

		return df_res

	def run(self):
		"""
			Reads both year data and airport data. Drop duplicates and save only new records.
		:return:
		"""
		df_iter = self.file_to_df(50000)
		df_airport = self.airport_file_to_df()
		for df in df_iter:  # type: pd.DataFrame
			df.drop_duplicates(inplace=True)
			df = self.transform(df, df_airport)

			df_result = self.get_only_new_records(
				df=df,
				df_columns=self.join_columns,
				table_columns=self.join_columns
			)

			if len(df_result) > 0:
				# df_result.drop(self.table_columns, axis=1)

				self.save(
					df=df_result,
					table_name="travel_dimension",
					df_columns=self.table_columns,
					table_colums=self.table_columns
				)


if __name__ == "__main__":
	x = TravelDimension(2008)
	x.run()
