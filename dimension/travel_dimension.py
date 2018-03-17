import os
from definitions import ROOT_DIR
import pandas as pd

from util.utils import get_db_client


class TravelDimension():

	def __init__(self, year):
		self.year = year
		self.db_client = get_db_client()

	def query_from_db(self):
		return pd.read_sql(
			sql="""
				SELECT origin_airport_iata, dest_airport_iata
				FROM travel_dimension
			""",
			con=self.db_client.get_conn_engine()
		)

	def file_to_df(self, chunksize=None):
		df = pd.read_csv(
			filepath_or_buffer=os.path.join(ROOT_DIR, "raw", "{}.csv.bz2".format(self.year)),
			sep=",", compression="bz2", encoding="utf-8", usecols=["Origin", "Dest", "Distance"],
			chunksize=chunksize
		)

		return df

	def airport_file_to_df(self):
		df = pd.read_csv(
			filepath_or_buffer=os.path.join(ROOT_DIR, "raw", "airports.csv".format(self.year)),
			sep=",", encoding="utf-8",
			usecols=["iata", "airport", "city", "state", "country", "lat", "long"]
		)

		return df

	def transform(self, df: pd.DataFrame, df_airport: pd.DataFrame):
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

		# "origin_airport_iata", "origin_airport_name", "origin_city", "origin_state", "origin_country",
		# "origin_longitude", "origin_latitude", "dest_airport_iata", "dest_airport_name", "dest_city",
		# "dest_state", "dest_country", "dest_longitude", "dest_latitude"],
		# # df_columns=["Cancelled", "reason"],

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

	def save(self, df):
		conn = self.db_client.get_conn_engine().raw_connection()
		cur = conn.cursor()

		try:
			self.db_client.copy_df_to_table(
				df=df,
				table_name="travel_dimension",
				commit_connection=conn,
				columns=[
					'distance', 'origin_airport_iata', 'origin_airport_name', 'origin_city',
					'origin_state', 'origin_country', 'origin_latitude', 'origin_longitude',
					'dest_airport_iata', 'dest_airport_name', 'dest_city', 'dest_state',
					'dest_country', 'dest_latitude', 'dest_longitude'],
				df_columns=[
					'distance', 'origin_airport_iata', 'origin_airport_name', 'origin_city',
					'origin_state', 'origin_country', 'origin_latitude', 'origin_longitude',
					'dest_airport_iata', 'dest_airport_name', 'dest_city', 'dest_state',
					'dest_country', 'dest_latitude', 'dest_longitude'],
				cursor=cur,
				index=False
			)
		finally:
			if not conn.closed:
				conn.close()

	def run(self):
		df_iter = self.file_to_df(50000)
		df_airport = self.airport_file_to_df()
		for df in df_iter:  # type: pd.DataFrame
			df.drop_duplicates(inplace=True)
			df = self.transform(df, df_airport)
			df_db = self.query_from_db()

			df_result = pd.merge(
				left=df,
				right=df_db,
				how="left",
				on=["origin_airport_iata", "dest_airport_iata"],
				indicator=True,
				suffixes=('', '_y')
			).query("_merge == 'left_only'").drop("_merge", axis=1)

			if len(df_result) > 0:
				self.save(df_result)


if __name__ == "__main__":
	x = TravelDimension(2008)
	x.run()
