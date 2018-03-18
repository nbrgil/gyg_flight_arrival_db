import os
from definitions import ROOT_DIR
import pandas as pd

from dimension.base_dimension import BaseDimension
from util.utils import get_db_client


class FlightDimension(BaseDimension):
	"""
		Loads data to the flight dimension.
		The data source is a file in raw/[year].csv.bz2
	"""

	def __init__(self, year):
		"""
			Connects to the target postgres
		:param year: Year of the data
		"""
		super().__init__()
		self.year = year
		self.db_client = get_db_client()
		self.file_columns = ["FlightNum", "TailNum"]
		self.table_columns = ["flight_number", "tail_number"]

	def query_from_db(self):
		"""
			Queries the full dimension table in to a pandas dataframe.
			This df is used to find out new records and avoid duplicate entries.
		:return:
		"""
		return pd.read_sql(
			sql="""
				SELECT flight_number, tail_number
				FROM flight_dimension
			""",
			con=self.db_client.get_conn_engine()
		)

	def file_to_df(self):
		"""
			Read the data file of the year (already downloaded) and returns into
			in a pandas dataframe.
			Only the necessary columns are read.
		:return: dataframe
		"""
		df = pd.read_csv(
			filepath_or_buffer=os.path.join(ROOT_DIR, "raw", "{}.csv.bz2".format(self.year)),
			sep=",", compression="bz2", encoding="utf-8", usecols=["FlightNum", "TailNum"])

		return df

	def run(self):
		"""
			Reads the file, drop the duplicates and saves to the db only the new records
		:return:
		"""
		df = self.file_to_df()
		df.drop_duplicates(inplace=True)

		df_result = self.get_only_new_records(
			df=df,
			df_columns=self.file_columns,
			table_columns=self.table_columns
		)

		if len(df_result) > 0:
			df_result.drop(self.table_columns, axis=1)

			self.save(
				df=df_result,
				table_name="flight_dimension",
				df_columns=self.file_columns,
				table_colums=self.table_columns
			)


if __name__ == "__main__":
	x = FlightDimension(2008)
	x.run()
