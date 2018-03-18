import os
from definitions import ROOT_DIR
import pandas as pd

from dimension.base_dimension import BaseDimension


class CarrierDimension(BaseDimension):
	"""
		Loads data to the carrier dimension.
		The data source is a file in raw/carriers.csv
	"""

	def __init__(self):
		"""
			Connects to the target postgres
		:param year: Year of the data
		"""
		super().__init__()
		self.file_columns = ["Code", "Description"]
		self.table_columns = ["code", "description"]

	def query_from_db(self):
		"""
			Queries the full dimension table in to a pandas dataframe.
			This df is used to find out new records and avoid duplicate entries.
		:return:
		"""
		return pd.read_sql(
			sql="""
				SELECT code, description
				FROM carrier_dimension
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
			filepath_or_buffer=os.path.join(ROOT_DIR, "raw", "carriers.csv"),
			sep=",", encoding="utf-8", usecols=self.file_columns)

		df.drop_duplicates()

		return df

	def run(self):
		"""
			Reads the file and only the new records (not already on database) are saved
		"""
		df = self.file_to_df()

		df_result = self.get_only_new_records(
			df=df,
			df_columns=self.file_columns,
			table_columns=self.table_columns
		)

		if len(df_result) > 0:

			self.save(
				df=df_result,
				table_name="carrier_dimension",
				df_columns=self.file_columns,
				table_colums=self.table_columns
			)


if __name__ == "__main__":
	os.environ["PGHOST"] = "localhost"
	x = CarrierDimension()
	x.run()
