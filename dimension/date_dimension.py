import os
from definitions import ROOT_DIR
import pandas as pd
from datetime import date

from dimension.base_dimension import BaseDimension
from util.utils import get_db_client


class DateDimension(BaseDimension):
	"""
		Loads data to the date dimension.
		The data source is a file in raw/[year].csv.bz2
	"""

	def __init__(self, year):
		"""
			Connects to the target postgres
		:param year: Year of the data
		"""
		super().__init__()
		self.year = year
		self.file_columns = ["Year", "Month", "DayofMonth", "DayOfWeek"]
		self.table_columns = ["year", "month", "day_of_month", "day_of_week"]
		self.db_client = get_db_client()

	def query_from_db(self):
		"""
			Queries the full dimension table in to a pandas dataframe.
			This df is used to find out new records and avoid duplicate entries.
		:return:
		"""
		return pd.read_sql(
			sql="""
				SELECT year, month, day_of_month, day_of_week
				FROM date_dimension
			""",
			con=self.db_client.get_conn_engine()
		)

	def file_to_df(self, chunksize=None):
		"""
			Read the data file of the year (already downloaded) and returns into
			in a pandas dataframe.
			Only the necessary columns are read.
		:return: dataframe
		"""
		df = pd.read_csv(
			filepath_or_buffer=os.path.join(ROOT_DIR, "raw", "{}.csv.bz2".format(self.year)),
			sep=",", compression="bz2", encoding="utf-8", usecols=["Year", "Month", "DayofMonth", "DayOfWeek"],
			chunksize=chunksize
		)

		return df

	def transform(self, df):
		"""
			Add a date columns with the existent columns
		:param df: Dataframe
		:return: Dataframe with new column
		"""

		full_date_list = []
		for _, row in df.iterrows():
			full_date_list.append(
				date(year=row["Year"], month=row["Month"], day=row["DayofMonth"])
			)

		df["full_date"] = full_date_list

		return df

	def run(self):
		"""
			Reads the file in chunks of 50000 records.
			To each chunk, the duplicate records are removed and the transform method is applied.
			After that, only the new records (not already on database) are saved
		:return:
		"""
		df_iter = self.file_to_df(50000)
		for df in df_iter:  # type: pd.DataFrame
			df.drop_duplicates(subset=self.file_columns, inplace=True)
			df = self.transform(df)

			df_result = self.get_only_new_records(
				df=df,
				df_columns=self.file_columns,
				table_columns=self.table_columns
			)

			if len(df_result) > 0:
				df_result.drop(["year", "month", "day_of_month", "day_of_week"], axis=1)

				self.save(
					df=df_result,
					table_name="date_dimension",
					df_columns=self.file_columns + ["full_date"],
					table_colums=self.table_columns
				)


if __name__ == "__main__":
	x = DateDimension(2008)
	x.run()
