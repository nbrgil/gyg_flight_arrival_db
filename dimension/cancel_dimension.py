import os
from definitions import ROOT_DIR
import pandas as pd

from dimension.base_dimension import BaseDimension
from util.utils import get_db_client


class CancelDimension(BaseDimension):
	"""
		Loads data to the cancel dimension.
		The data source is a file in raw/[year].csv.bz2
	"""

	def __init__(self, year):
		"""
			Connects to the target postgres
		:param year: Year of the data
		"""
		super().__init__()
		self.year = year
		self.file_columns = ["Cancelled", "CancellationCode", "reason"]
		self.table_columns = ["is_cancelled", "cancellation_code", "reason"]

	def query_from_db(self):
		"""
			Queries the full dimension table in to a pandas dataframe.
			This df is used to find out new records and avoid duplicate entries.
		:return:
		"""
		return pd.read_sql(
			sql="""
				SELECT is_cancelled, cancellation_code, reason
				FROM cancel_dimension
			""",
			con=self.db_client.get_conn_engine()
		)

	def file_to_df(self, chunksize=None):
		"""
			Read the data file of the year (already downloaded) and returns into
			in a pandas dataframe.
			Only the necessary columns are read.
		:param chunksize: It is used to avoid loading all the file in memory
		:return: dataframe
		"""
		df = pd.read_csv(
			filepath_or_buffer=os.path.join(ROOT_DIR, "raw", "{}.csv.bz2".format(self.year)),
			sep=",", compression="bz2", encoding="utf-8", usecols=["Cancelled", "CancellationCode"],
			chunksize=chunksize
		)

		return df

	@staticmethod
	def transform(df: pd.DataFrame):
		"""
			Creates a new column called 'reason', translating the CancellationCode
		:param df: dataframe
		:return: dataframe transformed
		"""

		def cancellation_code_to_reason(code):
			if code is None:
				return 'N/A'
			if code == "A":
				return "Carrier"
			if code == "B":
				return "Weather"
			if code == "C":
				return "NAS"
			if code == "D":
				return "Security"
			return 'N/A'

		reason_list = []
		for _, row in df.iterrows():
			reason_list.append(
				cancellation_code_to_reason(code=row["CancellationCode"])
			)

		df["reason"] = reason_list

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
			df.drop_duplicates(inplace=True)
			df = self.transform(df)

			df_result = self.get_only_new_records(
				df=df,
				df_columns=self.file_columns,
				table_columns=self.table_columns
			)

			if len(df_result) > 0:
				self.save(
					df=df_result,
					table_name="cancel_dimension",
					df_columns=self.file_columns,
					table_colums=self.table_columns
				)


if __name__ == "__main__":
	os.environ["PGHOST"] = "localhost"
	x = CancelDimension(2008)
	x.run()
