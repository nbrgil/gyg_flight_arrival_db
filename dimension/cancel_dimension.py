import os
from definitions import ROOT_DIR
import pandas as pd

from util.utils import get_db_client


class CancelDimension():

	def __init__(self, year):
		self.year = year
		self.db_client = get_db_client()

	def query_from_db(self):
		return pd.read_sql(
			sql="""
				SELECT is_cancelled, cancellation_code, reason
				FROM cancel_dimension
			""",
			con=self.db_client.get_conn_engine()
		)

	def file_to_df(self, chunksize=None):
		df = pd.read_csv(
			filepath_or_buffer=os.path.join(ROOT_DIR, "raw", "{}.csv.bz2".format(self.year)),
			sep=",", compression="bz2", encoding="utf-8", usecols=["Cancelled", "CancellationCode"],
			chunksize=chunksize
		)

		return df

	def transform(self, df: pd.DataFrame):

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

	def save(self, df):
		conn = self.db_client.get_conn_engine().raw_connection()
		cur = conn.cursor()

		try:
			self.db_client.copy_df_to_table(
				df=df,
				table_name="cancel_dimension",
				commit_connection=conn,
				columns=["is_cancelled", "cancellation_code", "reason"],
				df_columns=["Cancelled", "CancellationCode", "reason"],
				cursor=cur,
				index=False
			)
		finally:
			if not conn.closed:
				conn.close()

	def run(self):
		df_iter = self.file_to_df(50000)
		for df in df_iter:  # type: pd.DataFrame
			df.drop_duplicates(inplace=True)
			df = self.transform(df)
			df_db = self.query_from_db()

			df_result = pd.merge(
				left=df,
				right=df_db,
				how="left",
				left_on=["Cancelled", "CancellationCode", "reason"],
				right_on=["is_cancelled", "cancellation_code", "reason"],
				indicator=True,
				suffixes=('', '_y')
			).query("_merge == 'left_only'")
			self.save(df_result[["Cancelled", "CancellationCode", "reason"]])


if __name__ == "__main__":
	x = CancelDimension(2008)
	x.run()
