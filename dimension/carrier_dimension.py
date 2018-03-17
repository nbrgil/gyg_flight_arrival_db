import os
from definitions import ROOT_DIR
import pandas as pd
from datetime import date

from util.utils import get_db_client


class CarrierDimension():

	def __init__(self):
		self.db_client = get_db_client()

	def file_to_df(self):
		df = pd.read_csv(
			filepath_or_buffer=os.path.join(ROOT_DIR, "raw", "carriers.csv"),
			sep=",", encoding="utf-8", usecols=["Code", "Description"])

		df.drop_duplicates()

		return df

	def save(self, df):
		conn = self.db_client.get_conn_engine().raw_connection()
		cur = conn.cursor()

		try:
			self.db_client.copy_df_to_table(
				df=df,
				table_name="carrier_dimension",
				commit_connection=conn,
				columns=["code", "description"],
				cursor=cur,
				index=False
			)
		finally:
			if not conn.closed:
				conn.close()

	def run(self):
		df = self.file_to_df()
		self.save(df)


if __name__ == "__main__":
	x = CarrierDimension()
	x.run()
