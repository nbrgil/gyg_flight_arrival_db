import os
from definitions import ROOT_DIR
import pandas as pd


from util.utils import get_db_client


class FlightDimension():

	def __init__(self, year):
		self.year = year
		self.db_client = get_db_client()

	def query_from_db(self):
		return pd.read_sql(
			sql="""
				SELECT flight_number, tail_number
				FROM flight_dimension
			""",
			con=self.db_client.get_conn_engine()
		)

	def file_to_df(self, chunksize=None):
		df = pd.read_csv(
			filepath_or_buffer=os.path.join(ROOT_DIR, "raw", "{}.csv.bz2".format(self.year)),
			sep=",", compression="bz2", encoding="utf-8", usecols=["FlightNum", "TailNum"],
			chunksize=chunksize)

		return df

	def save(self, df):
		conn = self.db_client.get_conn_engine().raw_connection()
		cur = conn.cursor()

		try:
			self.db_client.copy_df_to_table(
				df=df,
				table_name="flight_dimension",
				commit_connection=conn,
				columns=["flight_number", "tail_number"],
				cursor=cur,
				index=False
			)
		finally:
			if not conn.closed:
				conn.close()


	def run(self):
		df = self.file_to_df()
		df.drop_duplicates(inplace=True)
		self.save(df)


if __name__ == "__main__":
	x = FlightDimension(2008)
	x.run()
