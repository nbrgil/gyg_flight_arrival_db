import os
from definitions import ROOT_DIR
import pandas as pd
from datetime import date

from util.utils import get_db_client


class DateDimension():

	def __init__(self, year):
		self.year = year
		self.db_client = get_db_client()

	def query_from_db(self):
		return pd.read_sql(
			sql="""
				SELECT year, month, day_of_month, day_of_week
				FROM date_dimension
			""",
			con=self.db_client.get_conn_engine()
		)

	def file_to_df(self, chunksize=None):
		df = pd.read_csv(
			filepath_or_buffer=os.path.join(ROOT_DIR, "raw", "{}.csv.bz2".format(self.year)),
			sep=",", compression="bz2", encoding="utf-8", usecols=["Year", "Month", "DayofMonth", "DayOfWeek"],
			chunksize=chunksize
		)

		return df

	def transform(self, df):

		full_date_list = []
		for _, row in df.iterrows():
			full_date_list.append(
				date(year=row["Year"], month=row["Month"], day=row["DayofMonth"])
			)

		df["full_date"] = full_date_list

		return df

	def save(self, df):
		conn = self.db_client.get_conn_engine().raw_connection()
		cur = conn.cursor()

		try:
			self.db_client.copy_df_to_table(
				df=df,
				table_name="date_dimension",
				commit_connection=conn,
				columns=["year", "month", "day_of_month", "day_of_week", "full_date"],
				cursor=cur,
				index=False
			)
		finally:
			if not conn.closed:
				conn.close()

	def run(self):
		df_iter = self.file_to_df(50000)
		for df in df_iter: #type: pd.DataFrame
			df.drop_duplicates(subset=["Year", "Month", "DayofMonth", "DayOfWeek"], inplace=True)
			df = self.transform(df)
			df_db = self.query_from_db()

			df_result = pd.merge(
				left=df,
				right=df_db,
				how="left",
				left_on=["Year", "Month", "DayofMonth", "DayOfWeek"],
				right_on=["year", "month", "day_of_month", "day_of_week"],
				indicator=True,
				suffixes=('', '_y')
			).query("_merge == 'left_only'").drop(
				["_merge", "year", "month", "day_of_month", "day_of_week"], axis=1
			)
			self.save(df_result)


if __name__ == "__main__":
	x = DateDimension(2008)
	x.run()
