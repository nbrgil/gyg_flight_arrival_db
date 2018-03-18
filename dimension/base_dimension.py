import pandas as pd

from util.utils import get_db_client


class BaseDimension():
	"""
		Base functions to load data to a dimension.
	"""
	def __init__(self):
		"""
			Connects to the target postgres
		"""
		self.db_client = get_db_client()

	def query_from_db(self):
		raise NotImplementedError

	def get_only_new_records(self, df, df_columns, table_columns):
		df_result = pd.merge(
			left=df,
			right=self.query_from_db(),
			how="left",
			left_on=df_columns,
			right_on=table_columns,
			indicator=True,
			suffixes=('', '_y')
		).query("_merge == 'left_only'")

		return df_result

	def save(self, df, table_name, df_columns=None, table_colums=None):
		"""
			Save one dataframe to postgres
		:param df: Dataframe
		:return: None
		"""
		conn = self.db_client.get_conn_engine().raw_connection()
		cur = conn.cursor()

		try:
			self.db_client.copy_df_to_table(
				df=df,
				table_name=table_name,
				commit_connection=conn,
				columns=df_columns,
				df_columns=table_colums,
				cursor=cur,
				index=False
			)
		finally:
			if not conn.closed:
				conn.close()