import sqlalchemy
from util.db_wrapper import get_postgres_client
import pandas as pd

from util.df_functions import pg_money_columns_to_float
from util.list_functions import sum_lists_without_duplicates
from datetime import date
from util.luigi.base_task import BaseTask
import logging
import time
from pandas.errors import MergeError


class BaseFato(BaseTask):
	table_name = None

	def __init__(
			self, stagingtable, rule, query_dados, chunksize, df_columns=None, table_columns=None, *args,
			**kwargs):
		self.postgres_dw = get_postgres_client("dw")
		self.postgres_stg = get_postgres_client("stg")
		self.df_dados = None
		self.query_dados = query_dados
		self.chunksize = chunksize
		self.df_columns = df_columns
		self.table_columns = table_columns

		super().__init__(stagingtable=stagingtable, rule=rule, *args, **kwargs)

	@classmethod
	def bulk_complete(cls, parameter_tuples):
		"""
			Usado pelo luigi quando uma lista de datas é informada (RangeDaily).
		:return: list com as datas que já foram executadas (não vão executar de novo)
		"""
		postgres = get_postgres_client("dw")
		conn = postgres.get_conn_engine().connect()

		try:
			date_column = cls.func.get_date_column_name()
			sql = "SELECT distinct {date_column} FROM {table_name} WHERE {date_column} = ANY(:date_list)".format(
				date_column=date_column,
				table_name=cls.func.table_name
			)
			results = conn.execute(sqlalchemy.sql.text(sql), date_list=list(parameter_tuples)).fetchall()

		finally:
			if not conn.closed:
				conn.close()

		return [x[0] for x in results]

	@staticmethod
	def get_date_column_name():
		"""
			Usado para conferir se o processo já executou.
		:return: str - Retorna nome da coluna de data da tabela de destino.
		"""
		return "data"

	def get_exists_params(self):
		"""
			Usado pelo luigi para definir se o processo executou (ver coment. classe pai)
		"""
		return {
			"postgres_client": self.postgres_dw,
			"table_name": self.table_name,
			"schema": "public",
			"sql_filter": "WHERE {} = '{}'".format(self.get_date_column_name(), self.target_date)
		}

	def interval_lookup(
			self, df: pd.DataFrame, dim_table_name, sk_name: str, df_column: str, dim_min_column: str,
			dim_max_column: str, closed_interval_type: str = "both", convert_money_to_float: bool = True):
		"""
			Lookup para tabelas com faixas de intervalos. Exemplo Dimensão de faixa de preços do imóvel.

		:param df: Dataframe
		:param dim_table_name: Nome da tabela dimensão
		:param sk_name: Nome da chave surrogate
		:param df_column: Coluna com a métrica do dataframe de origem
		:param dim_min_column: Coluna da tabela dimensão com o valor menor do intervalo
		:param dim_max_column: Coluna da tabela dimensão com o valor maior do intervalo
		:param closed_interval_type: Tipo de intervalo (ver doc de pd.IntervalIndex.from_arrays). Padrão 'both'
		:param convert_money_to_float: Indica se deve aplicar transformação de money para float do postgres
		:return: Dataframe com a nova coluna sk
		"""

		df_dim = pd.read_sql_query(
			sql="SELECT * FROM {}".format(dim_table_name),
			con=self.postgres_dw.get_conn_engine())

		if convert_money_to_float:
			df_dim = pg_money_columns_to_float(
				df=df_dim,
				column_list=[
					dim_min_column,
					dim_max_column
				]
			)

			df = pg_money_columns_to_float(
				df=df,
				column_list=[df_column]
			)

		idx = pd.IntervalIndex.from_arrays(
			left=df_dim[dim_min_column],
			right=df_dim[dim_max_column],
			closed=closed_interval_type
		)

		df[sk_name] = df_dim.loc[idx.get_indexer(df[df_column]), sk_name].values
		df[sk_name].fillna(0, inplace=True)

		return df

	def simple_lookup(
			self, df: pd.DataFrame, dim_table_name: str, df_columns: list, dim_columns: list, sk_name: str = None,
			drop_non_sk_after: bool = True, dimension_custom_query: str = None):
		"""
			Efetua um lookup em um dataframe, garantindo que nenhum registro é perdido.
		:param df: dataframe com os dados da staging
		:param dim_table_name: nome da tabela de dimensão
		:param df_columns: colunas do dataframe para efetuar o join
		:param dim_columns: colunas da dimensão para efetuar o join
		:param sk_name: Nome da coluna surrogate que será extraída. Não é necessário quando a custom query é usada.
		:param drop_non_sk_after: Indica se deve excluir as colunas que foram usadas para join
		:param dimension_custom_query: Query customizada para casos específicos
		:return:
		"""

		start_time = time.time()

		if dimension_custom_query is None:
			sql = "Select {}, {} from {}".format(sk_name, ", ".join(dim_columns), dim_table_name)
		else:
			sql = dimension_custom_query

		df_dimensao = pd.read_sql_query(sql=sql, con=self.postgres_dw.get_conn_engine())

		try:
			old_size = len(df)
			df = df.merge(right=df_dimensao, left_on=df_columns, right_on=dim_columns, how="inner", validate="m:1")
			if old_size != len(df):
				raise ValueError(
					"Registros perdidos na dimensão {}. Antes: {}, Depois: {}".format(
						dim_table_name, old_size, len(df)))
		except MergeError:
			logging.error("Erro ao fazer lookup m:1 com {}".format(dim_table_name))
			raise

		if drop_non_sk_after:
			drop_columns = sum_lists_without_duplicates(df_columns, dim_columns)
			df.drop(drop_columns, axis=1, inplace=True)

		elapsed_time = time.time() - start_time
		logging.info("FatoLeads - SimpleLookup - {} - {} s".format(dim_table_name, elapsed_time))

		return df

	def load_dados_df(self):
		"""
			Carrega dados da staging, usando o param 'query_dados'.
			Essa consulta deve sempre conter dois param para formatação:
			1) A tabela de staging
			2) A data de filtro
			Ex: SELECT * FROM {} WHERE data = '{}'
		:return: Dataframe iterator
		"""
		query = self.query_dados.format(self.stagingtable, self.target_date)

		self.df_dados = pd.read_sql_query(
			sql=query.format(self.target_date),
			con=self.postgres_stg.get_conn_engine(),
			chunksize=self.chunksize
		)

	def transform_dados(self, df):
		"""
			Transformação em dados da staging antes de aplicar os lookups com as dimensões.
		:param df: Dataframe (chunk)
		:return: Mesmo dataframe transformado
		"""
		return df

	def apply_all_lookups(self, df: pd.DataFrame):
		"""
			Chamada para cada chunk da staging, para aplicar os lookups necessários.
		:param df: Dataframe (chunk)
		:return: Mesmo dataframe transformado
		"""
		return df

	def save(self, df):
		"""
			Chamado para cada chunk, transformado e com todas as chaves das dimensões relacionadas,
			faz a cópia do dataframe para o DW.
		:param df: Dataframe (chunk)
		"""

		conn = self.postgres_dw.get_conn_engine().raw_connection()
		cur = conn.cursor()

		try:
			self.postgres_dw.copy_df_to_table(
				df=df,
				df_columns=self.df_columns,
				table_name=self.table_name,
				commit_connection=conn,
				cursor=cur,
				index=False,
				columns=self.table_columns)
		finally:
			if not conn.closed:
				conn.close()

	def run_step(self):
		"""
			Passos a execução para todas as tabelas fato.
		"""

		self.load_dados_df()
		self.delete()
		for df in self.df_dados:
			if df is None:
				raise ValueError("Não há registros na query de dados")
			df = self.transform_dados(df)
			if df is None:
				raise ValueError(
					"Não há registros após aplicar as transformações. "
					"Tenha certeza que o método 'transform_dados' tenho o return do df.")
			df = self.apply_all_lookups(df)
			self.save(df)
