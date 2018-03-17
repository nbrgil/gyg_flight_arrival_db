# coding=utf-8
import sqlalchemy
import pandas as pd
import json
import io


class PostgresClient:
	"""
		Classe para encapsular conexão com Postgres
	"""

	def __init__(self, auth_path="auth/postgres.json"):
		"""
			Construtor
		:param auth_path: Caminho do arquivo json com HOST, DB, USER, PWD e PORT
		"""
		self.__conn_engine = None
		self.__auth_params = None
		self.__auth_path = auth_path
		self.__connection_string = "postgresql+psycopg2://{USER}:{PWD}@{HOST}:{PORT}/{DB}"
		self.__csv_encoding = "utf-8"
		self.__read_auth_file()

	def __read_auth_file(self):
		"""
			Faz a leitura do arquivo de autenticação
		"""
		with open(self.__auth_path) as f:
			self.__auth_params = json.load(f)

	def __authenticate(self):
		"""
			Formata parâmetros para string de conexão e conecta com ODBC do SqlServer
		"""
		conn_params = self.__connection_string.format(**self.__auth_params)
		self.__conn_engine = sqlalchemy.create_engine(
			conn_params,
			pool_size=5,
			max_overflow=0
		)

	def get_conn_engine(self) -> sqlalchemy.engine.Engine:
		"""
			Retorna a conexão ODBC com o SQL server
		"""
		if self.__conn_engine is None:
			self.__authenticate()

		return self.__conn_engine

	def chunk_sql_to_csv(self, query, file_path, query_params=None, csv_delimiter=";", compression=False,
						 chunksize=1000):
		"""
			Executa uma query e gera o resultado em um arquivo .csv
		:param query: Consulta que será realizada
		:param file_path: Caminho do arquivo que será gerado
		:param query_params: Parâmetros da consulta informada
		:param csv_delimiter: Demilitador do arquivo csv (padrão ;)
		:param compression: Informa se deve compactar o arquivo para gzip
		:return:
		"""
		compression_string = None
		if compression:
			compression_string = "gzip"

		df_iter = pd.read_sql(sql=query, con=self.get_conn_engine(), params=query_params, chunksize=chunksize)
		header = True

		for df in df_iter:  # type: pd.DataFrame
			df.to_csv(
				path_or_buf=file_path, mode="a", sep=csv_delimiter, encoding=self.__csv_encoding, header=header,
				index=False, compression=compression_string
			)
			header = False

	def copy_df_iter_to_table(self, df_iter, table_name, sep=";", header=False, index=False):
		conn = self.get_conn_engine().raw_connection()
		cur = conn.cursor()
		for df in df_iter:
			self.copy_df_to_table(df, table_name, cur, conn, sep, header, index)
		conn.close()

	@staticmethod
	def copy_df_to_table(
			df, table_name, cursor, commit_connection=None, sep=";", header=False, index=False,
			columns=None, df_columns=None):
		"""
			Copiar um dataframe para o postgres usando o COPY
		:param df: Dataframe
		:param table_name: Nome da tabela de destino
		:param cursor: Cursor já aberto
		:param commit_connection: Conexão do cursor, se desejar commit dentro da função
		:param sep: Separador | padrão ';'
		:param header: Indica se atualiza o header
		:param index: Indica se atualiza índice
		:param columns: Colunas da tabela destino
		:param df_columns: Colunas do dataframe que serão exportadas
		"""
		output = io.StringIO()
		df.to_csv(output, sep=sep, header=header, index=index, columns=df_columns)
		output.seek(0)
		cursor.copy_from(output, table_name, null="", sep=sep, columns=columns)
		if commit_connection is not None:
			commit_connection.commit()

	@staticmethod
	def copy_df_to_csv_table(
			df, table_name, cursor, commit_connection=None, sep=";", header=False, index=False,
			columns=None, df_columns=None, file_name="/tmp/file_{}.csv"):
		"""
			Copiar um dataframe para o postgres usando o COPY (intermédio de um CSV)
		:param df: Dataframe
		:param table_name: Nome da tabela de destino
		:param cursor: Cursor já aberto
		:param commit_connection: Conexão do cursor, se desejar commit dentro da função
		:param sep: Separador | padrão ';'
		:param header: Indica se atualiza o header
		:param index: Indica se atualiza índice
		:param columns: Colunas da tabela destino
		:param df_columns: Colunas do dataframe que serão exportadas
		"""
		file_name = file_name.format(table_name)

		df.to_csv(file_name, sep=sep, header=header, index=index, columns=df_columns)
		file = open(file_name, 'r')
		try:
			cursor.copy_from(file, table_name, null="", sep=sep, columns=columns)
			if commit_connection is not None:
				commit_connection.commit()
		finally:
			file.close()