import urllib.request
import shutil

from util.postgres_client import PostgresClient
from definitions import ROOT_DIR
import os

postgres = None


def download_file(url, file_name):
	with urllib.request.urlopen(url) as response, open(file_name, 'wb') as out_file:
		shutil.copyfileobj(response, out_file)


def get_db_client():
	global postgres
	if postgres is None:
		postgres = PostgresClient(
			auth_path=os.path.join(ROOT_DIR, "auth", "{}.json".format(os.getenv("PGHOST", "localhost"))))

	return postgres


def sum_lists_without_duplicates(first: list, second: list):
	"""
		Faz a junção de duas listas, removendo os duplicados
		Ex: [1, 2] + [1, 3] = [1, 2, 3]
	:param first:
	:param second:
	:return:
	"""
	return first + list(set(second) - set(first))
