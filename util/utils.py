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
		postgres = PostgresClient(auth_path=os.path.join(ROOT_DIR, "auth", "postgres.json"))

	return postgres
