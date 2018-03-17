from util.utils import download_file
from definitions import ROOT_DIR
import os


def get_flight_arrival_data(year):
	download_file(
		url="http://stat-computing.org/dataexpo/2009/{}.csv.bz2".format(year),
		file_name=os.path.join(ROOT_DIR, "raw", "{}.csv.bz2".format(str(year)))
	)


if __name__ == '__main__':
	get_flight_arrival_data(2008)
