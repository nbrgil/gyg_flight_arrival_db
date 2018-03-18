import os

from dimension.cancel_dimension import CancelDimension
from dimension.carrier_dimension import CarrierDimension
from dimension.date_dimension import DateDimension
from dimension.flight_dimension import FlightDimension
from dimension.travel_dimension import TravelDimension
from fact.flight_arrival_fact import FlightArrivalFact
from raw.raw_data import get_flight_arrival_data
import logging

year = os.environ["FL_ARR_YEAR"]

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.info("**** Loading data for {}".format(year))

logging.info("Getting data source...".format(year))
get_flight_arrival_data(year)
logging.info("Getting data source... ok!".format(year))

logging.info("Loading cancel dimension...".format(year))
CancelDimension(year).run()

logging.info("Loading carrier dimension... ".format(year))
CarrierDimension().run()

logging.info("Loading date dimension... ".format(year))
DateDimension(year).run()

logging.info("Loading flight dimension...".format(year))
FlightDimension(year).run()

logging.info("Loading travel dimension...".format(year))
TravelDimension(year).run()

logging.info("Loading fact...".format(year))
FlightArrivalFact(year).run()

logging.info("Data loaded!".format(year))