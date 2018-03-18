## Flight Arrival DB

This project loads a flight arrival sample data to a postgres database.

Source: http://stat-computing.org/dataexpo/2009/the-data.html

#### Directory summary

- auth: Authentication to a postgres DB;
- db: Sample postgres DB. All .sql files inside 'sql' subdirectory are executed when the postgres docker is started.
In this directory, we also have the db model (image and xml);
- dimension: Python classes representing dimension tables. Each one receives the 'year' parameter, 
to load the diff data to the table;
- fact: Python classes representing the fact table.
- raw: Contains raw csv data. The year data is not versioned, but can be
downloaded with "raw_data.py"
- util: Other useful classes.
- Makefile: Auxiliary executor file

#### Starting an empty docker postgres

You can have a localhost postgres with: 
```ssh
make dev-up
```
This will download and build a postgres with the model tables in a 
db called "FlightArrival".

#### Loading data



Each python class 