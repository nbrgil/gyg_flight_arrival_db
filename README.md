## Flight Arrival DB

This project loads a flight arrival sample data to a postgres database.

Source: http://stat-computing.org/dataexpo/2009/the-data.html


#

#### Project summary

In this data source we have two auxiliary files:
- airports.csv
- carriers.csv


The main data, with flight arrival information, is separated by file in the format "[year].csv.bz2".
All the dimension (except carriers) and fact tables are loaded based on this file.

The star model image can be seen in 'db/sql/model.png'. There is also a xml file that can be loaded with a chrome 
extension called 'Vertabelo'.

The model:
- Flight_arrival_fact: Contains all the metrics about the flight. Delays, departure, arrival time, etc
- Cancel dimension: Contains the different types of flight cancellations;
- Carrier dimension: Contains the carrier names of the flight;
- Date dimension: Contains the full date of the flight;
- Flight dimension: Contains the codes and tail number of the flight;
- Travel dimension: Contains the origin / destination information, including the distance between them.

#### Directory summary

- auth: Authentication to a postgres DB;
- db: Database information. Models, docker e SQL files.
- dimension: Python classes representing dimension tables
- fact: Python classes representing the fact table.
- raw: Contains raw csv data. 
- util: Other useful classes.
- Makefile: Auxiliary executor file

#### Code explained

All the classes are using pandas dataframes to read data from file and load it into postgres. Sometimes, I had to 
process the data with chunks to avoid too much memory usage, but the basic code is composed by:
- READ CSV (or CSVs);
- TRANSFORM;
- LOAD TO POSTGRES (used postgres COPY to gain performance).

I used the docker hub postgres (https://hub.docker.com/_/postgres/) with the modification to copy sql files to it.
The main script is 'run.py', that:
- Downloads the raw data;
- Loads all dimensions;
- Loads fact table.

There is a Dockerfile in the root directory that installs all requirements and executes de process.


#### Executing in a local postgres:

To download a postgres docker, add the model sql and start a local postgres:
```ssh
make start-postgres
```

To download the file from http://stat-computing.org/dataexpo/2009/the-data.html, 
and load data to postgres:
 
```ssh
make run
```

#### Reporting

Didn't have the time to analyse and make much progress with reporting tools.
But I would have used, at first, the jupyter notebook. I left a sample called Reports.ipynb to where I would
start. It contains some queries and a PDF file. 