-- Created by Vertabelo (http://vertabelo.com)
-- Last modification date: 2018-03-17 20:31:06.625

-- tables
-- Table: cancel_dimension
CREATE TABLE cancel_dimension (
    sk_cancel serial  NOT NULL,
    is_cancelled int  NOT NULL,
    cancellation_code varchar(1)  NULL,
    reason varchar(10)  NOT NULL,
    CONSTRAINT cancel_dimension_pk PRIMARY KEY (sk_cancel)
);

-- Table: carrier_dimension
CREATE TABLE carrier_dimension (
    sk_carrier serial  NOT NULL,
    code varchar(10)  NULL,
    description varchar(100)  NULL,
    CONSTRAINT carrier_dimension_pk PRIMARY KEY (sk_carrier)
);

-- Table: date_dimension
CREATE TABLE date_dimension (
    sk_date serial  NOT NULL,
    year int  NULL,
    month int  NULL,
    day_of_month int  NULL,
    day_of_week int  NULL,
    full_date date  NULL,
    CONSTRAINT date_dimension_pk PRIMARY KEY (sk_date)
);

-- Table: flight_arrival_fact
CREATE TABLE flight_arrival_fact (
    sk_flight int  NOT NULL,
    sk_date int  NULL,
    sk_carrier int  NULL,
    sk_travel int  NULL,
    sk_cancel int  NULL,
    actual_departure_time time  NOT NULL,
    scheduled_departure_time time  NOT NULL,
    arrival_time time  NOT NULL,
    scheduled_arrival_time time  NOT NULL,
    actual_elapsed_time int  NOT NULL,
    estimated_elapsed_time int  NOT NULL,
    air_time int  NOT NULL,
    arrival_delay int  NOT NULL,
    departure_delay int  NOT NULL,
    taxi_in_time int  NOT NULL,
    taxi_out_time int  NOT NULL,
    diverted int  NOT NULL,
    carrier_delay int  NULL,
    weather_delay int  NULL,
    nas_delay int  NULL,
    security_delay int  NULL,
    late_aircraft_delay int  NULL
);

-- Table: flight_dimension
CREATE TABLE flight_dimension (
    sk_flight serial  NOT NULL,
    flight_number int  NULL,
    tail_number varchar(10)  NULL,
    CONSTRAINT flight_dimension_pk PRIMARY KEY (sk_flight)
);

-- Table: travel_dimension
CREATE TABLE travel_dimension (
    sk_travel serial  NOT NULL,
    origin_airport_iata varchar(5)  NOT NULL,
    origin_airport_name varchar(100)  NOT NULL,
    origin_city varchar(100)  NULL,
    origin_state varchar(2)  NULL,
    origin_country varchar(3)  NOT NULL,
    origin_longitude real  NULL,
    origin_latitude real  NULL,
    dest_airport_iata varchar(5)  NOT NULL,
    dest_airport_name varchar(100)  NOT NULL,
    dest_city varchar(100)  NULL,
    dest_state varchar(2)  NULL,
    dest_country varchar(3)  NOT NULL,
    dest_longitude real  NULL,
    dest_latitude real  NULL,
    distance int  NOT NULL,
    CONSTRAINT travel_dimension_pk PRIMARY KEY (sk_travel)
);

-- foreign keys
-- Reference: flight_arrival_fact_cancel_dimension (table: flight_arrival_fact)
ALTER TABLE flight_arrival_fact ADD CONSTRAINT flight_arrival_fact_cancel_dimension
    FOREIGN KEY (sk_cancel)
    REFERENCES cancel_dimension (sk_cancel)  
    NOT DEFERRABLE 
    INITIALLY IMMEDIATE
;

-- Reference: flight_arrival_fact_carrier_dimension (table: flight_arrival_fact)
ALTER TABLE flight_arrival_fact ADD CONSTRAINT flight_arrival_fact_carrier_dimension
    FOREIGN KEY (sk_carrier)
    REFERENCES carrier_dimension (sk_carrier)  
    NOT DEFERRABLE 
    INITIALLY IMMEDIATE
;

-- Reference: flight_arrival_fact_date_dimension (table: flight_arrival_fact)
ALTER TABLE flight_arrival_fact ADD CONSTRAINT flight_arrival_fact_date_dimension
    FOREIGN KEY (sk_date)
    REFERENCES date_dimension (sk_date)  
    NOT DEFERRABLE 
    INITIALLY IMMEDIATE
;

-- Reference: flight_arrival_fact_flight_dimension (table: flight_arrival_fact)
ALTER TABLE flight_arrival_fact ADD CONSTRAINT flight_arrival_fact_flight_dimension
    FOREIGN KEY (sk_flight)
    REFERENCES flight_dimension (sk_flight)  
    NOT DEFERRABLE 
    INITIALLY IMMEDIATE
;

-- Reference: travel_dimension_flight_arrival_fact (table: flight_arrival_fact)
ALTER TABLE flight_arrival_fact ADD CONSTRAINT travel_dimension_flight_arrival_fact
    FOREIGN KEY (sk_travel)
    REFERENCES travel_dimension (sk_travel)  
    NOT DEFERRABLE 
    INITIALLY IMMEDIATE
;

-- End of file.

