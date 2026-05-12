CREATE SCHEMA IF NOT EXISTS pap;

SET
  client_encoding = 'UTF8';

SET
  standard_conforming_strings = on;

SET
  check_function_bodies = false;

SET
  client_min_messages = warning;

SET
  search_path = pap,
  pg_catalog;

SET
  default_tablespace = '';

SET
  default_with_oids = false;

CREATE TABLE
  pap.location_dt (
    location_id integer PRIMARY KEY NOT NULL,
    latitude double precision NOT NULL,
    longitude double precision NOT NULL,
    altitude double precision
  );

CREATE TABLE
  pap.date_dt (
    date_id integer PRIMARY KEY NOT NULL,
    date date NOT NULL,
    day smallint NOT NULL CHECK (day BETWEEN 1 AND 31),
    month smallint NOT NULL CHECK (month BETWEEN 1 AND 12),
    year smallint NOT NULL
  );

CREATE TABLE
  pap.month_dt (
    month_id integer PRIMARY KEY NOT NULL,
    month smallint NOT NULL CHECK (month BETWEEN 1 AND 12),
    year smallint NOT NULL
  );

CREATE TABLE
  pap.municipality_dt (
    municipality_id integer PRIMARY KEY NOT NULL,
    name varchar(100) NOT NULL,
    urbanization varchar(100) NOT NULL,
    area double precision NOT NULL,
    littoral boolean NOT NULL DEFAULT FALSE,
    isle boolean NOT NULL DEFAULT FALSE,
    coast boolean NOT NULL DEFAULT FALSE,
    altitude_zone varchar(100) NOT NULL,
    location_id integer NOT NULL,
    CONSTRAINT fk_municipality_location FOREIGN KEY (location_id) REFERENCES pap.location_dt (location_id) ON UPDATE CASCADE ON DELETE CASCADE
  );

CREATE TABLE
  pap.station_dt (
    station_id integer PRIMARY KEY NOT NULL,
    location_id integer NOT NULL,
    name varchar(100) NOT NULL,
    type varchar(100) NOT NULL,
    municipality_id integer NOT NULL,
    CONSTRAINT fk_station_location FOREIGN KEY (location_id) REFERENCES pap.location_dt (location_id) ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT fk_station_municipality FOREIGN KEY (municipality_id) REFERENCES pap.municipality_dt (municipality_id) ON UPDATE CASCADE ON DELETE CASCADE
  );

CREATE TABLE
  pap.sensor_dt (
    sensor_id integer PRIMARY KEY NOT NULL,
    name varchar(100) NOT NULL,
    type varchar(100) NOT NULL,
    date_start date,
    date_stop date,
    uom varchar(20) NOT NULL,
    station_id integer NOT NULL,
    CONSTRAINT fk_sensor_station FOREIGN KEY (station_id) REFERENCES pap.station_dt (station_id) ON UPDATE CASCADE ON DELETE CASCADE
  );

CREATE TABLE
  pap.population_dt (
    population_id integer PRIMARY KEY NOT NULL,
    municipality_id integer NOT NULL,
    date_id integer NOT NULL,
    population integer NOT NULL,
    CONSTRAINT fk_population_municipality FOREIGN KEY (municipality_id) REFERENCES pap.municipality_dt (municipality_id) ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT fk_population_date FOREIGN KEY (date_id) REFERENCES pap.date_dt (date_id) ON UPDATE CASCADE ON DELETE CASCADE
  );

CREATE TABLE
  pap.gate_dt (
    gate_id integer PRIMARY KEY NOT NULL,
    name varchar(100) NOT NULL,
    location_id integer NOT NULL,
    CONSTRAINT fk_gate_location FOREIGN KEY (location_id) REFERENCES pap.location_dt (location_id) ON UPDATE CASCADE ON DELETE CASCADE
  );

CREATE TABLE
  pap.vehicle_dt (
    vehicle_id integer PRIMARY KEY NOT NULL,
    service varchar(100) NOT NULL,
    type varchar(100) NOT NULL,
    fuel varchar(100) NOT NULL,
    euro varchar(100) NOT NULL,
    fap varchar(100) NOT NULL,
    allowed varchar(100) NOT NULL,
    resident varchar(100) NOT NULL,
    category varchar(100) NOT NULL,
    class varchar(100) NOT NULL
  );

CREATE TABLE
  pap.sample_ft (
    sensor_id integer NOT NULL,
    date_id integer NOT NULL,
    mean_value double precision, -- TODO NOT NULL
    min_value double precision, -- TODO NOT NULL
    max_value double precision, -- TODO NOT NULL
    std_dev double precision, -- TODO NOT NULL
    PRIMARY KEY (sensor_id, date_id),
    CONSTRAINT fk_sample_sensor FOREIGN KEY (sensor_id) REFERENCES pap.sensor_dt (sensor_id) ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT fk_sample_date FOREIGN KEY (date_id) REFERENCES pap.date_dt (date_id) ON UPDATE CASCADE ON DELETE CASCADE
  );

CREATE TABLE
  pap.gate_access_ft (
    gate_id integer NOT NULL,
    date_id integer NOT NULL,
    vehicle_id integer NOT NULL,
    count integer NOT NULL DEFAULT 0,
    PRIMARY KEY (gate_id, date_id, vehicle_id),
    CONSTRAINT fk_gate_access_gate FOREIGN KEY (gate_id) REFERENCES pap.gate_dt (gate_id) ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT fk_gate_access_date FOREIGN KEY (date_id) REFERENCES pap.date_dt (date_id) ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT fk_gate_access_vehicle FOREIGN KEY (vehicle_id) REFERENCES pap.vehicle_dt (vehicle_id) ON UPDATE CASCADE ON DELETE CASCADE
  );

CREATE TABLE
  pap.overnights_ft (
    municipality_id integer NOT NULL,
    month_id integer NOT NULL,
    count integer NOT NULL DEFAULT 0,
    PRIMARY KEY (municipality_id, month_id),
    CONSTRAINT fk_overnights_municipality FOREIGN KEY (municipality_id) REFERENCES pap.municipality_dt (municipality_id) ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT fk_overnights_month FOREIGN KEY (month_id) REFERENCES pap.month_dt (month_id) ON UPDATE CASCADE ON DELETE CASCADE
  );