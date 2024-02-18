CREATE TABLE aqi_app.cities(
  country varchar(255), 
  city varchar(255), 
  count bigint, 
  first_updated varchar(255), 
  last_updated varchar(255), 
  parameters varchar(255))
  
  
  
CREATE TABLE aqi_app.daily_data(
  location varchar(255), 
  city varchar(255), 
  country varchar(255), 
  latitude float, 
  longitude float, 
  parameter varchar(255), 
  value DOUBLE PRECISION, 
  last_updated varchar(255), 
  unit varchar(255))
  
  
  CREATE TABLE aqi_app.countries(
  code varchar(255), 
  name varchar(255), 
  locations bigint, 
  first_updated varchar(255), 
  last_updated varchar(255), 
  parameters varchar(255), 
  count bigint, 
  cities bigint, 
  sources bigint)
  
  
  CREATE TABLE aqi_app.parameters(
  id bigint, 
  name varchar(255), 
  display_name varchar(255), 
  description varchar(255), 
  preferred_unit varchar(255))
  
  
  
CREATE TABLE aqi_app.aqi_data (
    location character varying(255) ENCODE lzo,
    city character varying(255) ENCODE lzo,
    country_cd character varying(255) ENCODE lzo,
    latitude character varying(255) ENCODE lzo,
    longitude character varying(255) ENCODE lzo,
    parameter character varying(255) ENCODE lzo,
    value character varying(255) ENCODE lzo,
    last_updated character varying(255) ENCODE lzo,
    unit character varying(255) ENCODE lzo,
    name character varying(255) ENCODE lzo,
    longitude_double double precision ENCODE raw,
    value_double double precision ENCODE raw,
    latitude_double double precision ENCODE raw
) DISTSTYLE AUTO;