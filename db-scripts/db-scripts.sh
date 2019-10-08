#create table 
CREATE TABLE dataengproj.locations_meta_data
(
    location_id integer NOT NULL,
    detail varchar,
    geometry GEOMETRY(Point,4326),
    CONSTRAINT locations_meta_data_pkey1 PRIMARY KEY (location_id)
)

CREATE TABLE dataengproj.taxi_travel_score
(
 	location_id integer,
	event_time timestamp,
	distance_avg integer,
	psg_count_avg integer,
	amount_avg integer,
	count integer,
    score integer,
	CONSTRAINT unq_location_eventime_key1 UNIQUE(location_id,event_time)
)

CREATE TABLE dataengproj.accident_Score
(
    location_id integer,
    event_time timestamp without time zone,
    accident_type integer,
    accident_description character varying(200) COLLATE pg_catalog."default"
)

CREATE TABLE dataengproj.crime_arrest_score
(
 	location_id integer,
	event_time timestamp,
	crime_ky_cd integer,
	crime_description varchar(200),
    score integer,
	CONSTRAINT unq_location_eventime_crime_ky_cd UNIQUE(location_id,event_time,crime_ky_cd)
)




#delete table
delete from dataengproj.location_meta_data


#select data
select * from dataengproj.location_meta_data

