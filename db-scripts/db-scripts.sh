#create table 
CREATE TABLE dataengproj.locations_meta_data
(
    location_id integer NOT NULL,
	detail varchar,
    geometry GEOMETRY(Point,4326),
    CONSTRAINT locations_meta_data_pkey1 PRIMARY KEY (location_id)
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

