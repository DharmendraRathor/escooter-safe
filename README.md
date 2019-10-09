# escooter-safe
Application to help electric scooter business companies find profitable and safe business locations

Project aim is to do batch processing of New york taxi and crime data and come up with scores
for each locaitons.  
Score will help to find locations which has chances of having more eScooter usage
Score will also help to find locations which are safe to customer and eScooter companies.


#db scripts
db script is to create tables in postGIS database
db-scripts/db-scripts.sh


#Input data used is in Amazon s3 bucket
  nyc taxi data (mynytc/trip data/)
  crime data (nyc-crime/)

#Running the script 
Meta data for locaiton to be loaded in data table first
Script : /src/meta-data/load_lactaion_meta_data.py

#Running crime data processing 
Script to load crime data in crime score tables for locations
Script : src/crime/crime_processor.py

#Taxi data processing.
Script to load taxi data in taxi locaiton score tables for locations
Script : src/taxi/taxi_processor.py




