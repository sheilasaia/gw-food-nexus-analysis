# retrieving noaa data

# ---- 1. set up ----

# load libraries
library(tidyverse)
library(here)
library(rnoaa) # https://github.com/ropensci/rnoaa
library(maps)
library(lubridate)

# define paths
# when we put the final data on github we'll use relative paths based on the project
# project_path <- here::here()

# force direct paths for now (need to figure out data storage structure so county data is in same directory)
county_data_path <- "/Users/ssaia/Dropbox/GW-Food Nexus/tabular_data/" # here::here("tabular_data", "nass_data")
tabular_data_path <- "/Users/ssaia/Dropbox/GW-Food Nexus/tabular_data/noaa_data/" # here::here("tabular_data", "noaa_data")
tabular_data_output_path <- "/Users/ssaia/Dropbox/GW-Food Nexus/tabular_data/noaa_data/reformatted_data/" # here::here("tabular_data", "noaa_data", "reformatted_data")

# load ncdc key
# ncdc_key = "<your nass key here>"
options(noaakey = ncdc_key)


# ---- 2. load state and county metadata ----

# state fips and abbreviation lookup
state_ids <- maps::state.fips %>%
  mutate(fips_pad = as.character(str_pad(fips, 2, pad = "0"))) %>%
  select(state_alpha = abb, 
         state_fips = fips_pad) %>%
  filter(state_alpha != "DC") %>%
  distinct()

# load county ids data (from 'AGDominatedCounties.xlsx' file)
county_ids_raw <- read_csv(paste0(county_data_path, "ag_dominated_counties_update05082019.csv"))

# define fips codes
county_ids <- county_ids_raw %>%
  mutate(state_fips_pad = str_pad(state_fips, 2, pad = "0"), # pad with zeros if not 2 digits
         county_fips_pad = str_pad(county_fips, 3, pad = "0"), #pad with zeros if not 3 digits
         fips = as.character(paste0(state_fips_pad, county_fips_pad))) %>% # create fips key for later dataframe joining
  select(-state_fips, -county_fips) %>%
  select(fips, state_fips = state_fips_pad, county_fips = county_fips_pad) %>%
  left_join(state_ids, by = "state_fips") %>%
  arrange(fips)


# ---- 3. get ghcnd data ----

# select county
temp_county_id <- paste0("FIPS:", county_ids$fips[1])

# look up stations in county
temp_stations <- ncdc_stations(datasetid = 'GHCND', locationid = temp_county_id)

# reformat list
current_year <- 2019
record_length_min_years <- 10 # change this to get longer 
temp_stations_list <- temp_stations$data %>%
  select(id, name, latitude, longitude, elevation, elevation_unit = elevationUnit, mindate, maxdate, data_coverage = datacoverage) %>%
  mutate(min_date = ymd(mindate),
         max_date = ymd(maxdate),
         record_length_years = year(max_date) - year(min_date),
         gage_activity = ifelse(year(max_date) == year_now & current_year >= record_length_min_years,
                              "active", "inactive")) %>%
  select(-mindate, -maxdate)

# select active gages
temp_active_stations <- temp_stations_list %>%
  filter(gage_activity == "active") %>%
  select(-gage_activity) %>%
  mutate(station_id = str_sub(id, start = 7))

# TO DO: get daily data for active stations that have both daily precip and avg temp
my_stations_short <- temp_active_stations$station_id
my_stations_long <- temp_active_stations$id
blah <- ghcnd(my_station_short, var = c("PRCP", "TAVG")) # daily values in tenths of mm and deg C
# variables: https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/readme.txt
blah2 <- ncdc_datacats(datasetid = "GHCND", stationid = my_stations_long)
blah3 <- ghcnd_stations() # lookup want elements = PRCP and TAVG
