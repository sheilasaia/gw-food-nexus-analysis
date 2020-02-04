# retrieving noaa data

# ---- 1. set up ----

# load libraries
library(tidyverse)
library(here)
library(rnoaa) # https://github.com/ropensci/rnoaa
library(maps)
library(lubridate)
library(dataRetrieval)

# define paths
# when we put the final data on github we'll use relative paths based on the project
# project_path <- here::here()

# force direct paths for now (need to figure out data storage structure so county data is in same directory)
county_data_path <- "/Users/sheila/Dropbox/GW-Food Nexus/tabular_data/" # here::here("tabular_data", "nass_data")
tabular_data_path <- "/Users/sheila/Dropbox/GW-Food Nexus/tabular_data/noaa_data/" # here::here("tabular_data", "noaa_data")
tabular_data_output_path <- "/Users/sheila/Dropbox/GW-Food Nexus/tabular_data/noaa_data/reformatted_data/" # here::here("tabular_data", "noaa_data", "reformatted_data")

# load ncdc key
# ncdc_key = "<your ncdc api key here>"
options(noaakey = ncdc_key)

# element descriptions for noaa ghcn data here:
# https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/readme.txt


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

# get all the station metadta (just do this once! b/c it takes a while...)
all_stations_raw <- rnoaa::ghcnd_stations()

# reformatted all stations 
all_stations <- all_stations_raw %>%
  mutate(station_id = id) %>%
  select(-id) %>%
  mutate(id = paste0("GHCND:", station_id)) %>%
  select(id, station_id, state, gsn_flag, wmo_id, element, first_year, last_year)
# change so id column matches output from ncdc_stations()

# make an empty list for all the data
noaa_annual_data <- data.frame(fips = character(),
                               noaa_station_list = character(),
                               noaa_station_count = numeric(),
                               water_year = numeric(),
                               noaa_var = character(),
                               value = character())

# loop through ag dominated counties
for (i in 1:dim(county_ids)[1]) {
  
  # variable naming conventions:
  # temp refers to a temporary variable that will be redefined for each loop
  # t refers to temperature
  
  # select count
  temp_county_id_short <- county_ids$fips[i]
  temp_county_id <- paste0("FIPS:", temp_county_id_short)
  # issue at i = 255
  
  # look up stations in county
  temp_county_stations <- ncdc_stations(datasetid = 'GHCND', locationid = temp_county_id)
  
  # reformat list
  current_year <- 2020
  record_length_min_years <- 10 # decrease this to get more stations (shorter required time record)
  temp_county_stations_list <- temp_county_stations$data %>%
    select(id, name, latitude, longitude, elevation, elevation_unit = elevationUnit, mindate, maxdate, data_coverage = datacoverage) %>%
    mutate(min_date = ymd(mindate),
           max_date = ymd(maxdate),
           min_wy = as.character(dataRetrieval::calcWaterYear(min_date)),
           max_wy = as.character(dataRetrieval::calcWaterYear(max_date)),
           min_wy_date = ymd(paste0(min_wy, "-10-01")),
           max_wy_date = ymd(paste0(max_wy, "-09-30")),
           record_length_years = year(max_wy_date) - year(min_wy_date),
           gage_activity = ifelse(year(max_wy_date) == current_year & record_length_years >= record_length_min_years,
                                  "active", "inactive")) %>%
    select(-mindate, -maxdate, -min_wy, -max_wy)
  
  # select active gages with precipitation (daily total) and temperature (daily average) data
  temp_county_active_stations <- temp_county_stations_list %>%
    filter(gage_activity == "active") %>%
    select(-gage_activity) %>%
    mutate(station_id = str_sub(id, start = 7)) %>%
    left_join(all_stations, by = c("id", "station_id")) %>%
    filter(element == "PRCP" | element == "TAVG" | element == "TMIN" | element == "TMAX") %>%
    mutate(first_wy = year(min_wy_date) + 1, # add one to buffer
           last_wy = year(max_wy_date) - 1) # subract one to buffer
  # TODO can make this this buffer more specific in future
  
  # active temperature stations
  temp_tavg_stations <- temp_county_active_stations %>%
    filter(element == "TAVG")
  temp_tmin_stations <- temp_county_active_stations %>%
    filter(element == "TMIN")
  temp_tmax_stations <- temp_county_active_stations %>%
    filter(element == "TMAX")
  
  # active precipitation stations
  temp_prcp_stations <- temp_county_active_stations %>%
    filter(element == "PRCP")
  
  # check length of stations
  num_tavg_stations <- dim(temp_tavg_stations)[1]
  num_tmin_stations <- dim(temp_tmin_stations)[1]
  num_tmax_stations <- dim(temp_tmax_stations)[1]
  num_prcp_stations <- dim(temp_prcp_stations)[1]
  
  # temperature
  # if there are tavg, tmin, and tmax stations
  if (num_tavg_stations > 0 & num_tmin_stations > 0 & num_tmax_stations > 0) {
    
    # combine stations into one list
    temp_t_stations <- bind_rows(temp_tavg_stations, temp_tmin_stations, temp_tmax_stations)
    
    # download tidy daily data (temp data is in tenth of deg C)
    temp_t_daily_data <- rnoaa::meteo_pull_monitors(monitors = temp_t_stations$station_id) %>%
      mutate(tavg_degc = tavg * 0.1, 
             tmin_degc = tmin * 0.1,
             tmax_degc = tmax * 0.1) %>%
      select(station_id = id, date, tavg_degc, tmin_degc, tmax_degc)
    
    # create lookup table for water year bounds
    temp_t_stations_wy_lookup <- temp_t_stations %>%
      select(station_id, first_wy, last_wy)
    
    # account for water year (from 10/1 to 9/31)
    temp_t_daily_data_wy <- temp_t_daily_data %>%
      mutate(water_year = dataRetrieval::calcWaterYear(date)) %>%
      left_join(temp_t_stations_wy_lookup, by = "station_id") %>%
      filter(water_year >= first_wy & water_year <= last_wy) %>%
      select(-first_wy, -last_wy)
    
    # define we have a maximum percentage of data per years
    t_perc_annual_data_required <- 90 # X% or more data required to keep data for that year
    temp_t_year_check <- temp_t_daily_data_wy %>%
      group_by(station_id, water_year) %>%
      count() %>%
      mutate(percent_complete = (n / 365) * 100) %>%
      filter(percent_complete >= t_perc_annual_data_required) %>% # filter out years that are less than 90% complete
      select(station_id, water_year) 
    
    # only keep stations with enough records
    temp_t_daily_data_wy_sel <- temp_t_daily_data_wy %>%
      right_join(temp_t_year_check, by = c("station_id", "water_year"))
    
    # convert to annual
    temp_t_annual_data <- temp_t_daily_data_wy %>%
      group_by(water_year) %>%
      summarise(tavg_degc_annual = mean(tavg_degc, na.rm = TRUE),
                tmin_degc_annual = mean(tmin_degc, na.rm = TRUE),
                tmax_degc_annual = mean(tmax_degc, na.rm = TRUE),
                noaa_station_list = list(unique(station_id)),
                noaa_station_count = length(unique(station_id))) %>%
      pivot_longer(cols = c(tavg_degc_annual, tmin_degc_annual, tmax_degc_annual), names_to = "noaa_var", values_to = "value") %>%
      mutate(fips = temp_county_id_short) %>%
      select(fips, noaa_station_list, noaa_station_count, water_year:value) %>%
      filter(is.nan(value) == FALSE) # keep only non-NaN values
    
  }
  
  # if only tmin or tmax stations
  else if (num_tavg_stations == 0 & (num_tmin_stations > 0 & num_tmax_stations > 0)) {
    
    # combine stations into one list
    temp_t_stations <- bind_rows(temp_tmin_stations, temp_tmax_stations)
    
    # download tidy daily data (temp data is in tenth of deg C)
    temp_t_daily_data <- rnoaa::meteo_pull_monitors(monitors = temp_t_stations$station_id) %>%
      mutate(tmin_degc = tmin * 0.1,
             tmax_degc = tmax * 0.1) %>%
      select(station_id = id, date, tmin_degc, tmax_degc)
    
    # create lookup table for water year bounds
    temp_t_stations_wy_lookup <- temp_t_stations %>%
      select(station_id, first_wy, last_wy)
    
    # account for water year (from 10/1 to 9/31)
    temp_t_daily_data_wy <- temp_t_daily_data %>%
      mutate(water_year = dataRetrieval::calcWaterYear(date)) %>%
      left_join(temp_t_stations_wy_lookup, by = "station_id") %>%
      filter(water_year >= first_wy & water_year <= last_wy) %>%
      select(-first_wy, -last_wy)
    
    # define we have a maximum percentage of data per years
    t_perc_annual_data_required <- 90 # X% or more data required to keep data for that year
    temp_t_year_check <- temp_t_daily_data_wy %>%
      group_by(station_id, water_year) %>%
      count() %>%
      mutate(percent_complete = (n / 365) * 100) %>%
      filter(percent_complete >= t_perc_annual_data_required) %>% # filter out years that are less than 90% complete
      select(station_id, water_year) 
    
    # only keep stations with enough records
    temp_t_daily_data_wy_sel <- temp_t_daily_data_wy %>%
      right_join(temp_t_year_check, by = c("station_id", "water_year"))
    
    # convert to annual
    temp_t_annual_data <- temp_t_daily_data_wy %>%
      group_by(water_year) %>%
      summarise(tmin_degc_annual = mean(tmin_degc, na.rm = TRUE),
                tmax_degc_annual = mean(tmax_degc, na.rm = TRUE),
                noaa_station_list = list(unique(station_id)),
                noaa_station_count = length(unique(station_id))) %>%
      pivot_longer(cols = c(tmin_degc_annual, tmax_degc_annual), names_to = "noaa_var", values_to = "value") %>%
      mutate(fips = temp_county_id_short) %>%
      select(fips, noaa_station_list, noaa_station_count, water_year:value) %>%
      filter(is.nan(value) == FALSE) # keep only non-NaN values
    
  }
  
  # if only tavg stations
  else if (num_tavg_stations > 0 & (num_tmin_stations == 0 | num_tmax_stations == 0)) {
    
    # only use tavg stations as list
    temp_t_stations <- temp_tavg_stations
    
    # download tidy daily data (temp data is in tenth of deg C)
    temp_t_daily_data <- rnoaa::meteo_pull_monitors(monitors = temp_t_stations$station_id) %>%
      mutate(tavg_degc = tavg * 0.1) %>%
      select(station_id = id, date, tavg_degc)
    
    # create lookup table for water year bounds
    temp_t_stations_wy_lookup <- temp_t_stations %>%
      select(station_id, first_wy, last_wy)
    
    # account for water year (from 10/1 to 9/31)
    temp_t_daily_data_wy <- temp_t_daily_data %>%
      mutate(water_year = dataRetrieval::calcWaterYear(date)) %>%
      left_join(temp_t_stations_wy_lookup, by = "station_id") %>%
      filter(water_year >= first_wy & water_year <= last_wy) %>%
      select(-first_wy, -last_wy)
    
    # define we have a maximum percentage of data per years
    t_perc_annual_data_required <- 90 # X% or more data required to keep data for that year
    temp_t_year_check <- temp_t_daily_data_wy %>%
      group_by(station_id, water_year) %>%
      count() %>%
      mutate(percent_complete = (n / 365) * 100) %>%
      filter(percent_complete >= t_perc_annual_data_required) %>% # filter out years that are less than 90% complete
      select(station_id, water_year) 
    
    # only keep stations with enough records
    temp_t_daily_data_wy_sel <- temp_t_daily_data_wy %>%
      right_join(temp_t_year_check, by = c("station_id", "water_year"))
    
    # convert to annual
    temp_t_annual_data <- temp_t_daily_data_wy %>%
      group_by(water_year) %>%
      summarise(tavg_degc_annual = mean(tavg_degc, na.rm = TRUE),
                noaa_station_list = list(unique(station_id)),
                noaa_station_count = length(unique(station_id))) %>%
      pivot_longer(cols = tavg_degc_annual, names_to = "noaa_var", values_to = "value") %>%
      mutate(fips = temp_county_id_short) %>%
      select(fips, noaa_station_list, noaa_station_count, water_year:value) %>%
      filter(is.nan(value) == FALSE) # keep only non-NaN values
    
  }
  
  # make an empty data frame
  else {
    temp_t_annual_data <- data.frame(fips = temp_county_id_short,
                                        noaa_station_list = NA,
                                        noaa_station_count = 0,
                                        water_year = NA,
                                        noaa_var = NA,
                                        value = NA)
  }
  
  # precipitation
  # only want to get data 
  if (num_prcp_stations > 0) {
    
    # download tidy daily data (precip data is in tenth of mm)
    temp_prcp_daily_data <- rnoaa::meteo_pull_monitors(monitors = temp_prcp_stations$station_id) %>%
      mutate(prcp_mm = prcp * 0.1) %>%
      select(station_id = id, date, prcp_mm)
    
    # create lookup table for water year bounds
    temp_prcp_stations_wy_lookup <- temp_prcp_stations %>%
      select(station_id, first_wy, last_wy)
    
    # account for water year (from 10/1 to 9/31)
    temp_prcp_daily_data_wy <- temp_prcp_daily_data %>%
      mutate(water_year = dataRetrieval::calcWaterYear(date)) %>%
      left_join(temp_prcp_stations_wy_lookup, by = "station_id") %>%
      filter(water_year >= first_wy & water_year <= last_wy) %>%
      select(-first_wy, -last_wy)
    
    # define we have a maximum percentage of data per years
    prcp_perc_annual_data_required <- 90 # X% or more data required to keep data for that year
    temp_prcp_year_check <- temp_prcp_daily_data_wy %>%
      group_by(station_id, water_year) %>%
      count() %>%
      mutate(percent_complete = (n / 365) * 100) %>%
      filter(percent_complete >= prcp_perc_annual_data_required) %>% # filter out years that are less than 90% complete
      select(station_id, water_year) 
    
    # only keep stations with enough records
    temp_prcp_daily_data_wy_sel <- temp_prcp_daily_data_wy %>%
      right_join(temp_prcp_year_check, by = c("station_id", "water_year"))
    
    # convert to annual
    temp_prcp_annual_data <- temp_prcp_daily_data_wy %>%
      group_by(water_year) %>%
      summarise(noaa_station_list = list(unique(station_id)),
                noaa_station_count = length(unique(station_id)),
                prcp_mm_annual = sum(prcp_mm, na.rm = TRUE)/noaa_station_count) %>%
      pivot_longer(cols = prcp_mm_annual, names_to = "noaa_var", values_to = "value") %>%
      mutate(fips = temp_county_id_short) %>%
      select(fips, noaa_station_list, noaa_station_count, water_year:value)

  }
  
  # make an empty data frame
  else {
    temp_prcp_annual_data <- data.frame(fips = temp_county_id_short,
                                        noaa_station_list = NA,
                                        noaa_station_count = 0,
                                        water_year = NA,
                                        noaa_var = "prcp_mm_annual",
                                        value = NA)
  }
  
  # bind data and keep only years 1990 and above
  temp_noaa_annual_data <- bind_rows(temp_prcp_annual_data, temp_t_annual_data) %>%
    filter(water_year >= 1990)
  
  # append to full noaa data frame
  noaa_annual_data <- bind_rows(temp_noaa_annual_data, noaa_annual_data)
  
  # print update
  perc_complete <- round(i/dim(county_ids)[1], 3) * 100
  print(paste0("Finished county id # ", temp_county_id_short, ". Run is ", perc_complete, "% complete."))

}
    
# TODO there are several sites without tavg available

# ---- 4. final wrangling ----

noaa_annual_data_clean <- noaa_annual_data %>%
  rowwise() %>%
  mutate(noaa_station_list_str = str_replace_all(str_c(unlist(noaa_station_list), collapse = " "), pattern = " ", replacement = ", ")) %>%
  pivot_wider(names_from = noaa_var, values_from = value) %>%
  select(fips, water_year, prcp_mm_annual:tavg_degc_annual, noaa_station_list_str, noaa_station_count) %>%
  arrange(fips, water_year)


# ---- 5. export ----

# export noaa annual data
write_csv(noaa_annual_data_clean, paste0(tabular_data_output_path, "noaa_annual_data.csv"))
