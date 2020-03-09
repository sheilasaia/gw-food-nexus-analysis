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
noaa_monthly_data <- data.frame(fips = character(),
                               noaa_station_list = character(),
                               noaa_station_count = numeric(),
                               year_month = character(),
                               noaa_var = character(),
                               value = character())
noaa_annual_data <- data.frame(fips = character(),
                               noaa_station_list = character(),
                               noaa_station_count = numeric(),
                               year = numeric(),
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
  
  # look up stations in county
  temp_county_stations <- ncdc_stations(datasetid = 'GHCND', locationid = temp_county_id)
  
  # define key cutoff variables here
  min_possible_year <- 1990 # will use this below when calculating annual values
  current_full_year <- 2019
  record_length_min_years <- 5 # decrease this to get more stations (shorter required time record)
  t_perc_annual_data_required <- 90 # X% or more temperature data required to keep data for that year
  prcp_perc_annual_data_required <- 90 # X% or more precip data required to keep data for that year

  # reformat list 
  temp_county_stations_list <- temp_county_stations$data %>%
    select(id, name, latitude, longitude, elevation, elevation_unit = elevationUnit)
  
  # select active gages with precipitation (daily total) and temperature (daily average) data
  temp_county_active_stations <- temp_county_stations_list %>%
    mutate(station_id = str_sub(id, start = 7)) %>%
    left_join(all_stations, by = c("id", "station_id")) %>%
    filter(element == "PRCP" | element == "TAVG" | element == "TMIN" | element == "TMAX") %>%
    mutate(min_year = as.numeric(first_year),
           max_year = as.numeric(last_year),
           record_length_years = max_year - min_year,
           gage_activity = ifelse(max_year >= current_full_year & record_length_years >= record_length_min_years,
                                  "active", "inactive")) %>%
    filter(gage_activity == "active") %>%
    select(-first_year, -last_year, -gage_activity)

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
      select(id, date, tavg, tmin, tmax) %>%
      mutate(no_data_test = is.na(tavg) + is.na(tmin) + is.na(tmax),
             no_min_max_pair_test = is.na(tmin) + is.na(tmax)) %>%
      filter(no_data_test != 3) %>% # if all three are NA then drop that date
      filter(no_min_max_pair_test !=1) %>% # if there is not both tmin and tmax then drop that date
      mutate(tavg_degc = tavg * 0.1, 
             tmin_degc = tmin * 0.1,
             tmax_degc = tmax * 0.1,
             year = year(date)) %>%
      select(station_id = id, date, year, tavg_degc, tmin_degc, tmax_degc)
    
    # define we have a maximum percentage of data per years
    temp_t_year_check <- temp_t_daily_data %>%
      group_by(station_id, year) %>%
      count() %>%
      mutate(percent_complete = (n / 365) * 100) %>%
      filter(percent_complete >= t_perc_annual_data_required) %>% # filter out years that are less than 90% complete
      select(station_id, year) 
    
    # only keep stations with enough records
    temp_t_daily_data_sel <- temp_t_daily_data %>%
      right_join(temp_t_year_check, by = c("station_id", "year"))
    
    # convert to monthly
    temp_t_monthly_data <- temp_t_daily_data_sel %>%
      filter(year >= min_possible_year) %>%
      mutate(year_month = paste0(year, "_", str_pad(month(date), width = 2, side = "left", pad = "0"))) %>%
      group_by(year_month) %>%
      summarise(tavg_degc_monthly = mean(tavg_degc, na.rm = TRUE),
                tmin_degc_monthly = mean(tmin_degc, na.rm = TRUE),
                tmax_degc_monthly = mean(tmax_degc, na.rm = TRUE),
                noaa_station_list = list(unique(station_id)),
                noaa_station_count = length(unique(station_id))) %>%
      pivot_longer(cols = c(tavg_degc_monthly, tmin_degc_monthly, tmax_degc_monthly), names_to = "noaa_var", values_to = "value") %>%
      mutate(fips = temp_county_id_short) %>%
      select(fips, noaa_station_list, noaa_station_count, year_month, noaa_var, value) %>%
      filter(is.nan(value) == FALSE) # keep only non-NaN values
    
    # convert to annual
    temp_t_annual_data <- temp_t_daily_data_sel %>%
      filter(year >= min_possible_year) %>%
      group_by(year) %>%
      summarise(tavg_degc_annual = mean(tavg_degc, na.rm = TRUE),
                tmin_degc_annual = mean(tmin_degc, na.rm = TRUE),
                tmax_degc_annual = mean(tmax_degc, na.rm = TRUE),
                noaa_station_list = list(unique(station_id)),
                noaa_station_count = length(unique(station_id))) %>%
      pivot_longer(cols = c(tavg_degc_annual, tmin_degc_annual, tmax_degc_annual), names_to = "noaa_var", values_to = "value") %>%
      mutate(fips = temp_county_id_short) %>%
      select(fips, noaa_station_list, noaa_station_count, year, noaa_var, value) %>%
      filter(is.nan(value) == FALSE) # keep only non-NaN values
    
  }
  
  # if only tmin or tmax stations
  else if (num_tavg_stations == 0 & (num_tmin_stations > 0 & num_tmax_stations > 0)) {
    
    # combine stations into one list
    temp_t_stations <- bind_rows(temp_tmin_stations, temp_tmax_stations)
    
    # download tidy daily data (temp data is in tenth of deg C)
    temp_t_daily_data <- rnoaa::meteo_pull_monitors(monitors = temp_t_stations$station_id) %>%
      select(id, date, tmin, tmax) %>%
      mutate(no_min_max_pair_test = is.na(tmin) + is.na(tmax)) %>%
      filter(no_min_max_pair_test == 0) %>% # if there is not both tmin and tmax then drop that date
      mutate(tmin_degc = tmin * 0.1,
             tmax_degc = tmax * 0.1,
             year = year(date)) %>%
      select(station_id = id, date, year, tmin_degc, tmax_degc)
    
    # define we have a maximum percentage of data per years
    temp_t_year_check <- temp_t_daily_data %>%
      group_by(station_id, year) %>%
      count() %>%
      mutate(percent_complete = (n / 365) * 100) %>%
      filter(percent_complete >= t_perc_annual_data_required) %>% # filter out years that are less than 90% complete
      select(station_id, year) 
    
    # only keep stations with enough records
    temp_t_daily_data_sel <- temp_t_daily_data %>%
      right_join(temp_t_year_check, by = c("station_id", "year"))
    
    # convert to monthly
    temp_t_monthly_data <- temp_t_daily_data_sel %>%
      filter(year >= min_possible_year) %>%
      mutate(year_month = paste0(year, "_", str_pad(month(date), width = 2, side = "left", pad = "0"))) %>%
      group_by(year_month) %>%
      summarise(tmin_degc_monthly = mean(tmin_degc, na.rm = TRUE),
                tmax_degc_monthly = mean(tmax_degc, na.rm = TRUE),
                noaa_station_list = list(unique(station_id)),
                noaa_station_count = length(unique(station_id))) %>%
      pivot_longer(cols = c(tmin_degc_monthly, tmax_degc_monthly), names_to = "noaa_var", values_to = "value") %>%
      mutate(fips = temp_county_id_short) %>%
      select(fips, noaa_station_list, noaa_station_count, year_month, noaa_var, value) %>%
      filter(is.nan(value) == FALSE) # keep only non-NaN values
    
    # convert to annual
    temp_t_annual_data <- temp_t_daily_data_sel %>%
      filter(year >= min_possible_year) %>%
      group_by(year) %>%
      summarise(tmin_degc_annual = mean(tmin_degc, na.rm = TRUE),
                tmax_degc_annual = mean(tmax_degc, na.rm = TRUE),
                noaa_station_list = list(unique(station_id)),
                noaa_station_count = length(unique(station_id))) %>%
      pivot_longer(cols = c(tmin_degc_annual, tmax_degc_annual), names_to = "noaa_var", values_to = "value") %>%
      mutate(fips = temp_county_id_short) %>%
      select(fips, noaa_station_list, noaa_station_count, year, noaa_var, value) %>%
      filter(is.nan(value) == FALSE) # keep only non-NaN values
    
  }
  
  # if only tavg stations
  else if (num_tavg_stations > 0 & (num_tmin_stations == 0 | num_tmax_stations == 0)) {
    
    # only use tavg stations as list
    temp_t_stations <- temp_tavg_stations
    
    # download tidy daily data (temp data is in tenth of deg C)
    temp_t_daily_data <- rnoaa::meteo_pull_monitors(monitors = temp_t_stations$station_id) %>%
      select(id, date, tavg) %>%
      na.omit() %>%
      mutate(tavg_degc = tavg * 0.1,
             year = year(date)) %>%
      select(station_id = id, date, year, tavg_degc)
    
    # define we have a maximum percentage of data per years
    temp_t_year_check <- temp_t_daily_data %>%
      group_by(station_id, year) %>%
      count() %>%
      mutate(percent_complete = (n / 365) * 100) %>%
      filter(percent_complete >= t_perc_annual_data_required) %>% # filter out years that are less than 90% complete
      select(station_id, year) 
    
    # only keep stations with enough records
    temp_t_daily_data_sel <- temp_t_daily_data %>%
      right_join(temp_t_year_check, by = c("station_id", "year"))
    
    # convert to monthly
    temp_t_monthly_data <- temp_t_daily_data_sel %>%
      filter(year >= min_possible_year) %>%
      mutate(year_month = paste0(year, "_", str_pad(month(date), width = 2, side = "left", pad = "0"))) %>%
      group_by(year_month) %>%
      summarise(tavg_degc_monthly = mean(tavg_degc, na.rm = TRUE),
                noaa_station_list = list(unique(station_id)),
                noaa_station_count = length(unique(station_id))) %>%
      pivot_longer(cols = tavg_degc_monthly, names_to = "noaa_var", values_to = "value") %>%
      mutate(fips = temp_county_id_short) %>%
      select(fips, noaa_station_list, noaa_station_count, year_month, noaa_var, value) %>%
      filter(is.nan(value) == FALSE) # keep only non-NaN values
    
    # convert to annual
    temp_t_annual_data <- temp_t_daily_data_sel %>%
      filter(year >= min_possible_year) %>%
      group_by(year) %>%
      summarise(tavg_degc_annual = mean(tavg_degc, na.rm = TRUE),
                noaa_station_list = list(unique(station_id)),
                noaa_station_count = length(unique(station_id))) %>%
      pivot_longer(cols = tavg_degc_annual, names_to = "noaa_var", values_to = "value") %>%
      mutate(fips = temp_county_id_short) %>%
      select(fips, noaa_station_list, noaa_station_count, year, noaa_var, value) %>%
      filter(is.nan(value) == FALSE) # keep only non-NaN values
    
  }
  
  # make an empty data frame
  else {
    # monthly
    temp_t_monthly_data <- data.frame(fips = temp_county_id_short,
                                      noaa_station_list = NA,
                                      noaa_station_count = 0,
                                      year_month = NA,
                                      noaa_var = NA,
                                      value = NA)
    
    # annual
    temp_t_annual_data <- data.frame(fips = temp_county_id_short,
                                     noaa_station_list = NA,
                                     noaa_station_count = 0,
                                     year = NA,
                                     noaa_var = NA,
                                     value = NA)
  }
  
  # precipitation
  # only want to get data 
  if (num_prcp_stations > 0) {
    
    # download tidy daily data (precip data is in tenth of mm)
    temp_prcp_daily_data <- rnoaa::meteo_pull_monitors(monitors = temp_prcp_stations$station_id) %>%
      select(id, date, prcp) %>%
      na.omit() %>%
      mutate(prcp_mm = prcp * 0.1,
             year = year(date)) %>%
      select(station_id = id, date, year, prcp_mm)
    
    # define we have a maximum percentage of data per years
    temp_prcp_year_check <- temp_prcp_daily_data %>%
      group_by(station_id, year) %>%
      count() %>%
      mutate(percent_complete = (n / 365) * 100) %>%
      filter(percent_complete >= prcp_perc_annual_data_required) %>% # filter out years that are less than 90% complete
      select(station_id, year) 
    
    # only keep stations with enough records
    temp_prcp_daily_data_sel <- temp_prcp_daily_data %>%
      right_join(temp_prcp_year_check, by = c("station_id", "year"))
    
    # convert to monthly
    temp_prcp_monthly_data <- temp_prcp_daily_data_sel %>%
      filter(year >= min_possible_year) %>%
      mutate(year_month = paste0(year, "_", str_pad(month(date), width = 2, side = "left", pad = "0"))) %>%
      group_by(year_month) %>%
      summarise(prcp_mm_monthly = sum(prcp_mm, na.rm = TRUE),
                noaa_station_list = list(unique(station_id)),
                noaa_station_count = length(unique(station_id))) %>%
      pivot_longer(cols = prcp_mm_monthly, names_to = "noaa_var", values_to = "value") %>%
      mutate(fips = temp_county_id_short) %>%
      select(fips, noaa_station_list, noaa_station_count, year_month, noaa_var, value) %>%
      filter(is.nan(value) == FALSE) # keep only non-NaN values
    
    # convert to annual
    temp_prcp_annual_data <- temp_prcp_daily_data_sel %>%
      filter(year >= min_possible_year) %>%
      group_by(year) %>%
      summarise(prcp_mm_annual = sum(prcp_mm, na.rm = TRUE),
                noaa_station_list = list(unique(station_id)),
                noaa_station_count = length(unique(station_id))) %>%
      pivot_longer(cols = prcp_mm_annual, names_to = "noaa_var", values_to = "value") %>%
      mutate(fips = temp_county_id_short) %>%
      select(fips, noaa_station_list, noaa_station_count, year, noaa_var, value) %>%
      filter(is.nan(value) == FALSE) # keep only non-NaN values

  }
  
  # make an empty data frame
  else {
    # monthly
    temp_prcp_monthly_data <- data.frame(fips = temp_county_id_short,
                                         noaa_station_list = NA,
                                         noaa_station_count = 0,
                                         year_month = NA,
                                         noaa_var = "prcp_mm_monthly",
                                         value = NA)
    
    # annual
    temp_prcp_annual_data <- data.frame(fips = temp_county_id_short,
                                        noaa_station_list = NA,
                                        noaa_station_count = 0,
                                        year = NA,
                                        noaa_var = "prcp_mm_annual",
                                        value = NA)
  }
  
  # bind data
  temp_noaa_monthly_data <- bind_rows(temp_prcp_monthly_data, temp_t_monthly_data)
  temp_noaa_annual_data <- bind_rows(temp_prcp_annual_data, temp_t_annual_data)
  
  # append to full noaa data frame
  noaa_monthly_data <- bind_rows(temp_noaa_monthly_data, noaa_monthly_data)
  noaa_annual_data <- bind_rows(temp_noaa_annual_data, noaa_annual_data)
  
  # print update
  perc_complete <- round(i/dim(county_ids)[1], 3) * 100
  print(paste0("Finished county id # ", temp_county_id_short, ". Run is ", perc_complete, "% complete."))

}


# ---- 4. final wrangling ----

# monthly data
noaa_annual_data_clean <- noaa_annual_data %>%
  filter(noaa_station_count != 0) %>%
  rowwise() %>%
  mutate(noaa_station_list_str = str_replace_all(str_c(unlist(noaa_station_list), collapse = " "), pattern = " ", replacement = ", ")) %>%
  pivot_wider(names_from = noaa_var, values_from = value) %>%
  select(fips, year_month, prcp_mm_monthly:tavg_degc_monthly, noaa_station_list_str, noaa_station_count) %>%
  arrange(fips, year_month)

# annual data
noaa_annual_data_clean <- noaa_annual_data %>%
  filter(noaa_station_count != 0) %>%
  rowwise() %>%
  mutate(noaa_station_list_str = str_replace_all(str_c(unlist(noaa_station_list), collapse = " "), pattern = " ", replacement = ", ")) %>%
  pivot_wider(names_from = noaa_var, values_from = value) %>%
  select(fips, year, prcp_mm_annual:tavg_degc_annual, noaa_station_list_str, noaa_station_count) %>%
  arrange(fips, year)


# ---- 5. export ----

# export noaa annual data
write_csv(noaa_monthly_data_clean, paste0(tabular_data_output_path, "noaa_monthly_data.csv"))
write_csv(noaa_annual_data_clean, paste0(tabular_data_output_path, "noaa_annual_data.csv"))



