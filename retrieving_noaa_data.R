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

# get all the stations (just do this once)
all_stations_raw <- ghcnd_stations()

# reformatted all stations 
all_stations <- all_stations_raw %>%
  mutate(station_id = id) %>%
  select(-id) %>%
  mutate(id = paste0("GHCND:", station_id)) %>%
  select(id, station_id, state, gsn_flag, wmo_id, element, first_year, last_year)
# change so id column matches output from ncdc_stations()

# make an empty list for all the data
noaa_annual_data <- data.frame(fips = character(),
                               year = numeric(),
                               tavg_annual_degc = numeric(),
                               precip_annual_mm = numeric())

# (loop should start here)
for (i in 1:dim(county_ids)[1]) {
  
  # select count
  temp_county_id_short <- county_ids$fips[i]
  temp_county_id <- paste0("FIPS:", temp_county_id_short)
  
  # look up stations in county
  temp_county_stations <- ncdc_stations(datasetid = 'GHCND', locationid = temp_county_id)
  
  # reformat list
  current_year <- 2020
  record_length_min_years <- 10 # decrease this to get more stations (shorter required time record)
  temp_county_stations_list <- temp_county_stations$data %>%
    select(id, name, latitude, longitude, elevation, elevation_unit = elevationUnit, mindate, maxdate, data_coverage = datacoverage) %>%
    mutate(min_date = ymd(mindate),
           max_date = ymd(maxdate),
           record_length_years = year(max_date) - year(min_date),
           gage_activity = ifelse(year(max_date) == current_year & record_length_years >= record_length_min_years,
                                  "active", "inactive")) %>%
    select(-mindate, -maxdate)
  
  # select active gages with precipitation (daily total) and temperature (daily average) data
  temp_county_active_stations <- temp_county_stations_list %>%
    filter(gage_activity == "active") %>%
    select(-gage_activity) %>%
    mutate(station_id = str_sub(id, start = 7)) %>%
    left_join(all_stations, by = c("id", "station_id")) %>%
    filter(element == "PRCP" | element == "TAVG")
  
  # active temperature stations
  temp_tavg_stations <- temp_county_active_stations %>%
    filter(element == "TAVG")
  
  # active precipitation stations
  temp_prcp_stations <- temp_county_active_stations %>%
    filter(element == "PRCP")
  
  # check length of stations
  num_tavg_stations <- dim(temp_tavg_stations)[1]
  num_prcp_stations <- dim(temp_prcp_stations)[1]
  
  # make empty dataframe for tavg data
  temp_tavg_data <- data.frame(fips = character(),
                               id = character(),
                               var_desc = character(),
                               date_text = character(),
                               year_text = character(),
                               value = numeric())
  
  # make empty dataframe for prcp data
  temp_prcp_data <- data.frame(fips = character(),
                               id = character(),
                               var_desc = character(),
                               date_text = character(),
                               year_text = character(),
                               value = numeric())
  
  # if multiple tavg stations then loop through
  if (num_tavg_stations > 0) {
    
    for (j in 1:num_tavg_stations) {
      
      # select temperature data station to download
      temp_sel_tavg_station <- temp_tavg_stations$station_id[j]
      
      # get temperature data
      temp_tavg_data_raw <- ghcnd(temp_sel_tavg_station, var = "TAVG") %>% # will grab all the data from ghcnd
        filter(element == "TAVG") %>%
        select(-element)
      
      # reformat temperature (very untidy IMO) data
      temp_tavg_value_data <- temp_tavg_data_raw %>%
        group_by(id, year, month) %>%
        pivot_longer(cols = starts_with("VALUE"), names_to = "raw_col_name", values_to = "tavg_tenth_degc") %>%
        select(id, year, month, raw_col_name, tavg_tenth_degc) %>%
        ungroup() %>%
        mutate(tavg_degc = tavg_tenth_degc * 0.1,
               year_text = str_trim(as.character(year), side = "both"),
               month_text = str_trim(as.character(month), side = "both"),
               day_text = str_trim(str_sub(raw_col_name, start = 6), side = "both"),
               date = lubridate::ymd(paste0(year_text, "-", month_text, "-", day_text))) %>% 
        filter(!is.na(date)) %>% # warning comes from dates that don't actually exist (i.e., 2/31/2007) so remove with filter
        select(id, date, year_text, tavg_degc)
      
      # check quality codes and remove data that are not good quality
      # quality code descriptions: https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/readme.txt
      temp_tavg_good_quality_dates <- temp_tavg_data_raw %>%
        group_by(id, year, month) %>%
        pivot_longer(cols = starts_with("QFLAG"), names_to = "raw_col_name", values_to = "quality_flag") %>%
        select(id, year, month, raw_col_name, quality_flag) %>%
        ungroup() %>%
        mutate(flag = str_trim(quality_flag, side = "both"),
               year_text = str_trim(as.character(year), side = "both"),
               month_text = str_trim(as.character(month), side = "both"),
               day_text = str_trim(str_sub(raw_col_name, start = 6), side = "both"),
               date = lubridate::ymd(paste0(year_text, "-", month_text, "-", day_text))) %>% 
        filter(!is.na(date)) %>%
        select(date, flag) %>%
        filter(str_length(flag) == 0) %>% # filter out dates where there's a flag (flags indicate issue with data reporting)
        select(date)
      
      # keep temperature data that are good quality
      temp_tavg_good_quality_data <- temp_tavg_value_data %>%
        right_join(temp_tavg_good_quality_dates, by = "date")
      
      # define bounds to check that we have a maximum percentage of data per years
      tavg_perc_annual_data_required <- 90 # X% or more data required to keep data for that year
      temp_tavg_year_check <- temp_tavg_good_quality_data %>%
        count(year_text) %>%
        mutate(percent_complete = (n / 365) * 100) %>%
        filter(percent_complete >= tavg_perc_annual_data_required) %>% # filter out years that are less than 90% complete
        select(year_text)
      
      # delete incomplete years and reformat
      temp_tavg_data_to_append <- temp_tavg_good_quality_data %>%
        filter(year_text %in% temp_tavg_year_check$year_text) %>%
        mutate(fips = temp_county_id_short,
               var_desc = "tavg_degc",
               date_text = as.character(date)) %>%
        select(fips, id, var_desc, date_text, year_text, value = tavg_degc)
      
      # add to noaa temperature data
      temp_tavg_data <- bind_rows(temp_tavg_data, temp_tavg_data_to_append)
      # this will throw a warning but it should be ok!
    }
    
    # calculate annual average temperature
    temp_tavg_annual_data <- temp_tavg_data %>%
      group_by(fips, year_text, var_desc) %>%
      na.omit() %>%
      summarize(value_annual = mean(value, na.rm = TRUE))
    
  } else {
    
    # add empty row
    temp_tavg_data_to_append <- data.frame(fips = temp_county_id_short,
                                           id = NA,
                                           var_desc = "tavg_degc",
                                           date_text = NA,
                                           year_text = NA,
                                           valuel = NA)
    
    # add empty row to to noaa temperature data
    temp_tavg_data <- bind_rows(temp_tavg_data, temp_tavg_data_to_append)
  }
  
  # if multiple tavg stations then loop through
  if (num_prcp_stations > 0) {
    
    for (k in 1:num_prcp_stations) {
      
      # select temperature data station to download
      temp_sel_prcp_station <- temp_prcp_stations$station_id[k]
      
      # get temperature data
      temp_prcp_data_raw <- ghcnd(temp_sel_prcp_station, var = "PRCP") %>% # will grab all the data from ghcnd
        filter(element == "PRCP") %>%
        select(-element)
      
      # reformat temperature (very untidy IMO) data
      temp_prcp_value_data <- temp_prcp_data_raw %>%
        group_by(id, year, month) %>%
        pivot_longer(cols = starts_with("VALUE"), names_to = "raw_col_name", values_to = "precip_tenth_mm") %>%
        select(id, year, month, raw_col_name, precip_tenth_mm) %>%
        ungroup() %>%
        mutate(precip_mm = precip_tenth_mm * 0.1,
               year_text = str_trim(as.character(year), side = "both"),
               month_text = str_trim(as.character(month), side = "both"),
               day_text = str_trim(str_sub(raw_col_name, start = 6), side = "both"),
               date = lubridate::ymd(paste0(year_text, "-", month_text, "-", day_text))) %>% 
        filter(!is.na(date)) %>% # warning comes from dates that don't actually exist (i.e., 2/31/2007) so remove with filter
        select(id, date, year_text, precip_mm)
      
      # check quality codes and remove data that are not good quality
      # quality code descriptions: https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/readme.txt
      temp_prcp_good_quality_dates <- temp_prcp_data_raw %>%
        group_by(id, year, month) %>%
        pivot_longer(cols = starts_with("QFLAG"), names_to = "raw_col_name", values_to = "quality_flag") %>%
        select(id, year, month, raw_col_name, quality_flag) %>%
        ungroup() %>%
        mutate(flag = str_trim(quality_flag, side = "both"),
               year_text = str_trim(as.character(year), side = "both"),
               month_text = str_trim(as.character(month), side = "both"),
               day_text = str_trim(str_sub(raw_col_name, start = 6), side = "both"),
               date = lubridate::ymd(paste0(year_text, "-", month_text, "-", day_text))) %>% 
        filter(!is.na(date)) %>%
        select(date, flag) %>%
        filter(str_length(flag) == 0) %>% # filter out dates where there's a flag (flags indicate issue with data reporting)
        select(date)
      
      # keep temperature data that are good quality
      temp_prcp_good_quality_data <- temp_prcp_value_data %>%
        right_join(temp_prcp_good_quality_dates, by = "date")
      
      # define bounds to check that we have a maximum percentage of data per years
      prcp_perc_annual_data_required <- 90 # X% or more data required to keep data for that year
      temp_prcp_year_check <- temp_prcp_good_quality_data %>%
        count(year_text) %>%
        mutate(percent_complete = (n / 365) * 100) %>%
        filter(percent_complete >= prcp_perc_annual_data_required) %>% # filter out years that are less than 90% complete
        select(year_text)
      
      # delete incomplete years and reformat
      temp_prcp_data_to_append <- temp_prcp_good_quality_data %>%
        filter(year_text %in% temp_prcp_year_check$year_text) %>%
        mutate(fips = temp_county_id_short,
               var_desc = "precip_mm",
               date_text = as.character(date)) %>%
        select(fips, id, var_desc, date_text, year_text, value = precip_mm)
      
      # add to noaa precipitation data
      temp_prcp_data <- bind_rows(temp_prcp_data, temp_prcp_data_to_append)
      # this will throw a warning but it should be ok!
    }
    
    # calucate annual total precipitation
    temp_prcp_annual_data <- temp_prcp_data %>%
      group_by(fips, year_text, var_desc) %>%
      na.omit() %>%
      summarize(value_annual = sum(value, na.rm = TRUE))
    
  } else {
    
    # add empty row
    temp_prcp_data_to_append <- data.frame(fips = temp_county_id_short,
                                           id = NA,
                                           var_desc = "precip_mm",
                                           date_text = NA,
                                           year_text = NA,
                                           value = NA)
    
    # add empty row to to noaa precipitation data
    temp_prcp_data <- bind_rows(temp_prcp_data, temp_prcp_data_to_append)
  }
  
  # only save if there is temperature and precipitation data
  if (num_tavg_stations > 0 & num_prcp_stations > 0) {
    
    # merge annual temperature and precipitation
    temp_merge_data <- bind_rows(temp_tavg_annual_data, temp_prcp_annual_data) %>%
      ungroup() %>%
      pivot_wider(id_cols = c("fips", "year_text"), 
                  names_from = var_desc, 
                  values_from = value_annual) %>%
      mutate(year = as.numeric(year_text)) %>%
      select(fips, year, tavg_annual_degc = tavg_degc, precip_annual_mm = precip_mm) %>%
      arrange(year)
    
    # bind to noaa annual data
    noaa_annual_data <- bind_rows(noaa_annual_data, temp_merge_data)
    
  } else {
    
    # add empty row
    temp_merge_data <- data.frame(fips = temp_county_id_short,
                                  year = NA,
                                  tavg_annual_degc = NA,
                                  precip_annual_mm = NA)
    
    # add empty row to to noaa precipitation data
    noaa_annual_data <- bind_rows(noaa_annual_data, temp_merge_data)
    
  }
}


# TO DOs
# loop through all gages
# data after 1990