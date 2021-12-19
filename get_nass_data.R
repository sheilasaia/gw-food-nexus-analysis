# ---- script header ----
# script name: get_nass_data.R
# purpose of script: get nass yield data for specific crops and specific counties in US
# author: sheila saia
# date created: 2021-12-01
# email: ssaia@ncsu.edu


# ---- notes ----
# notes:

# quick stats portal: https://quickstats.nass.usda.gov/

# ---- to do ----
# to do list:


# notes from 2021-12-03
# TODO other variables? acres?
# TODO pull out net yield in nass_yield_data
# TODO check on NA values (reason for)
# TODO also pull area planted, area harvested, farm size
# TODO ask Nicholas where to find farm number and average size of farm (not matching with Cornell data if we use AG LAND - ACRES and AG LAND- NUMBER OF OPERATIONS)
# is there documentation for each specific nass db term description?


# Nitin will look for farm size area change over time
# Nitin separate out short_desc a little more for sublevel classifications
# fips 12117, 12011, and 12021 don't have three crops


# ---- load libraries ----
library(tidyverse)
library(rnassqs)
library(here)


# ---- load data ----
# crop code data
crop_code_data <- read_csv(file = here::here("data", "crop_code.csv"), col_names = TRUE) %>%
  select(-pixel_count, -acreage)

# county level key crops of interest (top 3 crops per county)
county_crop_data <- read_csv(file = here::here("data", "county_crops_ranked.csv"), col_names = TRUE)

# check to see that there are top 3 results for each county
length(unique(county_crop_data$fips))
length(county_crop_data$fips)
# 1270 x 3 = 3810 but line 48 = 3807

# which county doesn't have three crops
county_topthree_check <- county_crop_data %>%
  group_by(fips) %>%
  summarize(count = n())
# fips 12117, 12011, and 12021 don't have three crops


# ---- set/load api key ----
# set for first time in R environment
# Sys.setenv(NASS_API_KEY = "") # <-- put your key in quotes here

# get api key after you set it
NASS_API_KEY <- Sys.getenv("NASS_API_KEY")

# assign it to rnassqs function
nassqs_auth(key = NASS_API_KEY)

# get nass params
nass_params <- data.frame(param = nassqs_params())
# descriptions here: https://quickstats.nass.usda.gov/api > click "Usage" on left side to see table


# ---- get nass data ----
# county id list
county_crop_data_tidy <- county_crop_data %>%
  mutate(fips_length = str_count(fips),
         fips_fix = if_else(fips_length == 4, str_pad(string = fips, width = 5, side = "left", pad = "0"), fips)) %>%
  select(- fips) %>%
  mutate(state_fips = str_sub(fips_fix, start = 1, end = 2),
         county_fips = str_sub(fips_fix, start = 3, end = -1),
         mean_freq_round = round(mean_freq, 2)) %>%
  left_join(crop_code_data, by = "lu_code_2008") %>%
  select(fips = fips_fix, state_fips, county_fips, lu_code_2008, mean_freq = mean_freq_round, crop_of_interest, nass_crop_name) %>%
  na.omit()

# empty data frame to hold data
nass_yield_data <- NULL

# short list to start
county_crop_data_tidy_tiny <- county_crop_data_tidy %>%
  arrange(fips) %>%
  slice_head(n = 6)

# for loop
for  (i in 1:dim(county_crop_data_tidy)[1]) {
  # get variables
  temp_commodity_desc <- county_crop_data_tidy$nass_crop_name[i]
  temp_state_ansi <- county_crop_data_tidy$state_fips[i]
  temp_county_ansi <- county_crop_data_tidy$county_fips[i]
  
  # define parameter list for record count query
  temp_yield_params <- list(commodity_desc = temp_commodity_desc,
                              agg_level_desc = "COUNTY",
                              source_desc = "SURVEY",
                              state_ansi = temp_state_ansi,
                              county_ansi = temp_county_ansi,
                              statisticcat_desc = "YIELD")
  
  # get record count
  temp_records_count <- nassqs_record_count(temp_yield_params)$count
  
  # when records are returned, run query
  if (temp_records_count > 0) {
    # get yield data
    temp_yield_raw <- nassqs(temp_yield_params)
    
    # tidy yield data
    temp_yield_tidy <- temp_yield_raw %>%
      mutate(nass_crop_name = temp_commodity_desc) %>%
      select(state_fips = state_fips_code,
             county_fips = county_ansi,
             nass_crop_name,
             class_desc, year, 
             value = Value, 
             unit_desc,
             short_desc,
             location_desc) %>%
      arrange(year, unit_desc)
    
    # append to nass yield data
    nass_yield_data <- bind_rows(temp_yield_tidy, nass_yield_data)
    
    # print
    print(paste0("got yield data for row ", i))
    
  } else {
    # create empty row
    # tidy yield data
    temp_yield_tidy <- data.frame(state_fips = temp_state_ansi,
                                  county_fips = temp_county_ansi,
                                  nass_crop_name = temp_commodity_desc,
                                  class_desc = NA, 
                                  year = NA, 
                                  value = NA, 
                                  unit_desc = NA,
                                  short_desc = NA,
                                  location_desc = NA)
    
    # append to nass yield data
    nass_yield_data <- bind_rows(temp_yield_tidy, nass_yield_data)
    
    # print
    print(paste0("no yield data available for row ", i))
  }
}


# ---- export data ----
write_csv(x = nass_yield_data, file = here::here("data", "nass_yield_data.csv"))



# 
