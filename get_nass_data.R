# ---- script header ----
# script name: get_nass_data.R
# purpose of script: get nass yield data for specific crops and specific counties in US
# author: sheila saia
# date created: 2021-12-01
# email: ssaia@ncsu.edu


# ---- notes ----
# notes:


# ---- to do ----
# to do list:

# TODO ask Nitin where pixel count and acres come from in crop_code_data file
# TODO ask Nitin about irrigated vs non-irrigated

# what is the difference between "yield" alone and "yield of entire crop"?


# ---- load libraries ----
library(tidyverse)
library(rnassqs)
library(here)



# ---- load data ----
# crop code data
crop_code_data <- read_csv(file = here::here("data", "crop_code.csv"), col_names = TRUE)

# county level key crops
county_crop_data <- read_csv(file = here::here("data", "county_level_keycrop.csv"), col_names = TRUE)

# check to see if there are multiple counties
length(unique(county_crop_data$fips))
length(county_crop_data$fips)
# each is unique


# ---- set/load api key ----
# set for first time in R environment
# Sys.setenv(NASS_API_KEY = "") # <-- put your key in quotes here

# get api key after you set it
NASS_API_KEY <- Sys.getenv("NASS_API_KEY")

# assign it to rnassqs function
nassqs_auth(key = NASS_API_KEY)

# get nass params
nass_params <- nassqs_params()
# descriptions here: https://quickstats.nass.usda.gov/api > click "Usage" on left side to see table


# ---- get nass data ----
# county id list
county_crop_data_tidy <- county_crop_data %>%
  mutate(state_fips = str_sub(fips, start = 1, end = 2),
         county_fips = str_sub(fips, start = 3, end = -1),
         mean_pixel_count_round = round(mean_pixel_count, 2)) %>%
  left_join(crop_code_data, by = "lu_code_2008") %>%
  select(fips, state_fips, county_fips, mean_pixel_count_round, crop_of_interest, nass_crop_name)

# empty data frame to hold data
nass_yield_data <- NULL

# short list to start
# county_crop_data_tidy_tiny <- county_crop_data_tidy %>%
#   arrange(fips) %>%
#   slice_head(n = 5)

# for loop
for  (i in 704:dim(county_crop_data_tidy)[1]) {
  # get variables
  temp_commodity_desc <- county_crop_data_tidy$nass_crop_name[i]
  temp_state_ansi <- county_crop_data_tidy$state_fips[i]
  temp_county_ansi <- county_crop_data_tidy$county_fips[i]
  
  # define parameter list for record count query
  temp_records_params <- list(commodity_desc = temp_commodity_desc,
                              agg_level_desc = "COUNTY",
                              source_desc = "SURVEY",
                              state_ansi = temp_state_ansi,
                              county_ansi = temp_county_ansi,
                              statisticcat_desc = "YIELD")
  
  # get record count
  temp_records_count <- nassqs_record_count(temp_records_params)$count
  
  # when records are returned, run query
  if (temp_records_count > 0) {
    # define parameter list for yield query
    temp_yield_params <- list(commodity_desc = temp_commodity_desc,
                              agg_level_desc = "COUNTY",
                              source_desc = "SURVEY",
                              state_ansi = temp_state_ansi,
                              county_ansi = temp_county_ansi)
    
    # yet yield data
    temp_yield_raw <- nassqs_yields(temp_yield_params)
    
    # tidy yield data
    temp_yield_tidy <- temp_yield_raw %>%
      mutate(nass_crop_name = temp_commodity_desc) %>%
      select(state_fips = state_fips_code,
             county_fips = county_ansi,
             nass_crop_name,
             class_desc, year, 
             value = Value, 
             unit_desc,
             short_desc)
    
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
                                  short_desc = NA)
    
    # append to nass yield data
    nass_yield_data <- bind_rows(temp_yield_tidy, nass_yield_data)
    
    # print
    print(paste0("no yield data available for row ", i))
  }
}


# ---- export data ----
write_csv(x = nass_yield_data, file = here::here("data", "nass_yield_data.csv"))
