# retrieving nass data

# load libraries
library(tidyverse)
library(httr)
library(jsonlite)
library(tidycensus)
library(purrr)

# define paths
project_path <- here::here()
tabular_data_path <- "/Users/ssaia/Dropbox/GW-Food Nexus/tabular_data/nass_data/"
tabular_data_output_path <- "/Users/ssaia/Dropbox/GW-Food Nexus/tabular_data/nass_data/"

# load county ids data (reformatted 'AGDominatedCounties.xlsx' file)
county_ids_raw <- read.csv(paste0(tabular_data_path, "ag_dominated_counties_update05082019.csv"))

# define fips codes from reformatted 'AGDominatedCounties.xlsx' file
county_ids <- county_ids_raw %>%
  mutate(state_fips_pad = str_pad(state_fips, 2, pad = "0"),
         county_fips_pad = str_pad(county_fips, 3, pad = "0"),
         fips = as.character(paste0(state_fips_pad, county_fips_pad))) %>%
  select(fips, county_area_sqkm:county_area_under_ag_percent)

# nass key
# need to call your nass key into Sys.Env memory
# nass_key <- "<your number here>"

# nass url
nass_url <- "http://quickstats.nass.usda.gov"

# commodity description of interest
my_commodity_desc <- "CORN"

# query start year
# my_year <- "2017"

# state of interest
my_state <- "LA"

# final path string
# path_corn <- paste0("api/api_GET/?key=", nass_key, "&commodity_desc=", my_commodity_desc, "&year__GE=", my_year, "&state_alpha=", my_state) # for specific year
api_path <- paste0("api/api_GET/?key=", nass_key, "&commodity_desc=", my_commodity_desc, "&state_alpha=", my_state) # for all years

# get raw data
raw_data <- GET(url = nass_url, path = api_path)

# check status of api query
raw_data$status_code
# 200 is good! no issues.

# convert to character
char_raw_data <- rawToChar(raw_data$content)

# make into a list
list_raw_data <- fromJSON(char_raw_data)

# map to data frame
raw_data <- pmap_dfr(list_raw_data, rbind)

# clean up data
data <- raw_data %>%
  filter(agg_level_desc == "COUNTY") %>% # only want countly level data
  filter(prodn_practice_desc == "ALL PRODUCTION PRACTICES") %>%
  mutate(fips = paste0(state_fips_code, county_code),
         county_name_full = str_to_title(county_name),
         region = str_to_title(asd_desc),
         nass_description = str_to_lower(str_replace_all(str_replace_all(str_replace_all(str_remove_all(str_remove_all(short_desc, ","), "-"), "  ", " "), "/", "PER"), " ", "_")),
         statistic_cat_desc = str_remove_all(str_replace_all(str_to_lower(statisticcat_desc), " ", "_"), ","),
         units = str_replace_all(str_replace(str_to_lower(unit_desc), "/", "per"), " ", "_")) %>%
  mutate(value_annual = str_trim(Value)) %>% # trim white-space
  select(fips, state_alpha, county_name_full, region, year, value_annual, statistic_cat_desc, units, nass_description) %>%
  filter(value_annual != "(D)" & value_annual != "(Z)") %>% # remove rows without data
  mutate(value_annual = as.numeric(str_remove_all(value_annual, ","))) %>%
  filter(county_name_full != "Other (Combined) Counties") # remove un-named counties

# county info
county_data <- data %>%
  select(fips:region) %>%
  distinct() %>%
  left_join(county_ids, by = "fips")

# select and reformat yield data
yield_data <- data %>%
  filter(statistic_cat_desc == "yield" & nass_description == "corn_grain_yield_measured_in_bu_per_acre") %>%
  mutate(cat_units = paste0(statistic_cat_desc, "_", units),
         fips_year = paste0(fips, "_", year)) %>%
  select(fips_year, nass_description, value_annual, cat_units)

# select and reformat area planted data
area_planted_data <- data %>%
  filter(statistic_cat_desc == "area_planted") %>%
  mutate(cat_units = paste0(statistic_cat_desc, "_", units),
         fips_year = paste0(fips, "_", year)) %>%
  select(fips_year, nass_description, value_annual, cat_units)

# combine yield and area planted
merge_data <- rbind(yield_data, area_planted_data) %>%
  select(-nass_description) %>%
  # mutate(row = seq(1, dim(rbind(yield_data, area_planted_data))[1], 1)) %>%
  # mutate(fips_year = paste0(fips, "_", year)) %>%
  # select(fips, year, value_annual, cat_units) %>%
  group_by(fips_year) %>%
  spread(key = cat_units, value = value_annual) %>%
  mutate(fips = str_sub(fips_year, 1, 5),
         year = as.numeric(str_sub(fips_year, 7, 10))) %>%
  ungroup(fips_year) %>%
  select(fips, year, yield_bu_per_acre, area_planted_acres) %>%
  left_join(county_data, by = "fips") %>%
  mutate(yield_bu_per_sqkm = yield_bu_per_acre * 247.105,
         area_planted_sqkm = area_planted_acres * (1/247.105)) %>%
  select(year, state_alpha, county_name_full, fips, region,
         ag_area_sqkm, county_area_sqkm, county_area_under_ag_percent,
         yield_bu_per_sqkm, area_planted_sqkm) %>%
  na.omit()

# export merged data
write_csv(merge_data, paste0(tabular_data_output_path, "la_nass_corn_data.csv"))

#  extra code
# select and reformat area harvested data
corn_area_harvested_data <- corn_data %>%
  filter(statisticcat_desc_full == "area_harvested") %>%
  filter(unit_desc_full == "acres") %>% # don't want # operations
  mutate(area_harvested_acres = value_annual) %>% # consolidate
  select(fips:year, area_harvested_acres)

# other potential datasets (corn sales and production)
corn_sales_data <- corn_data %>%
  filter(statisticcat_desc_full == "sales") %>%
  filter(unit_desc_full == "$")

corn_production_data <- corn_data %>%
  filter(statisticcat_desc_full == "production")


# to do:
# for expense data need to make another r script