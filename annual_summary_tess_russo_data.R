# reformatting tess russo data

# ---- 1. set up ----

# load libraries
library(tidyverse)
library(here)

# define paths
# when we put the final data on github we'll use relative paths based on the project
# project_path <- here::here()

# force direct paths for now (need to figure out data storage structure so county data is in same directory)
county_data_path <- "/Users/ssaia/Dropbox/GW-Food Nexus/tabular_data/nass_data/" # here::here("tabular_data", "nass_data")
tabular_data_path <- "/Users/ssaia/Dropbox/GW-Food Nexus/tabular_data/tess_russo_data/us_gw_precip/" # here::here("tabular_data", "tess_russo_data", "us_gw_precip")
tabular_data_output_path <- "/Users/ssaia/Dropbox/GW-Food Nexus/tabular_data/tess_russo_data/us_gw_precip/reformatted_data/" # here::here("tabular_data", "tess_russo_data", "reformatted_data")


# ---- 2. load county metadata ----

# load county ids data (reformatted 'AGDominatedCounties.xlsx' file)
county_ids_raw <- read_csv(paste0(county_data_path, "ag_dominated_counties_update05082019.csv"))

# define fips codes from reformatted 'AGDominatedCounties.xlsx' file
county_ids <- county_ids_raw %>%
  mutate(state_fips_pad = str_pad(state_fips, 2, pad = "0"), # pad with zeros if not 2 digits
         county_fips_pad = str_pad(county_fips, 3, pad = "0"), #pad with zeros if not 3 digits
         fips = as.character(paste0(state_fips_pad, county_fips_pad))) %>% # create fips key for later dataframe joining
  select(fips, county_area_sqkm:county_area_under_ag_percent) # select only needed columns


# ---- 3. load tess russo data ----

tess_russo_data_raw <- read_csv(paste0(tabular_data_path, "W.tmp.csv"), col_names = FALSE)
tess_russo_metadata_raw <- read_csv(paste0(tabular_data_path, "meta.tmp.csv")) #16475 sites (row), 13 variables (column)
