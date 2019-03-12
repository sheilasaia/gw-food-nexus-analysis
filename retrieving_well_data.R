# retrieving usgs well data

# load libraries
library(sf)
library(tidyverse)
library(lubridate)
library(here)
library(dataRetrieval)

# define paths
project_path <- here::here()
gis_data_path <- "/Users/ssaia/Dropbox/GW-Food Nexus/gis_data/raw_data/"
tabular_data_output_path <- "/Users/ssaia/Dropbox/GW-Food Nexus/tabular_data/reformatted_well_data/"

# load data
well_locations_shp <- st_read(paste0(gis_data_path, "50List_Wells/50list_wells.shp"))

# save well id's
well_ids <- st_set_geometry(well_locations_shp, NULL) %>%
  mutate(station_id = SITEID,
         lat = DECLAT,
         long = DECLON,
         state = STATE,
         state_name = STATE_NM,
         county_name = COUNTY_NM,
         state_fips = STATE_CD,
         county_fips = COUNTY) %>%
  select(station_id:county_fips)

# export well id table
# write_csv(well_ids,paste0(tabular_data_output_path, "gw_well_metadata.csv"))

well_id_list <- well_ids$station_id
well_id_list_short <- as.vector(well_id_list[1:10])

my_site_number <- "401444093442001"
my_statistic_code <- "00003" # mean
my_parameter_code <- "72019" # depth to water table level (ft below land surface)



test <- readNWISdv(siteNumbers = my_site_number,
                   parameterCd = my_parameter_code,
                   statCd = my_statistic_code,
                   startDate = "",
                   endDate = "") %>%
  mutate(station_id = site_no,
         date = as_date(Date),
         depth_to_water_level_ft = X_72019_00003,
         approval_code = X_72019_00003_cd) %>%
  select(station_id:approval_code)

write_csv(test,paste0(tabular_data_output_path, "daily_gw_", my_site_number, ".csv"))
