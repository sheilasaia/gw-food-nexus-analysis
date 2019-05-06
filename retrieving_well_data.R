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
well_id_list_short <- as.vector(well_id_list[1:5])

# metadata dataframe
metadata <- tibble(
  station_id = character(),
  number_observations = integer(),
  start_date = character(),
  end_date = character()
)

# for loop
for(i in 1:length(well_id_list_short)) {
  
  # select site
  my_site_number <- well_id_list_short[i]
  
  # get raw groundwater data
  data_raw <- readNWISgwl(siteNumbers = my_site_number,
                          startDate = "",
                          endDate = "",
                          convertType = FALSE)
  
  # if there is some data to download (i.e., if dim(data_raw))[1] > 0)
  if (dim(data_raw)[1] > 0) {
    data <- data_raw %>%
      mutate(station_id = site_no,
             date = lev_dt,
             date_tz = lev_tz_cd,
             depth_to_water_ft_below_surf = lev_va,
             approval_code = lev_status_cd) %>%
      select(station_id:approval_code)
    
    # save to metadata
    new_metadata <- tibble(station_id = as.character(my_site_number),
                           number_observations = dim(data)[1],
                           start_date = as.character(data$date[1]),
                           end_date = as.character(data$date[dim(data)[1]]))
    metadata <- bind_rows(metadata, new_metadata)
                        
    
    write_csv(data, paste0(tabular_data_output_path, "daily_gw_", my_site_number, ".csv"))
  }
  
  # if there's no data to download
  else {
    # save to metadata
    new_metadata <- tibble(station_id = my_site_number,
                           number_observations = 0,
                           start_date = NA,
                           end_date = NA)
    
    metadata <- bind_rows(metadata, new_metadata)
  }
}

# add other site specific metadata
metadata_full <- left_join(metadata, well_ids, by = "station_id")

# write metadata to csv
write_csv(metadata_full, paste0(tabular_data_output_path, "daily_gw_metadata.csv"))

