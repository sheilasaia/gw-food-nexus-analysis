# reformatting well data

# ---- 1. load libraries ----

library(tidyverse)
library(lubridate)


# ---- 2. read in data ----

# first try for one file then iterate over more than one

# define paths
raw_data_path <- "/Users/ssaia/Dropbox/GW-Food Nexus/tabular_data/raw_data/"

# look for csv files
well_files_list <- list.files(path = raw_data_path, pattern = ".csv")

# ideas:
# need to pull number from file name
# loop through this list and reformat and rbind
# maybe create function to do this all

well_data_raw <- read_delim(file = paste0(raw_data_path, well_files_list[1]),
                          skip = 36,
                          col_names = FALSE, 
                          delim = " ", 
                          col_types = cols(.default = "c"))

well_data <- well_data_raw %>%
  mutate(agency_code = X1,
         station_id = X2,
         parameter_code = X3,
         statistic_code = X4,
         X5_temp = gsub(" ", ",", 
                       gsub("      ", ",", 
                            gsub("[\xca]", " ", X5))),
         data_approval_code = str_sub(gsub("-[\xca]", "", X6), 2, 2)) %>%
  separate(X5_temp,
           c("date_char", "depth_to_water_level_ft_char", "qualification_code"), 
           sep = ",", 
           extra = "drop") %>%
  mutate(date = mdy(date_char), 
         depth_to_water_level_ft = as.numeric(depth_to_water_level_ft_char)) %>%
  select(agency_code:statistic_code, date, depth_to_water_level_ft, data_approval_code)


