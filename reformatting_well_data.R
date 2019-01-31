# reformatting well data

# ---- 1. load libraries ----

library(tidyverse)


# ---- 2. read in data ----

# first try for one file then iterate over more than one

# define paths
raw_data_path <- "/Users/ssaia/Dropbox/GW-Food Nexus/tabular_data/raw_data/"

# look for csv files
raw_well_files_list <- list.files(path = raw_data_path, pattern = ".csv")

# ideas:
# need to pull number from file name
# loop through this list and reformat and rbind
# maybe create function to do this all

well_test_raw <- read_delim(file = paste0(raw_data_path, raw_well_files_list[1]),
                          skip = 36,
                          col_names = FALSE, 
                          delim = " ")

gsub("[\xca]", " ", well_test_raw$X5[1])
