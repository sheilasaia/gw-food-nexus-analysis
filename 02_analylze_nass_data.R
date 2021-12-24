# ---- script header ----
# script name: 02_analyze_nass_data.R
# purpose of script: wrangle nass data and do analyses
# author: sheila saia
# author email: ssaia@ncsu.edu
# date created: 2021-12-23


# ---- notes ----
# notes:


# ---- to do ----
# to do list:


# ---- load libraries ----
library(tidyverse)
library(here)


# ---- load data ----
# load data
nass_ag_land_data <- read_csv(file = here::here("data", "nass_ag_land_data.csv"), col_names = TRUE)

# filter out only data we need
nass_ag_land_data_sel <- nass_ag_land_data %>%
  filter((domain_desc == "AREA OPERATED")) %>%
  mutate(fips = paste0(state_fips, county_fips)) %>%
  select(fips, year:unit_desc, domaincat_desc)
  
# area operated domain categories key
domaincat_desc_key <- data.frame(domaincat_desc = unique(nass_ag_land_data_sel$domaincat_desc),
                                 op_size_category_in_acres = c("1000-1999", "1-9.9", "10-49.9",
                                                               "100-139", "140-179", "180-219",
                                                               "2000+", "220-259", "260-499",
                                                               "50-69.9", "500-999", "70-99.9",
                                                               "1000+", "180-499", "50-179"))

# select out operations
nass_operations_data <- nass_ag_land_data_sel %>%
  filter(unit_desc == "OPERATIONS") %>%
  left_join(domaincat_desc_key, by = "domaincat_desc") %>%
  select(fips, year, op_size_category_in_acres, num_ops = value)

# select out acres
nass_acres_data <- nass_ag_land_data_sel %>%
  filter(unit_desc == "ACRES") %>%
  left_join(domaincat_desc_key, by = "domaincat_desc") %>%
  select(fips, year, op_size_category_in_acres, acres = value)

# join data
nass_avg_area_op_data <- nass_acres_data %>%
  left_join(nass_operations_data, by = c("fips", "year", "op_size_category_in_acres")) %>%
  mutate(avg_op_size_ac = round(acres/num_ops, 2)) %>%
  arrange(fips, year) %>%
  mutate(op_size_category_in_acres = fct_relevel(as.factor(op_size_category_in_acres),
                                                 "1-9.9", "10-49.9", "50-69.9", "70-99.9", "100-139", "140-179", 
                                                 "180-219", "220-259", "260-499", "500-999", "1000-1999", "2000+"))

# plot to check
ggplot(data = nass_avg_area_op_data) +
  geom_boxplot(mapping = aes(x = as.factor(year), y = avg_op_size_ac, fill = op_size_category_in_acres)) +
  facet_wrap(~ op_size_category_in_acres, scales = "free_y") +
  labs(x = "Year", y = "Average County Opperation Size (ac)", fill = "Op. Size Category (ac)")


# ---- export data ----
write_csv(x = nass_avg_area_op_data, file = here::here("data", "nass_avg_area_op_data.csv"))


