# retrieving nass data v2

# ---- 1. set up ----

# load libraries
library(tidyverse)
library(here)
library(maps)
library(rnassqs)

# define paths
# when we put the final data on github we'll use relative paths based on the project
# project_path <- here::here()

# force direct paths for now
tabular_data_path <- "/Users/ssaia/Dropbox/GW-Food Nexus/tabular_data/nass_data/"
tabular_data_output_path <- "/Users/ssaia/Dropbox/GW-Food Nexus/tabular_data/nass_data/reformatted_data/"

# load nass key
# nass_key = "<your nass key here>"

# set nass key for rnassqs package
nassqs_auth(key = nass_key)


# ---- 2. load county metadata ----

# state fips and abbreviation lookup
state_ids <- maps::state.fips %>%
  mutate(fips_pad = as.character(str_pad(fips, 2, pad = "0"))) %>%
  select(state_alpha = abb, 
         state_fips = fips_pad) %>%
  filter(state_alpha != "DC") %>%
  distinct()

# load county ids data (reformatted 'AGDominatedCounties.xlsx' file)
county_ids_raw <- read_csv(paste0(tabular_data_path, "ag_dominated_counties_update05082019.csv"))

# define fips codes from reformatted 'AGDominatedCounties.xlsx' file
county_ids <- county_ids_raw %>%
  mutate(state_fips_pad = str_pad(state_fips, 2, pad = "0"), # pad with zeros if not 2 digits
         county_fips_pad = str_pad(county_fips, 3, pad = "0"), #pad with zeros if not 3 digits
         fips = as.character(paste0(state_fips_pad, county_fips_pad))) %>% # create fips key for later dataframe joining
  select(-state_fips, -county_fips) %>%
  select(fips, state_fips = state_fips_pad, county_fips = county_fips_pad, COUNTYNS:county_area_under_ag_percent) %>%
  left_join(state_ids, by = "state_fips") %>%
  arrange(fips)


# ---- 3. get_nass_corn function v2 ----

# list of parameters
# nassqs_params() # just to see what parameters you can define
# look at https://ropensci.github.io/rnassqs/articles/rnassqs.html for more info
# nassqs_params("county_ansi") # to see specific parameter description

gwn_get_nass_data <- function(nass_state_fips, nass_county_fips, nass_crop, nass_crop_full, nass_yield_desc, nass_planted_desc, nass_harvested_desc) {
  # state_fips must be a 2-digit character
  # county_fips must be a 3-digit character
  # crop
  # nass_crop_yield_desc
  # nass_area_planted_desc
  # there's also corn grain (organic) and silage (organic and non-organic) values we could add here

  # define corn yield parameters
  yield_params <- list(commodity_desc = nass_crop,
                       short_desc = nass_yield_short_desc,
                       agg_level_desc = "COUNTY",
                       source_desc = "SURVEY",
                       #year__GE = "2000", # years greater than 2000
                       state_ansi = nass_state_fips, # 2-digit state fips 00 - 99
                       county_ansi = nass_county_fips) # 3-digit county code 000 - 999
  
  # define area planted parameters
  planted_params <- list(commodity_desc = nass_crop,
                         short_desc = nass_planted_short_desc,
                         agg_level_desc = "COUNTY",
                         source_desc = "SURVEY",
                         #year__GE = "2000", # years greater than 2000
                         state_ansi = nass_state_fips, # 2-digit state fips 00 - 99
                         county_ansi = nass_county_fips) # 3-digit county code 000 - 999
  
  # define area harvested parameters
  harvested_params <- list(commodity_desc = nass_crop,
                           short_desc = nass_harvested_short_desc,
                           domain_desc = "TOTAL",
                           agg_level_desc = "COUNTY",
                           source_desc = "SURVEY",
                           #year__GE = "2000", # years greater than 2000
                           state_ansi = nass_state_fips, # 2-digit state fips 00 - 99
                           county_ansi = nass_county_fips) # 3-digit county code 000 - 999
  
  # define net income parameters
  net_income_params <- list(short_desc = "INCOME, NET CASH FARM, OF OPERATIONS - NET INCOME, MEASURED IN $",
                            domain_desc = "TOTAL",
                            agg_level_desc = "COUNTY",
                            #year__GE = "2000", # years greater than 2000
                            state_ansi = nass_state_fips, # 2-digit state fips 00 - 99
                            county_ansi = nass_county_fips) # 3-digit county code 000 - 999
  
  # define income parameters
  income_params <- list(short_desc = "INCOME, FARM-RELATED - RECEIPTS, MEASURED IN $",
                        domain_desc = "TOTAL",
                        agg_level_desc = "COUNTY",
                        #year__GE = "2000", # years greater than 2000
                        state_ansi = nass_state_fips, # 2-digit state fips 00 - 99
                        county_ansi = nass_county_fips) # 3-digit county code 000 - 999
  
  # download data from each using parameters
  yield_data_raw <- nassqs(yield_params)
  planted_data_raw <- nassqs(planted_params)
  harvested_data_raw <- nassqs(harvested_params)
  net_income_data_raw <- nassqs(net_income_params)
  income_data_raw <- nassqs(income_params)
  
  # record record numbers
  dim_yield_data <- dim(yield_data_raw)[1]
  dim_planted_data <- dim(planted_data_raw)[1]
  dim_harvested_data <- dim(harvested_data_raw)[1]
  dim_net_income_data <- dim(net_income_data_raw)[1]
  dim_income_data <- dim(income_data_raw)[1]
  
  # check for length!
  if (dim_yield_data > 0 & dim_planted_data > 0 & dim_harvested_data > 0 & dim_net_income_data > 0 & dim_income_data > 0) {
    
    # reformat yield data
    yield_data <- yield_data_raw %>%
      select(year, state_fips = state_ansi, county_fips = county_ansi, short_desc, Value) %>%
      mutate(fips = paste0(state_fips, county_fips),
             fips_year = paste0(fips, "_", year),
             nass_desc = str_to_lower(str_replace_all(str_replace_all(str_replace_all(str_remove_all(str_remove_all(short_desc, ","), "-"), "  ", " "), "/", "PER"), " ", "_")),
             value_annual = str_trim(Value)) %>%
      filter(value_annual != "(D)" & value_annual != "(Z)") %>% # remove rows without data
      mutate(value_annual = as.numeric(str_remove_all(value_annual, ",")),
             nass_desc = recode(nass_desc, "corn_grain_yield_measured_in_bu_per_acre" = "yeild_bu_per_acre")) %>%
      select(fips_year, value_annual, nass_desc)
    
    # reformat area planted data
    planted_data <- planted_data_raw %>%
      select(year, state_fips = state_ansi, county_fips = county_ansi, short_desc, Value) %>%
      mutate(fips = paste0(state_fips, county_fips),
             fips_year = paste0(fips, "_", year),
             nass_desc = str_to_lower(str_replace_all(str_replace_all(str_replace_all(str_remove_all(str_remove_all(short_desc, ","), "-"), "  ", " "), "/", "PER"), " ", "_")),
             value_annual = str_trim(Value)) %>%
      filter(value_annual != "(D)" & value_annual != "(Z)") %>% # remove rows without data
      mutate(value_annual = as.numeric(str_remove_all(value_annual, ",")),
             nass_desc = recode(nass_desc, "corn_acres_planted" = "area_planted_acres")) %>%
      select(fips_year, value_annual, nass_desc)
    
    # reformat area harvested data
    harvested_data <- harvested_data_raw %>%
      select(year, state_fips = state_ansi, county_fips = county_ansi, short_desc, Value) %>%
      mutate(fips = paste0(state_fips, county_fips),
             fips_year = paste0(fips, "_", year),
             nass_desc = str_to_lower(str_replace_all(str_replace_all(str_replace_all(str_remove_all(str_remove_all(short_desc, ","), "-"), "  ", " "), "/", "PER"), " ", "_")),
             value_annual = str_trim(Value)) %>%
      filter(value_annual != "(D)" & value_annual != "(Z)") %>% # remove rows without data
      mutate(value_annual = as.numeric(str_remove_all(value_annual, ",")),
             nass_desc = recode(nass_desc, "corn_grain_acres_harvested" = "area_harvested_acres")) %>%
      select(fips_year, value_annual, nass_desc)
    
    # reformat net income data
    net_income_data <- net_income_data_raw %>%
      select(year, state_fips = state_ansi, county_fips = county_ansi, short_desc, Value) %>%
      mutate(fips = paste0(state_fips, county_fips),
             fips_year = paste0(fips, "_", year),
             nass_desc = str_to_lower(str_replace_all(str_replace_all(str_replace_all(str_remove_all(str_remove_all(short_desc, ","), "-"), "  ", " "), "/", "PER"), " ", "_")),
             value_annual = str_trim(Value)) %>%
      filter(value_annual != "(D)" & value_annual != "(Z)") %>% # remove rows without data
      mutate(value_annual = as.numeric(str_remove_all(value_annual, ",")),
             nass_desc = recode(nass_desc, "income_net_cash_farm_of_operations_net_income_measured_in_$" = "net_income_farm_ops_usd")) %>%
      select(fips_year, value_annual, nass_desc)
    
    # reformat income data
    income_data <- income_data_raw %>%
      select(year, state_fips = state_ansi, county_fips = county_ansi, short_desc, Value) %>%
      mutate(fips = paste0(state_fips, county_fips),
             fips_year = paste0(fips, "_", year),
             nass_desc = str_to_lower(str_replace_all(str_replace_all(str_replace_all(str_remove_all(str_remove_all(short_desc, ","), "-"), "  ", " "), "/", "PER"), " ", "_")),
             value_annual = str_trim(Value)) %>%
      filter(value_annual != "(D)" & value_annual != "(Z)") %>% # remove rows without data
      mutate(value_annual = as.numeric(str_remove_all(value_annual, ",")),
             nass_desc = recode(nass_desc, "income_farmrelated_receipts_measured_in_$" = "income_farm_receipts_usd")) %>%
      select(fips_year, value_annual, nass_desc)
    
    # combine yield, area planted, net income, and income receipts data
    merge_data <- rbind(yield_data, planted_data, harvested_data, net_income_data, income_data) %>% # NOTE! all dataframes have to have the same columns & column order!
      group_by(fips_year) %>%
      spread(key = nass_desc, value = value_annual) %>% # spread data to format as requested
      mutate(fips = str_sub(fips_year, 1, 5),
             year = as.numeric(str_sub(fips_year, 7, 10)), # break out fips and year
             crop_type = nass_crop_full) %>%
      ungroup(fips_year) %>% # ungroup to prevent errors later
      select(fips, year, crop_type, yeild_bu_per_acre, area_planted_acres, area_harvested_acres, net_income_farm_ops_usd, income_farm_receipts_usd) %>% # select only necessary columns
      mutate(yield_bu_per_sqkm = yeild_bu_per_acre * 247.105, # change from bu per acres to bu per sqkm)
             area_planted_sqkm = area_planted_acres * (1/247.105), # change from acres to sqkm)
             area_harvested_sqkm = area_harvested_acres * (1/247.105)) %>% # change from acres to sqkm)
      select(year, fips, yield_bu_per_sqkm, area_planted_sqkm, area_harvested_sqkm, net_income_farm_ops_usd, income_farm_receipts_usd, crop_type) %>%
      filter(year > 1996) %>%
      arrange(year) %>% na.omit()
    
    # return merged data
    return(merge_data)
    
  }

  else {
    
    # if data doesn't exist, then return empty data frame
    merge_data <- data.frame()
    
    # return empty merged data
    return(merge_data)
    
  }
}


# ---- 4. using get_nass_data function ----

# loop
for (i in 1:10) { #dim(county_ids)[1]) {
  
  # define a state and county
  temp_state_fips <- county_ids$state_fips[i]
  temp_county_fips <- county_ids$county_fips[i]
  
  # define other variables
  my_crop = "CORN" # from nass
  my_crop_full = "corn_grain"
  my_yield_desc = "CORN, GRAIN - YIELD, MEASURED IN BU / ACRE" # from nass, depends on crop
  my_planted_desc = "CORN - ACRES PLANTED" # from nass, depends on crop
  my_harvested_desc = "CORN, GRAIN - ACRES HARVESTED"  # from nass, depends on crop
  
  # get data
  temp_data <- gwn_get_nass_data(nass_state_fips = temp_state_fips, 
                                 nass_county_fips = temp_county_fips,
                                 nass_crop = my_crop,
                                 nass_crop_full = my_crop_full,
                                 nass_yield_desc = my_yield_desc,
                                 nass_planted_desc = my_planted_desc,
                                 nass_harvested_desc = my_harvested_desc)
  
  # only export if there's data
  if (dim(temp_data)[1] > 1) {
    
    # need to join
    temp_data_join <- temp_data %>%
      left_join(county_ids, by = "fips") %>%
      select(year, fips, state_alpha, state_fips, county_fips:county_area_under_ag_percent, 
             yield_bu_per_sqkm:income_farm_receipts_usd, crop_type)
    
    # export data
    write_csv(temp_data, paste0(tabular_data_output_path, str_to_lower(my_crop), "_", county_ids$state_alpha[i], "_", my_fips_list$fips[i],".csv"))
  }
  
  else {
    write_csv(temp_data, paste0(tabular_data_output_path, str_to_lower(my_crop), "_", county_ids$state_alpha[i], "_", my_fips_list$fips[i],"_nodata.csv"))
  }
}


# ---- 5. combine data ----

# read files in folder
nass_files <- list.files(path = tabular_data_output_path)
nass_files_sel <- grep("nodata", nass_files)
nass_file_paths <- paste0(tabular_data_output_path, nass_files)

