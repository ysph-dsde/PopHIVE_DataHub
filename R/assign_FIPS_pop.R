suppressPackageStartupMessages({
  library("arrow")
  library("parquetize")
  library("readxl")
  library("assertthat")
  library("tidyverse")
  library("MMWRweek")
  library("lubridate")
  library("RSocrata")
  library("usmap")
  library("stringr")
  library("ggplot2")
  library("usdata")
  library("sf")
})



############
# Overview
############

# This script reads in Cosmos ED data files (diabetes & obesity, age-stratified) at county level and state level 
# and assign FIPS code (for counties) and population (for states and counties)
# Percent population captured by cosmos pct_captured is calculated using N_patients / pop * 100


#############################################################
# read in and stack county level data on diabetes and obesity 
#############################################################
# ------ a ------
temp <- read_csv("Data/other_data/diabetes_obesity/2025_05_01_DM_and_Obesity_Pts_PREV_County_Age_C02a.csv", skip=12, col_names=T) %>%
  dplyr::rename(age=`Age in Years (Current)`, county = `County of Residence`,
                N_patients = ...3, pct_diabetes = ...4, pct_obesity = ...5)
row_index <- which(!is.na(temp$age))
# temp[row_index,]

temp$age[(row_index[1] + 1):(row_index[2] - 1)] <- "Less than 10 years"
temp$age[(row_index[2] + 1):(row_index[3] - 1)] <- "≥ 10 and < 15 years"
temp$age[(row_index[3] + 1):(row_index[4] - 1)] <- "≥ 15 and < 20 years"
temp$age[(row_index[4] + 1):(row_index[5] - 1)] <- "≥ 20 and < 40 years"
temp$age[(row_index[5] + 1):(row_index[6] - 1)] <- "≥ 40 and < 65 years"
temp$age[(row_index[6] + 1):(row_index[7] - 1)] <- "65 years or more"
temp$age[(row_index[7] + 1):(row_index[8] - 1)] <- "No value"
temp$age[(row_index[8] + 1):(nrow(temp))] <- "Total"

diabetes_obesity_county <- temp

# ------ b ------
temp <- read_csv("Data/other_data/diabetes_obesity/2025_05_01_DM_and_Obesity_Pts_PREV_County_Age_C02b.csv", skip=12, col_names=T) %>%
  dplyr::rename(age=`Age in Years (Current)`, county = `County of Residence`,
                N_patients = ...3, pct_diabetes = ...4, pct_obesity = ...5)
row_index <- which(!is.na(temp$age))
# temp[row_index,]

temp$age[(row_index[1] + 1):(row_index[2] - 1)] <- "Less than 10 years"
temp$age[(row_index[2] + 1):(row_index[3] - 1)] <- "≥ 10 and < 15 years"
temp$age[(row_index[3] + 1):(row_index[4] - 1)] <- "≥ 15 and < 20 years"
temp$age[(row_index[4] + 1):(row_index[5] - 1)] <- "≥ 20 and < 40 years"
temp$age[(row_index[5] + 1):(row_index[6] - 1)] <- "≥ 40 and < 65 years"
temp$age[(row_index[6] + 1):(row_index[7] - 1)] <- "65 years or more"
temp$age[(row_index[7] + 1):(row_index[8] - 1)] <- "No value"
temp$age[(row_index[8] + 1):(nrow(temp))] <- "Total"

diabetes_obesity_county <- bind_rows(diabetes_obesity_county, temp)


# ------ c ------
temp <- read_csv("Data/other_data/diabetes_obesity/2025_05_01_DM_and_Obesity_Pts_PREV_County_Age_C02c.csv", skip=12, col_names=T) %>%
  dplyr::rename(age=`Age in Years (Current)`, county = `County of Residence`,
                N_patients = ...3, pct_diabetes = ...4, pct_obesity = ...5)
row_index <- which(!is.na(temp$age))
# temp[row_index,]

temp$age[(row_index[1] + 1):(row_index[2] - 1)] <- "Less than 10 years"
temp$age[(row_index[2] + 1):(row_index[3] - 1)] <- "≥ 10 and < 15 years"
temp$age[(row_index[3] + 1):(row_index[4] - 1)] <- "≥ 15 and < 20 years"
temp$age[(row_index[4] + 1):(row_index[5] - 1)] <- "≥ 20 and < 40 years"
temp$age[(row_index[5] + 1):(row_index[6] - 1)] <- "≥ 40 and < 65 years"
temp$age[(row_index[6] + 1):(row_index[7] - 1)] <- "65 years or more"
temp$age[(row_index[7] + 1):(row_index[8] - 1)] <- "No value"
temp$age[(row_index[8] + 1):(nrow(temp))] <- "Total"

diabetes_obesity_county <- bind_rows(diabetes_obesity_county, temp)


# ------ d ------
temp <- read_csv("Data/other_data/diabetes_obesity/2025_05_01_DM_and_Obesity_Pts_PREV_County_Age_C02d.csv", skip=12, col_names=T) %>%
  dplyr::rename(age=`Age in Years (Current)`, county = `County of Residence`,
                N_patients = ...3, pct_diabetes = ...4, pct_obesity = ...5)
row_index <- which(!is.na(temp$age))
# temp[row_index,]

temp$age[(row_index[1] + 1):(row_index[2] - 1)] <- "Less than 10 years"
temp$age[(row_index[2] + 1):(row_index[3] - 1)] <- "≥ 10 and < 15 years"
temp$age[(row_index[3] + 1):(row_index[4] - 1)] <- "≥ 15 and < 20 years"
temp$age[(row_index[4] + 1):(row_index[5] - 1)] <- "≥ 20 and < 40 years"
temp$age[(row_index[5] + 1):(row_index[6] - 1)] <- "≥ 40 and < 65 years"
temp$age[(row_index[6] + 1):(row_index[7] - 1)] <- "65 years or more"
temp$age[(row_index[7] + 1):(row_index[8] - 1)] <- "No value"
temp$age[(row_index[8] + 1):(nrow(temp))] <- "Total"

diabetes_obesity_county <- bind_rows(diabetes_obesity_county, temp)

# remove "total" and "none of above" in county column
diabetes_obesity_county <- diabetes_obesity_county %>% filter(!county %in% c("None of the above", "Total"))


################################################
# match county level data with pop and FIPS code 
################################################

#---------------------------------------------------------------------
# read in  population data by county and by age (2021) from census.gov 
#---------------------------------------------------------------------

# I used 2021 census as data from 2022 doesn't have pop data for the following 8 counties in CT: HARTFORD, NEW HAVEN, FAIRFIELD, NEW LONDON, MIDDLESEX, LITCHFIELD, TOLLAND, WINDHAM 
pop_county_age <- read.csv("Data/other_data/diabetes_obesity/population_county_age_censusgov_2021.csv", skip = 1) %>% 
  dplyr::select(c(1,2,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39)) %>%
  pivot_longer(cols = 3:21, names_to = "agegroup", values_to = "pop_2021", names_prefix = "Estimate..Total..Total.population") %>% 
  mutate(age = case_when(agegroup %in% c("..AGE..Under.5.years", "..AGE..5.to.9.years") ~ "Less than 10 years", 
                         agegroup %in% c("..AGE..10.to.14.years") ~ "≥ 10 and < 15 years",
                         agegroup %in% c("..AGE..15.to.19.years") ~ "≥ 15 and < 20 years",
                         agegroup %in% c("..AGE..20.to.24.years", "..AGE..25.to.29.years", "..AGE..30.to.34.years", "..AGE..35.to.39.years") ~ "≥ 20 and < 40 years",
                         agegroup %in% c("..AGE..40.to.44.years", "..AGE..45.to.49.years", "..AGE..50.to.54.years", "..AGE..55.to.59.years", "..AGE..60.to.64.years") ~ "≥ 40 and < 65 years",
                         agegroup %in% c("..AGE..65.to.69.years", "..AGE..70.to.74.years", "..AGE..75.to.79.years", "..AGE..80.to.84.years", "..AGE..85.years.and.over") ~ "65 years or more",
                         agegroup == "" ~ "Total")) %>%
  group_by(Geography, Geographic.Area.Name, age) %>%
  summarize(pop_2021 = sum(pop_2021)) %>% ungroup() %>%
  mutate(fips = str_replace(Geography, "0500000US", "")) %>%
  separate(Geographic.Area.Name, into = c("county", "state"), sep = ", ", remove = FALSE) %>%
  mutate(county = tolower(county)) %>%
  mutate(county = str_replace(county, "-", " ")) %>%
  mutate(county = gsub("'", "", county)) %>%
  mutate(county = gsub("\\.", "", county)) %>%
  mutate(county = str_replace(county, "^st ", "saint ")) %>%
  mutate(county = str_replace(county, "^ste ", "sainte ")) %>%
  mutate(county = str_replace(county, " county| city and borough| borough| parish| municipality| municipio", "")) %>%
  dplyr::select(fips, state, county, age, pop_2021) %>% 
  mutate(state = state2abbr(state)) %>%
  mutate(county = case_when(county == "lasalle" ~ "la salle",
                            county == "laporte" ~ "la porte",
                            county == "doña ana" & state == "NM" ~ "dona ana",
                            county == "radford city" & state == "VA" ~ "radford",
                            county == "salem city" & state == "VA" ~ "salem", 
                            TRUE ~ county)) %>%
  filter(!is.na(state))



#-----------------------------------------------------------------------------------
# match diabetes_obesity_county with population data and fips code using county name
#-----------------------------------------------------------------------------------

# minor processing county names in our data
temp <- diabetes_obesity_county %>% 
  dplyr::rename(county_state = county) %>%
  separate(county_state, into = c("county", "state"), sep = ", ", remove = FALSE) %>%
  mutate(county = tolower(county)) %>%
  mutate(county = str_replace(county, "-", " ")) %>%
  mutate(county = str_replace(county, "^st ", "saint ")) 


# match with fips and pop using county and state name (3029 out of 3029 counties matched)
temp_matched <- temp %>% filter(county %in% pop_county_age$county)  %>%
  left_join(pop_county_age, by = c("state", "county", "age")) %>% 
  filter(!is.na(fips))


#-----------------------------------------
# delete duplicate rows in KY (Kentucky)
#-----------------------------------------

# seems that data from KY are duplicated twice in the original 4 county-level datasets that Stephanie sent, and one set doesn't have data, remove these rows
temp_KY <- temp_matched %>% filter(state == "KY") %>% arrange(county_state)
rows_to_delete <- unlist(lapply(seq(1, nrow(temp_KY), by = 2*length(unique(temp_KY$age))), function(x) x:(x + length(unique(temp_KY$age))-1 )))
temp_KY <- temp_KY[-rows_to_delete,]

# get the county-level dataset with population data for "Total" age group only
temp_matched <- temp_matched %>% filter(state != "KY") %>% bind_rows(temp_KY) 


#-------------------------------------------------------------------------------------------------
# calculate pct_captured (N_patients / pop * 100)% and merge with shapefiles loaded from usmap pkg
#-------------------------------------------------------------------------------------------------

diabetes_obesity_county <- temp_matched %>% 
  mutate(pct_captured = ifelse(N_patients == "10 or fewer", NA, as.numeric(N_patients)/pop_2021 * 100 )) %>%
  mutate(pct_diabetes = as.numeric(gsub('%','',pct_diabetes)),
        pct_obesity = as.numeric(gsub('%','',pct_diabetes))) %>%
  left_join( us_map("counties") %>% dplyr::select(fips), by = "fips") %>% 
  st_as_sf() %>%
  mutate(age = factor(age, levels = c("Less than 10 years", "≥ 10 and < 15 years", "≥ 15 and < 20 years", "≥ 20 and < 40 years", "≥ 40 and < 65 years", "65 years or more", "Total")))


#----------------------------------------------------------------
# save county level diabetes and obesity data with FIPS and pop
#----------------------------------------------------------------

saveRDS(diabetes_obesity_county, "Data/Plot Files/Cosmos ED/diabetes_obesity_county.rds")

diabetes_obesity_county_slim <- diabetes_obesity_county %>%
  as.data.frame() %>%
    dplyr::select(fips, age,pct_diabetes ,pct_obesity,pct_captured) %>%
  reshape2::melt(., id.vars=c('fips', 'age')) %>%
  rename(disease=variable)
  
read_parquet(diabetes_obesity_county_slim,'')

#----------------------------------------------------------
# make county level maps of %population captured by cosmos
#----------------------------------------------------------

diabetes_obesity_county %>% 
  ggplot() +
  scale_fill_viridis_c(na.value = "grey") +         
  geom_sf(aes(fill = pct_captured), lwd = 0) +
  facet_wrap( ~ age) +
  theme_minimal()


# if limit max pct_captured = 100%
diabetes_obesity_county %>% 
  mutate(pct_captured = ifelse(pct_captured > 100, 100, pct_captured)) %>%
  ggplot() +
  scale_fill_viridis_c(na.value = "grey") +         
  geom_sf(aes(fill = pct_captured), lwd = 0) +
  facet_wrap( ~ age) +
  theme_minimal()

# if limit max pct_captured = 100%
diabetes_obesity_county %>% 
  filter(age=='Total') %>%
  mutate(pct_captured = ifelse(pct_captured > 100, 100, pct_captured)) %>%
  ggplot() +
  scale_fill_viridis_c(na.value = "grey") +         
  geom_sf(aes(fill = pct_captured), lwd = 0) +
  theme_minimal()



diabetes_obesity_county %>% 
  filter(age=='Total') %>%
  mutate(pct_diabetes = ifelse(pct_diabetes > 100, 100, pct_diabetes)) %>%
  ggplot() +
  scale_fill_viridis_c(na.value = "grey") +         
  geom_sf(aes(fill = pct_diabetes), lwd = 0) +
  theme_minimal()

###############################################
# also process the state level data
###############################################

# read in pop data by state from census.gov (2021) (this additionally has DC, PR)
pop_state_age <- read.csv("Data/other_data/diabetes_obesity/population_state_age_censusgov_2021.csv", skip = 1) %>% 
  dplyr::select(c(1,2,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39)) %>%
  pivot_longer(cols = 3:21, names_to = "agegroup", values_to = "pop_2021", names_prefix = "Estimate..Total..Total.population") %>% 
  mutate(age_level = case_when(agegroup %in% c("..AGE..Under.5.years", "..AGE..5.to.9.years") ~ "<10 Years", 
                         agegroup %in% c("..AGE..10.to.14.years") ~ "10-14 Years",
                         agegroup %in% c("..AGE..15.to.19.years") ~ "15-19 Years",
                         agegroup %in% c("..AGE..20.to.24.years", "..AGE..25.to.29.years", "..AGE..30.to.34.years", "..AGE..35.to.39.years") ~ "20-39 Years",
                         agegroup %in% c("..AGE..40.to.44.years", "..AGE..45.to.49.years", "..AGE..50.to.54.years", "..AGE..55.to.59.years", "..AGE..60.to.64.years") ~ "40-64 Years",
                         agegroup %in% c("..AGE..65.to.69.years", "..AGE..70.to.74.years", "..AGE..75.to.79.years", "..AGE..80.to.84.years", "..AGE..85.years.and.over") ~ "65+ Years",
                         agegroup == "" ~ "Total")) %>%
  dplyr::rename(geography = Geographic.Area.Name) %>%
  group_by(geography, age_level) %>%
  summarize(pop_2021 = sum(pop_2021))



# load state level data and join with pop data (from 2021 data from census.gov)
diabetes_obesity_state <- open_dataset( './Data/Plot Files/Cosmos ED/diabetes_obesity.parquet') %>% collect() %>%
  filter(!is.na(age_level)) %>% 
  left_join(pop_state_age, by = c("geography", "age_level")) %>%
  mutate(pct_captured = ifelse(outcome_name == "N_patients", Outcome_value1 / pop_2021 * 100, NA)) %>%
  left_join(us_map("states") %>% dplyr::select(full), by = c("geography" = "full")) %>%
  st_as_sf()


# save state level data
saveRDS(diabetes_obesity_state, "Data/Plot Files/Cosmos ED/diabetes_obesity_state.rds")


# make county level maps of %population captured by cosmos
diabetes_obesity_state %>% 
  ggplot() +
  scale_fill_viridis_c(na.value = "grey") +         
  geom_sf(aes(fill = pct_captured), lwd = 0) +
  facet_wrap( ~ age_level) +
  theme_minimal()


# if limit max pct_captured = 100%
diabetes_obesity_state %>% 
  mutate(pct_captured = ifelse(pct_captured > 100, 100, pct_captured)) %>%
  ggplot() +
  scale_fill_viridis_c(na.value = "grey") +         
  geom_sf(aes(fill = pct_captured), lwd = 0) +
  facet_wrap( ~ age_level) +
  theme_minimal()





