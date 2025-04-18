## ----------------------------------------------------------------
## Ingest API Pulled Google Searches from DISSC's Repo
##
##      Authors: Daniel Weinberger PhD and Micah Iserman, PhD
## Date Created: April 11th, 2025
## 
##     Version: v2
## Last Edited: April 17th, 2025
##      Source: https://dissc-yale.github.io/gtrends_collection/
## 
## Description: 
## 

## ----------------------------------------------------------------
## SET UP THE ENVIRONMENT

# NOTE: the renv initializing might need to be run twice when the repo is
#       first being copied.
# renv::restore()


# suppressPackageStartupMessages({
#   library("arrow")
#   library("readxl")
#   library("readr")
#   library("data.table")
#   library("R.utils")
#   library("tidyr")
#   library("dplyr")
#   library("stringr")
#   library("lubridate")
#   library("glue")
#   library("RSocrata")
#   library("MMWRweek")
# })

# "%!in%" <- function(x,y)!("%in%"(x,y))

# source('./R/Support Functions/API Interaction.R')

g_states <- paste('US', state.abb, sep = '-')




## ----------------------------------------------------------------
## Google searches for RSV vaccination

url2 <- "https://github.com/DISSC-yale/gtrends_collection/raw/refs/heads/main/data/term=%252Fg%252F11j30ybfx6/part-0.parquet" #rsv vaccination category

temp_file2 <- tempfile(fileext = ".parquet")
download.file(url2, temp_file2, mode = "wb")

google_rsv_vax <- runIfExpired(sourceName = "google_rsv_vax", storeIn = "Google RSV", 
                               f = ~ read_parquet(temp_file2),
                               fileType = "parquet", tolerance = (24*7))


g1_vax_state <- google_rsv_vax %>%
  filter( location %in% g_states ) %>%
  mutate(date=as.Date(date),
         date = as.Date(ceiling_date(date, 'week'))-1,
         stateabb= gsub('US-','', location),
         state=state.name[match(stateabb,state.abb)],
         value=round(value,2)) %>%
  rename(search_volume_vax=value) %>%
  dplyr::select(state, date, search_volume_vax) %>%
  distinct() %>%
  filter(date>='2014-01-01') %>%
  group_by(state,date) %>%
  summarize(search_volume_vax=mean(search_volume_vax, na.rm=T))




## ----------------------------------------------------------------
## Google searches for RSV

url1 <- "https://github.com/DISSC-yale/gtrends_collection/raw/refs/heads/main/data/term=rsv/part-0.parquet"

temp_file1 <- tempfile(fileext = ".parquet")
download.file(url1, temp_file1, mode = "wb")

google_rsv <- runIfExpired(sourceName = "google_rsv", storeIn = "Google RSV", 
                           f = ~ read_parquet(temp_file1),
                           fileType = "parquet", tolerance = (24*7))


google_merged_rsv = google_rsv %>%
  filter(location %in% g_states) %>%
  mutate(date=as.Date(date),
         date = as.Date(ceiling_date(date, 'week'))-1,
         stateabb= gsub('US-','', location),
         state=state.name[match(stateabb,state.abb)],
         value=round(value,2)) %>%
  rename(search_volume=value) %>%
  dplyr::select(state, date, search_volume) %>%
  group_by(state,date) %>%
  summarize(search_volume=mean(search_volume, na.rm=T)) %>%
  filter(date>='2014-01-01') %>%
  full_join(g1_vax_state, by=c('state', 'date') ) %>%
  mutate(month=month(date),
         season = if_else(month>=7 & month <=10,1,0),
         rsv_novax = search_volume - search_volume_vax ,
         rsv_novax = rsv_novax / max(rsv_novax, na.rm=T),
         rsv_novax2 = search_volume - season*(4.41-1.69)*search_volume_vax - (1-season)*3.41*search_volume_vax,  #2.655 based on the regression below
         rsv_novax2 = if_else(rsv_novax2<0,0,rsv_novax2),
         rsv_novax2 = rsv_novax2 / max(rsv_novax2, na.rm=T),
         search_volume = search_volume/ max(search_volume, na.rm=T),
         search_volume_vax = search_volume_vax/ max(search_volume_vax, na.rm=T)
  ) 

g1_state_harmonized_v1 <- google_merged_rsv %>%
  rename(Outcome_value1=rsv_novax,
         geography=state) %>%
  mutate(outcome_type='Google Searches',
         outcome_label1 = 'Google Searches 1',
         domain = 'Respiratory infections',
         date_resolution = 'week',
         update_frequency = 'weekly',
         source = 'Google Health Trends',
         url = 'https://dissc-yale.github.io/gtrends_collection/',
         geo_strata = 'state',
         age_strata = 'none',
         race_strata = 'none',
         race_level = NA_character_,
         additional_strata1 = 'none',
         additional_strata_level = NA_character_,
         sex_strata = 'none',
         sex_level = NA_character_) 


g1_state_harmonized_v2 <- google_merged_rsv %>%
  rename(Outcome_value1=rsv_novax2,
         geography=state) %>%
  mutate(outcome_type='Google Searches',
         outcome_label1 = 'Google Searches 2',
         domain = 'Respiratory infections',
         date_resolution = 'week',
         update_frequency = 'weekly',
         source = 'Google Health Trends',
         url = 'https://dissc-yale.github.io/gtrends_collection/',
         geo_strata = 'state',
         age_strata = 'none',
         race_strata = 'none',
         race_level = NA_character_,
         additional_strata1 = 'none',
         additional_strata_level = NA_character_,
         sex_strata = 'none',
         sex_level = NA_character_) 

#od1 <- lme4::lmer(search_volume~ 1 + season*search_volume_vax +  (1|state), data=g1_state[g1_state$date>=as.Date('2023-07-01'),])
# coef.vax <- summary(mod1)$coefficients['search_volume_vax','Estimate']
