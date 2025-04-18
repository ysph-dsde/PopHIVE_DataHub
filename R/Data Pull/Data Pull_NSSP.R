## ----------------------------------------------------------------
## API Pull of NSSP Surveillance Data Sets
##
##      Authors: Daniel Weinberger PhD
## Date Created: April 11th, 2025
## 
##     Version: v2
## Last Edited: April 17th, 2025
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




## ----------------------------------------------------------------
## National syndromic surveillance data (CDC) 
## 
## county*week (no age stratification)

url_nssp <- "https://data.cdc.gov/resource/rdmq-nq56.csv"

cdc_nssp_rsv_flu_covid_ed1 <- runIfExpired(sourceName = "nssp_ed1", storeIn = "NSSP ED1", 
                                           f = ~ read.socrata(url_nssp),
                                           fileType = "parquet", tolerance = (24*7))


nssp_harmonized_rsv <- cdc_nssp_rsv_flu_covid_ed1 %>%
  filter(county=='All'  ) %>%
  rename(state=geography, date='week_end') %>%
  dplyr::select(state, date, percent_visits_rsv) %>%
  collect() %>%
  as.data.frame() %>%
  rename(Outcome_value1=percent_visits_rsv,
         geography=state) %>%
  mutate(outcome_type='ED',
         outcome_label1 = 'Pct of ED visits',
         domain = 'Respiratory infections (RSV)',
         date_resolution = 'week',
         update_frequency = 'weekly',
         source = 'CDC NSSP',
         url = 'https://healthdata.gov/dataset/NSSP-Emergency-Department-Visit-Trajectories-by-St/hr4c-e7p6/',
         geo_strata = 'state',
         age_strata = 'none',
         race_strata = 'none',
         race_level = NA_character_,
         additional_strata1 = 'none',
         additional_strata_level = NA_character_,
         sex_strata = 'none',
         sex_level = NA_character_)

nssp_harmonized_flu <- cdc_nssp_rsv_flu_covid_ed1 %>%
  filter(county=='All'  ) %>%
  rename(state=geography, date='week_end') %>%
  dplyr::select(state, date, percent_visits_influenza) %>%
  collect() %>%
  as.data.frame() %>%
  rename(Outcome_value1=percent_visits_influenza,
         geography=state) %>%
  mutate(outcome_type='ED',
         outcome_label1 = 'Pct of ED visits',
         domain = 'Respiratory infections (flu)',
         date_resolution = 'week',
         update_frequency = 'weekly',
         source = 'CDC NSSP',
         url = 'https://healthdata.gov/dataset/NSSP-Emergency-Department-Visit-Trajectories-by-St/hr4c-e7p6/',
         geo_strata = 'state',
         age_strata = 'none',
         race_strata = 'none',
         race_level = NA_character_,
         additional_strata1 = 'none',
         additional_strata_level = NA_character_,
         sex_strata = 'none',
         sex_level = NA_character_)

nssp_harmonized_covid <- cdc_nssp_rsv_flu_covid_ed1 %>%
  filter(county=='All'  ) %>%
  rename(state=geography, date='week_end') %>%
  dplyr::select(state, date, percent_visits_covid) %>%
  collect() %>%
  as.data.frame() %>%
  rename(Outcome_value1=percent_visits_covid,
         geography=state) %>%
  mutate(outcome_type='ED',
         outcome_label1 = 'Pct of ED visits',
         domain = 'Respiratory infections (flu)',
         date_resolution = 'week',
         update_frequency = 'weekly',
         source = 'CDC NSSP',
         url = 'https://healthdata.gov/dataset/NSSP-Emergency-Department-Visit-Trajectories-by-St/hr4c-e7p6/',
         geo_strata = 'state',
         age_strata = 'none',
         race_strata = 'none',
         race_level = NA_character_,
         additional_strata1 = 'none',
         additional_strata_level = NA_character_,
         sex_strata = 'none',
         sex_level = NA_character_)


