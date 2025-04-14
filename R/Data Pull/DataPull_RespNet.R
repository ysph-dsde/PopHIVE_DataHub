## ----------------------------------------------------------------
## API Pull of RSV Surveillance Data Sets
##
##      Authors: Daniel Weinberger PhD and Shelby Golden, M.S.
## Date Created: April 10th, 2025
## 
##     Version: v2
## Last Edited: April 13th, 2025
## 
## Description: 
## 

## ----------------------------------------------------------------
## SET UP THE ENVIRONMENT

# NOTE: the renv initializing might need to be run twice when the repo is
#       first being copied.
renv::restore()


suppressPackageStartupMessages({
  library("arrow")
  library("readxl")
  library("readr")
  library("data.table")
  library("R.utils")
  library("tidyr")
  library("dplyr")
  library("stringr")
  library("lubridate")
  library("glue")
  library("RSocrata")
  library("MMWRweek")
})

"%!in%" <- function(x,y)!("%in%"(x,y))

source('./R/Support Functions/API Interaction.R')




## ----------------------------------------------------------------
## RESP-NET

# ---------------------------------
# RESP-NET
#
# By week, state, race, etc. Age*state missing for RSV.

url_resp_net <- "https://data.cdc.gov/resource/kvib-3txy.csv"

# Try out the new directory features
# 1. commit "respnet" to "Data/Pulled Data/test" when "~/test" does not exist.
# 2. change sourceName = "respnet-2" and try to see the function commit
#    regardless of no previous archives for that source.
cdc_respnet <- runIfExpired(sourceName = "respnet", storeIn = "test", 
                            f = ~ read.socrata(url_resp_net), 
                            fileType = "parquet", tolerance = (24*7))

#runIfExpired(source = 'respnet',storeIn='Raw',  basepath='./Data/Archive',=
#               ~ read.socrata(url_resp_net),tolerance=(24*7))

h1_harmonized_rsv <- cdc_respnet %>%
  filter( site != 'Overall' & 
            surveillance_network=='RSV-NET' & type=='Unadjusted Rate' & age_group=='Overall' &
            sex=='Overall' & race_ethnicity=='Overall') %>%
  rename(state=site, hosp_rate=weekly_rate, date=X_weekenddate) %>%
  mutate(date=as.Date(substr(date,1,10)) ) %>%
  dplyr::select(state, date, hosp_rate) %>%
  as.data.frame() %>% 
  rename(Outcome_value1=hosp_rate,
         geography=state) %>%
  mutate(outcome_type='Inpatient Hospitalization',
         outcome_label1 = 'Hospitalization Rate',
         domain = 'Respiratory infections',
         date_resolution = 'week',
         update_frequency = 'weekly',
         source = 'CDC RSV-NET (RespNet)',
         url = 'https://data.cdc.gov/resource/kvib-3txy.csv',
         geo_strata = 'state',
         age_strata = 'none',
         race_strata = 'none',
         race_level = NA_character_,
         additional_strata1 = 'none',
         additional_strata_level = NA_character_,
         sex_strata = 'none',
         sex_level = NA_character_)

h1_harmonized_flu <- cdc_respnet %>%
  filter( site != 'Overall' & 
            surveillance_network=='FluSurv-NET' & type=='Unadjusted Rate' & age_group=='Overall' &
            sex=='Overall' & race_ethnicity=='Overall') %>%
  rename(state=site, hosp_rate=weekly_rate, date=X_weekenddate) %>%
  mutate(date=as.Date(substr(date,1,10)) ) %>%
  dplyr::select(state, date, hosp_rate) %>%
  as.data.frame() %>% 
  rename(Outcome_value1=hosp_rate,
         geography=state) %>%
  mutate(outcome_type='Inpatient Hospitalization',
         outcome_label1 = 'Hospitalization Rate',
         domain = 'Respiratory infections',
         date_resolution = 'week',
         update_frequency = 'weekly',
         source = 'CDC FluSurv-NET',
         url = 'https://data.cdc.gov/resource/kvib-3txy.csv',
         geo_strata = 'state',
         age_strata = 'none',
         race_strata = 'none',
         race_level = NA_character_,
         additional_strata1 = 'none',
         additional_strata_level = NA_character_,
         sex_strata = 'none',
         sex_level = NA_character_)

h1_harmonized_covid <- cdc_respnet %>%
  filter( site != 'Overall' & 
            surveillance_network=='COVID-NET' & type=='Unadjusted Rate' & age_group=='Overall' &
            sex=='Overall' & race_ethnicity=='Overall') %>%
  rename(state=site, hosp_rate=weekly_rate, date=X_weekenddate) %>%
  mutate(date=as.Date(substr(date,1,10)) ) %>%
  dplyr::select(state, date, hosp_rate) %>%
  as.data.frame() %>% 
  rename(Outcome_value1=hosp_rate,
         geography=state) %>%
  mutate(outcome_type='Inpatient Hospitalization',
         outcome_label1 = 'Hospitalization Rate',
         domain = 'Respiratory infections',
         date_resolution = 'week',
         update_frequency = 'weekly',
         source = 'CDC COVID-NET',
         url = 'https://data.cdc.gov/resource/kvib-3txy.csv',
         geo_strata = 'state',
         age_strata = 'none',
         race_strata = 'none',
         race_level = NA_character_,
         additional_strata1 = 'none',
         additional_strata_level = NA_character_,
         sex_strata = 'none',
         sex_level = NA_character_)
