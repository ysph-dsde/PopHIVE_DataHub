## ----------------------------------------------------------------
## API Pull of NWSS Surveillance Data Sets
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
## Wastewater for RSV

url_ww_rsv <- "https://www.cdc.gov/wcms/vizdata/NCEZID_DIDRI/RSVStateLevelDownloadCSV.csv"

cdc_ww_rsv <- runIfExpired(sourceName = "nwss_rsv", storeIn = "NWSS", 
                           f = ~ read_csv(url_ww_rsv),
                           fileType = "parquet", tolerance = (24*7))


ww1_rsv_harmonized <- cdc_ww_rsv%>%
  mutate(date=as.Date(Week_Ending_Date)) %>%
  filter(Data_Collection_Period=='All Results') %>%
  rename(state="State/Territory", rsv_ww="State/Territory_WVAL") %>%
  arrange(state, date) %>%
  dplyr::select(state, date, rsv_ww) %>%
  arrange(state, date) %>%
  collect() %>%
  rename(Outcome_value1=rsv_ww,
         geography=state) %>%
  mutate(outcome_type='WasteWater',
         outcome_label1 = 'Waste Water wval (RSV)',
         domain = 'Respiratory infections',
         date_resolution = 'week',
         update_frequency = 'weekly',
         source = 'CDC NWWS',
         url = 'https://www.cdc.gov/wcms/vizdata/NCEZID_DIDRI/RSVStateLevelDownloadCSV.csv',
         geo_strata = 'state',
         age_strata = 'none',
         race_strata = 'none',
         race_level = NA_character_,
         additional_strata1 = 'none',
         additional_strata_level = NA_character_,
         sex_strata = 'none',
         sex_level = NA_character_) 




## ----------------------------------------------------------------
## Wastewater for Flu A

url_ww_flu <- "https://www.cdc.gov/wcms/vizdata/NCEZID_DIDRI/FluA/FluAStateMapDownloadCSV.csv"

cdc_ww_flu <- runIfExpired(sourceName = "nwss_flu-a", storeIn = "NWSS", 
                           f = ~ read_csv(url_ww_flu),
                           fileType = "parquet", tolerance = (24*7))


ww1_flu_harmonized <- cdc_ww_flu %>%
  mutate(date=as.Date(Week_Ending_Date)) %>%
  filter(Data_Collection_Period=='All Results') %>%
  rename(state="State/Territory", flu_ww="State/Territory_WVAL") %>%
  arrange(state, date) %>%
  dplyr::select(state, date, flu_ww) %>%
  arrange(state, date) %>%
  collect() %>%
  rename(Outcome_value1=flu_ww,
         geography=state) %>%
  mutate(outcome_type='WasteWater',
         outcome_label1 = 'Waste Water wval (Influenza)',
         domain = 'Respiratory infections',
         date_resolution = 'week',
         update_frequency = 'weekly',
         source = 'CDC NWWS',
         url = 'https://www.cdc.gov/wcms/vizdata/NCEZID_DIDRI/FluA/FluAStateMapDownloadCSV.csv',
         geo_strata = 'state',
         age_strata = 'none',
         race_strata = 'none',
         race_level = NA_character_,
         additional_strata1 = 'none',
         additional_strata_level = NA_character_,
         sex_strata = 'none',
         sex_level = NA_character_) 




## ----------------------------------------------------------------
## Wastewater for COVID

url_ww_covid <- "https://www.cdc.gov/wcms/vizdata/NCEZID_DIDRI/SC2StateLevelDownloadCSV.csv"

cdc_ww_covid <- runIfExpired(sourceName = "nwss_covid", storeIn = "NWSS", 
                             f = ~ read_csv(url_ww_covid),
                             fileType = "parquet", tolerance = (24*7))


ww1_covid_harmonized <- cdc_ww_covid %>%
  mutate(date=as.Date(Week_Ending_Date)) %>%
  filter(Data_Collection_Period=='All Results') %>%
  rename(state="State/Territory", covid_ww="State/Territory_WVAL") %>%
  arrange(state, date) %>%
  dplyr::select(state, date, covid_ww) %>%
  arrange(state, date) %>%
  collect() %>%
  rename(Outcome_value1=covid_ww,
         geography=state) %>%
  mutate(outcome_type='WasteWater',
         outcome_label1 = 'Waste Water wval (SARS-CoV-2)',
         domain = 'Respiratory infections',
         date_resolution = 'week',
         update_frequency = 'weekly',
         source = 'CDC NWWS',
         url = 'https://www.cdc.gov/wcms/vizdata/NCEZID_DIDRI/SC2StateLevelDownloadCSV.csv',
         geo_strata = 'state',
         age_strata = 'none',
         race_strata = 'none',
         race_level = NA_character_,
         additional_strata1 = 'none',
         additional_strata_level = NA_character_,
         sex_strata = 'none',
         sex_level = NA_character_) 

<<<<<<< HEAD:R/Data Pull/Data Pull_Wastewater.R






=======
>>>>>>> 20769f9312e975f56e694444b4366e589a89125c:R/Data Pull/DataPull_wastewater.R
