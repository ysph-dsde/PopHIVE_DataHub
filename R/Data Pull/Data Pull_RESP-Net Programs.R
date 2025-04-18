## ----------------------------------------------------------------
## API Pull of RESP-NET Surveillance Data Sets
##
##      Authors: Daniel Weinberger PhD and Shelby Golden, M.S.
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
## RESP-NET
## 
## By week, state, race, etc. Age*state missing for RSV.

url_resp_net <- "https://data.cdc.gov/resource/kvib-3txy.csv"

cdc_respnet <- runIfExpired(sourceName = "resp-net", storeIn = "RESP-NET Programs", 
                            f = ~ read.socrata(url_resp_net), returnRecent = FALSE,
                            fileType = "parquet", tolerance = (24*7))




# ---------------------------------
# RSV-NET

h1_harmonized_rsv <- cdc_respnet %>%
  filter( site != 'Overall' & 
            surveillance_network=='RSV-NET' & type=='Unadjusted Rate' & age_group=='Overall' &
            sex=='Overall' & race_ethnicity=='Overall') %>%
  rename(state=site, hosp_rate=weekly_rate, date=X_weekenddate) %>%
  mutate(date=as.Date(substr(date,1,10)) ) %>%
  dplyr::select(state, date, hosp_rate) %>%
  as.data.frame() %>% 
  rename(Outcome_value1=hosp_rate, geography=state) %>%
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




# ---------------------------------
# FluSurv-NET

h1_harmonized_flu <- cdc_respnet %>%
  filter( site != 'Overall' & 
            surveillance_network=='FluSurv-NET' & type=='Unadjusted Rate' & age_group=='Overall' &
            sex=='Overall' & race_ethnicity=='Overall') %>%
  rename(state=site, hosp_rate=weekly_rate, date=X_weekenddate) %>%
  mutate(date=as.Date(substr(date,1,10)) ) %>%
  dplyr::select(state, date, hosp_rate) %>%
  as.data.frame() %>% 
  rename(Outcome_value1=hosp_rate, geography=state) %>%
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




# ---------------------------------
# COVID-NET

h1_harmonized_covid <- cdc_respnet %>%
  filter( site != 'Overall' & 
            surveillance_network=='COVID-NET' & type=='Unadjusted Rate' & age_group=='Overall' &
            sex=='Overall' & race_ethnicity=='Overall') %>%
  rename(state=site, hosp_rate=weekly_rate, date=X_weekenddate) %>%
  mutate(date=as.Date(substr(date,1,10)) ) %>%
  dplyr::select(state, date, hosp_rate) %>%
  as.data.frame() %>% 
  rename(Outcome_value1=hosp_rate, geography=state) %>%
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




## ----------------------------------------------------------------
## RSV-NET
## 
## NOTE: Not all of the same sites are represented for RSV-NET in
##       the RESP-NET compilation. Namely, Ohio and Iowa.

url_rsv_net <- "https://data.cdc.gov/resource/29hc-w46k.csv"

cdc_rsvnet <- runIfExpired(sourceName = "rsv-net", storeIn = "RESP-NET Programs", 
                           f = ~ read.socrata(url_rsv_net), returnRecent = FALSE,
                           fileType = "parquet", tolerance = (24*7))




h1.age.rsv <- cdc_rsvnet %>%
  filter(state!="RSV-NET" & sex=='All' & race=='All' & type=='Crude Rate') %>%
  rename( hosp_rate=rate, date=week_ending_date, Level=age_category) %>%
  filter(Level %in% c('1-4 years', '0-<1 year','5-17 years', '18-49 years' ,
                      "≥65 years" ,"50-64 years" )) %>%
  mutate( Level = if_else(Level=='0-<1 year',"<1 Years",
                          if_else( Level=='1-4 years', "1-4 Years",
                                   if_else(Level=="5-17 years" ,"5-17 Years",
                                           if_else(Level=="18-49 years" ,"18-49 Years",
                                                   if_else(Level=="50-64 years" ,"50-64 Years",
                                                           if_else(Level=="≥65 years",'65+ Years', 'other'               
                                                           ))))))
  ) %>%
  dplyr::select(state, date, hosp_rate, Level) %>%
  ungroup() %>%
  filter( date >=as.Date('2023-07-01')) %>%
  arrange(state, Level, date) %>%
  group_by(state, Level) %>%
  mutate(hosp_rate_3m=zoo::rollapplyr(hosp_rate,3,mean, partial=T, na.rm=T),
         hosp_rate_3m = if_else(is.nan(hosp_rate_3m), NA, hosp_rate_3m),
         
         scale_age=hosp_rate_3m/max(hosp_rate_3m, na.rm=T )*100,
         
  ) %>%
  as.data.frame()

write.csv(h1.age.rsv,'./Data/Plot Files/rsv_hosp_age_respnet.csv')






## ----------------------------------------------------------------
## COVID-NET

url_covid_net <- "https://data.cdc.gov/resource/6jg4-xsqq.csv"

cdc_covidnet <- runIfExpired(sourceName = "covid-net", storeIn = "RESP-NET Programs", 
                             f = ~ read.socrata(url_covid_net), returnRecent = FALSE,
                             fileType = "parquet", tolerance = (24*7))





