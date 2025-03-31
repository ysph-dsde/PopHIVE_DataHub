## ----------------------------------------------------------------
## API Pull of RSV Surveillance Datasets
##
## Date: March 6th, 2025
## Author: Shelby Golden, M.S.
## 
## Sources:
##    - CDC's RSV-NET (connected with NREVSS data)
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

source('./Data Pull/Support Functions/API Interaction.R')




## ----------------------------------------------------------------
## RESP-NET

url_rsv_net <- "https://data.cdc.gov/resource/29hc-w46k.csv"

# Creates a time/date stamped parquet file in the folder "RESP NET Archive".
# If a file has been downloaded within past week (24*7 hours), 
# it just reads in latest file, otherwise it downloads fresh copy.

cdc_rsv_net_ed1 <- runIfExpired("some_name", "RESP NET Archive", "Data Pull", 
                                f=~ read.socrata(url_rsv_net), tolerance=24)




url_covid_net <- "https://data.cdc.gov/resource/6jg4-xsqq.csv"

# Creates a time/date stamped parquet file in the folder "RESP NET Archive".
# If a file has been downloaded within past week (24*7 hours), 
# it just reads in latest file, otherwise it downloads fresh copy.

cdc_covid_net_ed1 <- runIfExpired("RESP NET Archive", "Data Pull", "covid", ~ read.socrata(url_covid_net), 
                                maxage = hours(24*7))

















