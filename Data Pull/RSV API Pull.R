## ----------------------------------------------------------------
## API Pull of RSV Surveillance Data Sets
##
## Date: March 6th, 2025
## Author: Shelby Golden, M.S.
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
  library("parquetize")
  library("RSocrata")
  library("MMWRweek")
})

"%!in%" <- function(x,y)!("%in%"(x,y))

source('./Data Pull/Support Functions/API Interaction.R')




## ----------------------------------------------------------------
## RESP-NET

# ---------------------------------
# Record History

# Give archival history by source within a specified directory. Also
# provides the recent pull and the most proximate data pull information
# relative to a "goalDate".
timeStamp("RESP NET Archive", "Data Pull", goalDate = lubridate::now())




# ---------------------------------
# RSV-NET

url_rsv_net <- "https://data.cdc.gov/resource/29hc-w46k.csv"

# Creates a time/date stamped parquet file in the folder "RESP NET Archive".
# If a file has been downloaded within past day (tolerance = 24hrs), 
# it just reads in latest file, otherwise it downloads fresh copy.

cdc_rsv_net_ed1 <- runIfExpired("rsv-net", "RESP NET Archive", "Data Pull", 
                                f=~ read.socrata(url_rsv_net), tolerance = 24)




# ---------------------------------
# COVID-NET

url_covid_net <- "https://data.cdc.gov/resource/6jg4-xsqq.csv"

# Creates a time/date stamped parquet file in the folder "RESP NET Archive".
# If a file has been downloaded within past day (tolerance = 24hrs), 
# it just reads in latest file, otherwise it downloads fresh copy.

cdc_covid_net_ed1 <- runIfExpired("covid-net", "RESP NET Archive", "Data Pull", 
                                  f=~ read.socrata(url_covid_net), tolerance = 24)




# ---------------------------------
# FluSurv-NET

url_flu_net <-

# Creates a time/date stamped parquet file in the folder "RESP NET Archive".
# If a file has been downloaded within past day (tolerance = 24hrs), 
# it just reads in latest file, otherwise it downloads fresh copy.

#cdc_flu_net_ed1 <- runIfExpired("flu-net", "RESP NET Archive", "Data Pull", 
#                                  f=~ read.socrata(url_covid_net), tolerance = 24)















