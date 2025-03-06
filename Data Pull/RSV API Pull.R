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
  library("glue")
})

"%!in%" <- function(x,y)!("%in%"(x,y))

source('./Data Pull/runIfExpired.R')




## ----------------------------------------------------------------
## RESP-NET

url_rsv_net <- "https://data.cdc.gov/resource/29hc-w46k.csv"

# Creates a time/date stamped parquet file in the folder "RESP NET Archive".
# If a file has been downloaded within past week (24*7 hours), 
# it just reads in latest file, otherwise it downloads fresh copy.

cdc_rsv_net_ed1 <- runIfExpired("RESP NET Archive", "Data Pull", ~ read.socrata(url_rsv_net), maxage = hours(24*7))

















