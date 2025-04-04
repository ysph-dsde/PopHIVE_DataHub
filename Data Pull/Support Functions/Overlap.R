## ----------------------------------------------------------------
## Functions to deal with dataset overlapping.
##
## Date: March 29th, 2025
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
  library("RSocrata")
  library("MMWRweek")
})

"%!in%" <- function(x,y)!("%in%"(x,y))




## ----------------------------------------------------------------
## RESP-NET

old_data <- read_parquet(filePath(getwd(), "Data Pull/RESP NET Archive/source_info_2025_03_06_16_11.parquet"))


url_rsv_net <- "https://data.cdc.gov/resource/29hc-w46k.csv"
f = ~ read.socrata(url_rsv_net)
f <- rlang::as_function(f)
data <- f()


# ---------------------------------
# Input needs
# 1. The recently pulled dataset needs to have its date column class = Date.

class(data$week_ending_date)
data$week_ending_date     <- as.Date(data$week_ending_date)
old_data$week_ending_date <- as.Date(old_data$week_ending_date)




# ---------------------------------
# Remove recent entries with no value.
# 1. Find the recent consecutive dates with "NULL" or NA's in both
#    Outcomes variables, defined in the Data Dictionary.
# 2. Remove any rows that meet this criteria.
# 3. If none, exit function and throw an error.
# 
# NOTE: Function was tested in 
#       "Data Pull/Support Functions/Nuanced Import Simulation"

rm_recent_null <- function(data, date_col, dictionary){
  # This will remove recently dated entries that have no value entered.
  # Some sources will add rows that are recent to the API call, but
  # lack any value entries. They're recorded as "NULL" or NA's
  #
  #       "data": the data that was recently called in by the API.
  #   "date_col": specify the column with whole date associated. 
  #               with the data entry. Needs to be class type "Date".
  # "dictionary": the CSV containing information about the raw data
  #               columns that are expected.
  
  
  # Pull the column names and their use assignment from the data dictionary.
  col_type = raw_columns(dictionary)
  # Isolate the variables that are used for "Outcome". These are the
  # numeric columns of interest.
  variables <- col_type[col_type$Use == "Outcome", "Raw Data Columns"]
  
  filter_statement <- variables %>%
    str_c(., " %in% c('NULL', NA)") %>%
    str_flatten(collapse = " & ")
  
  # Confirm that date_col is a Date object.
  assertthat::assert_that(any("Date" %in% sapply(data[, colnames(data) %in% date_col], class)))
  
  
  # -----------------------------
  # Find and remove the recent date range that has NA's and NULL.
    
  # Group dates that are consecutive weeks.
  dates_na_null <- data %>%
    # Filter over the outcome columns.
    filter(eval(rlang::parse_expr(filter_statement))) %>%
    # Save and order the dates associated.
    select(week_ending_date) %>% pull() %>% unique() %>% sort() %>%
    # Evaluate which dates are not within 7 days of each other and
    # save in separate list entries.
    (\(x) { split(x, cumsum(c(TRUE, diff(x) != 7))) }) ()
  
  
  recent_date <- data[, colnames(data) %in% date_col] %>% pull() %>% max()
  
  # The list entry that represents the most recent set of entries.
  eval_recent_dates <- dates_na_null %>%
    # Search each list element for the max year in the consecutive
    # series of weeks.
    lapply(., function(x){
      any(x %in% recent_date)
    }) %>%
    unlist() %>% 
    # Report the index of the weeks that are most recent.
    (\(x) { which(x == TRUE) }) ()
  
  
  # Confirm the recent date of entry is included in the recent date
  # ranges where NULL and NA's were found for all variables. If not,
  # exit the function.
  if(length(eval_recent_dates) < 1){
    stop( sprintf("Most recent entries where %s have NULL or NA's does not include \nthe most recent date. Step not needed; abort operation.", str_flatten(variables, collapse = " and ")) )
  }
  
  # Create an argument that will filter out the recent dates with
  # no reported values.
  date_filter_argument <- 
    str_flatten(dates_na_null[[eval_recent_dates]], collapse = "', '") %>%
    str_c("c('", ., "')") %>%
    paste(date_col, ., sep = " %!in% ")
  
  # Remove these values.
  result <- data %>% ƒdelta
    filter(eval(rlang::parse_expr(date_filter_argument))) %>%
    `rownames<-`(NULL)
  
  
  result
  
}




# ---------------------------------
# Blunt ends
# 1. Prune missing values near the max of the queried dataset.
# 2. Cut off at the max of the recently existing archive.




# ---------------------------------
# Add:
# - blunt ends, overlap consideration.
# - function to compile recent pull into one running file for harmonization.
# - QC to confirm column names, based on that runifexpired() function.








