## ----------------------------------------------------------------
## Simulation to construct nuanced file ingestion following API call.
##
## Date: March 30th, 2025
## Author: Shelby Golden, M.S.
## 
## Description: The API call functions account for recently updated archived
##              data, but does not account for entry crossover. Some
##              datasets will require "blunt" archival, only keeping new
##              rows of entries for new dates, and some need a degree of
##              crossover maintained to trace backward value updating.
##              Additionally, some recent entries in data pulled from the API
##              will have missing values. These do not need to be retained.
##              
##              This script is intended to practice nuanced archival using
##              simulated datasets.
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
## MAKE FILES

set.seed(0)

starting <- read_parquet(file.path(getwd(), "/Data Pull/Support Functions/Nuanced Inport Simulation/rsv-net_2025_03_28_23_54.parquet")) %>%
  as.data.frame()

starting <- starting[with(starting, order(week_ending_date, season, state, sex, race, age_category)), ] %>% 
  `rownames<-`(NULL)


data_dictionary <- read_excel("Data Pull/Support Functions/Nuanced Inport Simulation/Data Dictionary_RESP-NET Programs_copied 03.30.2025.xlsx", 
                              sheet = "RSV-NET") %>%
  as.data.frame()




# -----------------------------
# NA's/NULL in recent entries

recent_weeks <- starting %>%
  filter(season %in% c("2024-25", "2024-25 (All Ages)")) %>%
  select(week_ending_date) %>% 
  pull() %>% unique() %>%
  .[15:25]


null_one <- starting
null_one[null_one$week_ending_date %in% recent_weeks[8:11], c("rate", "cumulative_rate")] <- c("NULL", "NULL")

null_one %>%
  tibble::rownames_to_column() %>%
  filter(rate %in% 'NULL' & cumulative_rate %in% 'NULL') %>%
  `[[`("rowname") %>%
  as.numeric() -> indices

null_one %>% 
  tibble::rownames_to_column() %>% filter(rowname %!in% indices) %>% 
  `[[`("rowname") %>%
  as.numeric() -> not_indices

rand_sample_rec     <- indices[sample(1:length(indices), 50, replace = FALSE)]
rand_sample_earlier <- not_indices[sample(1:length(not_indices), 50, replace = FALSE)]

null_one[c(rand_sample_rec, rand_sample_earlier), c("rate", "cumulative_rate")] <- c(NA, NA)
null_one[, "week_ending_date"] <- null_one[, "week_ending_date"] %>% as.Date()


write_parquet(null_one, file.path(file.path(getwd(), "/Data Pull/Support Functions/Nuanced Inport Simulation/null_one.parquet")))




# -----------------------------
# New dates entries for simulated data

add = c("2024-25", "2024-25 (All Ages)", "2025-26", "2025-26 (All Ages)")

cross_over_rows <- starting %>%
  filter(season %in% unique(starting$season)[7:16])

new_rows <- starting %>%
  filter(season %in% unique(starting$season)[1:4])


new_rows[new_rows$season %in% "2016-17", "season"] <- "2024-25"
new_rows[new_rows$season %in% "2017-18", "season"] <- "2025-26"
new_rows[new_rows$season %in% "2016-17 (All Ages)", "season"] <- "2024-25 (All Ages)"
new_rows[new_rows$season %in% "2017-18 (All Ages)", "season"] <- "2025-26 (All Ages)"

new_rows$week_ending_date <- as.Date(new_rows$week_ending_date) %m+% years(8)
new_rows$week_ending_date <- as.character(new_rows$week_ending_date)





# -----------------------------
# Back-filling





# Calculate the cumulative sum.
c <- new_rows %>%
  filter(type == "Crude Rate") %>%
  group_by(season, state, age_category, sex, race) %>%
  mutate("new_cumSum" = cumsum(rate)) %>% 
  ungroup() %>%
  as.data.frame()





## ----------------------------------------------------------------
## FUNCTIONS

raw_columns <- function(dictionary){
  # Pull the expected raw column names, as is recorded in the Data
  # Dictionary for the dataset. The order or column names might not
  # be the same, so keep column order generalized.
  #
  # "dictionary": the CSV containing information about the raw data
  #               columns that are expected.
  
  # Select for the data dictionary column denoting these names and their
  # application: metadata or outcome.
  raw_colNames <- dictionary %>% select(`Raw Data Columns`, Use)
  
  # Remove any empty spaces, denoted by a "-" typically, but account
  # for empty entries as well.
  col_names <- raw_colNames[raw_colNames$`Raw Data Columns` %!in% c("-", ""), ] %>%
    `rownames<-`(NULL)
  
  col_names
}


# -----------------------------
# Remove recent entries that are all NULL or NA

na1 <- read_parquet(file.path(getwd(), "/Data Pull/Support Functions/Nuanced Inport Simulation/null_one.parquet"))

# Save result that removed recent NULL/NA's
na2 <- rm_recent_null(na1, "week_ending_date", data_dictionary)
# Should receive the error that says the step isn't needed.
rm_recent_null(na2, "week_ending_date", data_dictionary)



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
  result <- data %>% 
    filter(eval(rlang::parse_expr(date_filter_argument))) %>%
    `rownames<-`(NULL)
  
  
  result
  
}




# -----------------------------
# Blunt date storage, no accounting for overlap.









