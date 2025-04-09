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
# Blunt date storage, no accounting for overlap.









