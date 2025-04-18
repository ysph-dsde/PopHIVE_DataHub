## ----------------------------------------------------------------
## Ingest API Pulled Google Searches from DISSC's Repo
##
##      Authors: Daniel Weinberger PhD and Micah Iserman, PhD
## Date Created: April 11th, 2025
## 
##     Version: v2
## Last Edited: April 17th, 2025
##      Source: https://dissc-yale.github.io/gtrends_collection/
## 
## Description: 
## 
## 

#  to add a term and updating the site:
#   python scripts/add_terms.py "term, another term"
# python scripts/build_summary.py
# mkdocs build --clean
# You would need to set GOOGLE_API_KEY (such as in a .env file in the repo), install the package to collect, and install hatch or mkdocs to build the site.

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

g_states <- paste('US', state.abb, sep = '-')




## ----------------------------------------------------------------
## Google searches for naloxone and drug overdoses

url2 <- "https://github.com/DISSC-yale/gtrends_collection/raw/refs/heads/main/data/term=Naloxone/part-0.parquet" #rsv vaccination category

temp_file2 <- tempfile(fileext = ".parquet")
download.file(url2, temp_file2, mode = "wb")

google_naloxone <- runIfExpired(sourceName = "google_naloxone", storeIn = "Google Naloxone", 
                               f = ~ read_parquet(temp_file2), returnRecent = FALSE,
                               fileType = "parquet", tolerance = (24*7))


g1_naloxone <- google_naloxone %>%
  filter( location %in% g_states ) %>%
  mutate(date=as.Date(date),
         date = as.Date(ceiling_date(date, 'week'))-1,
         stateabb= gsub('US-','', location),
         state=state.name[match(stateabb,state.abb)],
         value=round(value,2)) %>%
  rename(search_volume_naloxone=value) %>%
  dplyr::select(state, date, search_volume_naloxone) %>%
  group_by(state, date) %>%
  filter(date>='2015-01-01') %>%
  summarize(search_volume_naloxone=mean(search_volume_naloxone, na.rm=T)) %>%
  ungroup() %>%
  arrange(state, date) %>%
  mutate(naloxone_search_12m = zoo::rollapplyr(search_volume_naloxone,52,mean, partial=T, na.rm=T) ) %>%
  ungroup() %>%
  #filter(date==max(date)) %>%
  dplyr::select(date,state, naloxone_search_12m )




## ----------------------------------------------------------------
## Google searches for drug overdose

url3 <- "https://github.com/DISSC-yale/gtrends_collection/raw/refs/heads/main/data/term=overdose/part-0.parquet" #rsv vaccination category

temp_file3 <- tempfile(fileext = ".parquet")
download.file(url3, temp_file3, mode = "wb")

google_naloxone <- runIfExpired(sourceName = "google_overdose", storeIn = "Google Overdose", 
                                f = ~ read_parquet(temp_file3), returnRecent = FALSE,
                                fileType = "parquet", tolerance = (24*7))


g1_overdose <- google_overdose %>%
  filter( location %in% g_states ) %>%
  mutate(date=as.Date(date),
         date = as.Date(ceiling_date(date, 'week'))-1,
         stateabb= gsub('US-','', location),
         state=state.name[match(stateabb,state.abb)],
         value=round(value,2)) %>%
  rename(search_volume_overdose=value) %>%
  dplyr::select(state, date, search_volume_overdose) %>%
  group_by(state, date) %>%
  summarize(search_volume_overdose=mean(search_volume_overdose, na.rm=T)) %>%
  ungroup() %>%
  arrange(state, date) %>%
  mutate(overdose_search_12m = zoo::rollapplyr(search_volume_overdose,52,mean, partial=T, na.rm=T) ) %>%
  ungroup() %>%
  dplyr::select(date,state, overdose_search_12m ) %>%
  #filter(date==max(date)) %>%
  full_join(g1_naloxone, by=c('state','date'))

write.csv(g1_overdose,'./Data/Archive/google_overdose/OD_search_state_recent12m.csv')

#plot(g1_overdose$overdose_search_12m, g1_overdose$naloxone_search_12m)