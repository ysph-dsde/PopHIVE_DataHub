

library(tidyverse)
source('./R/EpicClean/epic_chronic_import.R')

#EPIC ED all cause
epic_ed_all_latest_file = datetimeStamp( basepath='./Data/Archive/Cosmos ED/Chronic Disease/Raw')$`Report Relative to Date` %>%
  filter(Delta==min(Delta)) %>%
  pull(filePath) 

epic_chronic <- epic_import_chronic(ds_name = paste0('./Data/Archive/Cosmos ED/Chronic Disease/Raw/',epic_ed_all_latest_file)) %>%
  rename(outcome_name=Measures) %>% 
  mutate(
    outcome_type = 'prevalence',
    domain = 'Chronic diseases',
    date_resolution = 'none',
    update_frequency = 'annual',
    source = 'Epic Cosmos',
    url = 'https://www.epicresearch.org/',
    geo_strata = 'state',
    age_strata = 'age_scheme_2',
    race_strata = 'none',
    race_level = NA_character_,
    additional_strata1 = 'none',
    additional_strata_level = NA_character_,
    sex_strata = 'none',
    sex_level = NA_character_,
    
    age_strata = if_else(Level=='Total','none',age_strata ),
    geo_strata = if_else(geography=='Total', 'none',geo_strata )
  )  %>%
  ungroup() %>%
  rename(
    Outcome_value1 = pct,
    age_level = Level
  ) %>%
  dplyr::select(
    "geography",
    "age_level",
    "outcome_name",
    "outcome_type",
    "source",
    "url",
    "geo_strata",
    "age_strata",
    "race_strata",
    "race_level",
    "additional_strata1",
    "additional_strata_level",
    "sex_strata",
    "sex_level",
    "Outcome_value1"
  ) %>%
  mutate(
    outcome_label1 = 'Prevalence'
  )


write_parquet(epic_chronic,
              './Data/Archive/Cosmos ED/diabetes_obesity.parquet')

#test <- read_parquet( './Data/harmonized_epic_flu_rsv_covid.parquet') %>% collect()