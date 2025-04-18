

library(tidyverse)
source('./R/Cleaning and Harmonization/EpicClean/epic_age_import.R')

#EPIC ED all cause
epic_ed_all_latest_file = datetimeStamp(storeIn = "Cosmos ED/All visits")$`Report Relative to Date` %>%
  filter(Delta==min(Delta)) %>%
  pull(filePath)

epic_ed_all <- epic_age_import(ds_name = paste0('./Data/Pulled Data/Cosmos ED/All visits/',epic_ed_all_latest_file)) %>%
  rename(N_ED_epic_all_cause = N_cases_epic) %>%
  dplyr::select(geography, Level, date, N_ED_epic_all_cause)


#EPIC ED RSV
epic_ed_rsv_latest_file = datetimeStamp(storeIn = 'Cosmos ED/rsv')$`Report Relative to Date` %>%
  filter(Delta==min(Delta)) %>%
  pull(filePath)

epic_ed_rsv <-
  epic_age_import(ds_name = paste0('./Data/Pulled Data/Cosmos ED/rsv/',epic_ed_rsv_latest_file))%>%
  rename(N_ED_type = N_cases_epic) %>%
  dplyr::select(geography, Level, date, N_ED_type) %>%
  filter(!is.na(Level)) %>%
  full_join(epic_ed_all, by = c('geography', 'Level', 'date')) %>%
  arrange(Level, geography, date) %>%
  group_by(Level, geography)  %>%
  mutate(outcome_name = 'RSV')


# EPIC ED FLU
epic_ed_flu_latest_file = datetimeStamp(storeIn='Cosmos ED/flu/')$`Report Relative to Date` %>%
  filter(Delta==min(Delta)) %>%
  pull(filePath)

epic_ed_flu <-
  epic_age_import(ds_name = paste0('./Data/Pulled Data/Cosmos ED/flu/',epic_ed_flu_latest_file), skipN=12)%>%
  rename(N_ED_type = N_cases_epic) %>%
  dplyr::select(geography, Level, date, N_ED_type) %>%
  filter(!is.na(Level)) %>%
  full_join(epic_ed_all, by = c('geography', 'Level', 'date')) %>%
  arrange(Level, geography, date) %>%
  group_by(Level, geography)  %>%
  mutate(outcome_name = 'FLU')

# EPIC ED COVID
epic_ed_covid_latest_file = datetimeStamp(storeIn = 'Cosmos ED/covid/')$`Report Relative to Date` %>%
  filter(Delta==min(Delta)) %>%
  pull(filePath)

epic_ed_covid <-
  epic_age_import(ds_name = paste0('./Data/Pulled Data/Cosmos ED/covid/',epic_ed_covid_latest_file))%>%
  rename(N_ED_type = N_cases_epic) %>%
  filter(!is.na(Level)) %>%
  dplyr::select(geography, Level, date, N_ED_type) %>%
  full_join(epic_ed_all, by = c('geography', 'Level', 'date')) %>%
  arrange(Level, geography, date) %>%
  group_by(Level, geography)  %>%
  mutate(outcome_name = 'COVID')


epic_ed_combo <- bind_rows(epic_ed_rsv, epic_ed_flu , epic_ed_covid) %>%
  filter(!is.na(Level)) %>%
  arrange(outcome_name, Level, geography, date) %>%
  group_by(outcome_name, Level, geography) %>%
  mutate(
    N_ED_type = if_else(
      !is.na(N_ED_epic_all_cause) &
        is.na(N_ED_type),
      4.9999,
      N_ED_type
    ),
    #is suppressed <10 counts, set to 4.9999
    
    pct_ED_epic = N_ED_type / N_ED_epic_all_cause * 100,
    
    pct_ED_epic = if_else(N_ED_type<5, min(pct_ED_epic, na.rm=T)/2, pct_ED_epic ), #if suppressed, half of posiivity
    
    pct_ED_epic_smooth = zoo::rollapplyr(pct_ED_epic,3,mean, partial=T, na.rm=T),
    
    pct_ED_epic_smooth = if_else(is.nan(pct_ED_epic_smooth), NA, pct_ED_epic_smooth),
    
    
    ED_epic_scale = 100 * pct_ED_epic_smooth / max(pct_ED_epic_smooth , na.rm =
                                                     T),
    
    outcome_type = 'ED',
    domain = 'Respiratory infections',
    date_resolution = 'week',
    update_frequency = 'weekly',
    source = 'Epic Cosmos',
    url = 'https://www.epicresearch.org/',
    geo_strata = 'state',
    age_strata = 'age_scheme_1',
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
    Outcome_value1 = pct_ED_epic,
    Outcome_value2 = pct_ED_epic_smooth,
    Outcome_value3 = ED_epic_scale,
    Outcome_value4 = N_ED_type,
    Outcome_value5 = N_ED_epic_all_cause,
    age_level = Level
  ) %>%
  dplyr::select(
    "geography",
    "age_level",
    "date",
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
    "Outcome_value1",
    "Outcome_value2",
    "Outcome_value3" ,
    "Outcome_value4",
    "Outcome_value5"
  ) %>%
  mutate(
    outcome_label1 = 'Pct of ED visits (Epic)',
    outcome_label2 = 'Pct of ED visits (Epic, smoothed)',
    outcome_label3 = 'Pct of ED visits (Epic, smoothed and scaled)',
    outcome_label4 = 'Number of ED visits for the outcome, Epic',
    outcome_label5 = 'Number of ED visits for any outcome, Epic'
    
  )


write_parquet(epic_ed_combo,
              './Data/Plot Files/Cosmos ED/flu_rsv_covid_epic_cosmos_ed.parquet')

#test <- read_parquet( './Data/harmonized_epic_flu_rsv_covid.parquet') %>% collect()