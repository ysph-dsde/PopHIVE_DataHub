
source('./R/Epic Cleaning/epic_age_import.R')

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
  epic_age_import(ds_name = paste0('./Data/Pulled Data/Cosmos ED/flu/',epic_ed_flu_latest_file))%>%
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
    
    suppressed_flag = if_else(N_ED_type==4.9999, 1,0),
    
    pct_ED_epic = N_ED_type / N_ED_epic_all_cause * 100,
    
    pct_ED_epic = if_else(N_ED_type<5, min(pct_ED_epic, na.rm=T)/2, pct_ED_epic ), #if suppressed, half of posiivity
    
    pct_ED_epic_smooth = zoo::rollapplyr(pct_ED_epic,3,mean, partial=T, na.rm=T),
    
    pct_ED_epic_smooth = if_else(is.nan(pct_ED_epic_smooth), NA, pct_ED_epic_smooth),
    
    pct_ED_epic_smooth = pct_ED_epic_smooth - min(pct_ED_epic_smooth, na.rm=T),
    
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
    'suppressed_flag',
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


#test <- read_parquet( './Data/Plot Files/Cosmos ED/flu_rsv_covid_epic_cosmos_ed.parquet') %>% collect()

###############################################################################
####Vaccine data
###############################################################################
source('./R/Epic Cleaning/epic_vax_age.R')

vax1 <- epic_vax_import('./Data/Pulled Data/Cosmos ED/Immunizations/mmr_age_state_2025_05_07.csv')%>%
  rename(age_level=Level,
         Outcome_value1=pct_vax_epic,
         Outcome_value2 = N_patients) %>%
  mutate(
    suppressed_flag = if_else(is.na(Outcome_value1),1,0),
   
    outcome_type='Percent immunized',
         outcome_label1 = 'Immunization (Epic Cosmos)',
         outcome_label2 = 'N Patients (Epic Cosmos)',
         domain = 'Childhood Immunizations',
         date_resolution = 'year',
         update_frequency = 'yearly',
         source = 'Epic Cosmos',
         url = 'https://www.epicresearch.org/',
         geo_strata = 'state',
         age_strata = 'age_level',
         race_strata = 'none',
         race_level = NA_character_,
         additional_strata1 = 'none',
         additional_strata_level = NA_character_,
         sex_strata = 'none',
         sex_level = NA_character_) %>%
  filter(!(age_level %in% c("<1 Years","9+ years")))
write_parquet(vax1, './Data/Plot Files/vax_age_cosmos.parquet')



#########################
#Chronic
###

source('./R/Epic Cleaning/epic_chronic_import.R')

#EPIC ED all cause
epic_ed_all_latest_file = datetimeStamp( storeIn = 'Cosmos ED/Chronic Disease/Raw')$`Report Relative to Date` %>%
  filter(Delta==min(Delta)) %>%
  pull(filePath) 

epic_diabetes <- epic_import_chronic(ds_name = paste0('./Data/Pulled Data/Cosmos ED/Chronic Disease/Raw/',epic_ed_all_latest_file)) %>%
  dplyr::select(geography, Level, pct_diabetes) %>%
  mutate(
    suppressed_flag = if_else(is.na(pct_diabetes),1,0),
    outcome_name='diabetes',
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
    Outcome_value1 = pct_diabetes,
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

epic_obesity <- epic_import_chronic(ds_name = paste0('./Data/Pulled Data/Cosmos ED/Chronic Disease/Raw/',epic_ed_all_latest_file)) %>%
  dplyr::select(geography, Level, pct_obesity) %>%
  mutate(
    suppressed_flag = if_else(is.na(pct_obesity),1,0),
    outcome_name='obesity',
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
    Outcome_value1 = pct_obesity,
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

epic_patients <- epic_import_chronic(ds_name = paste0('./Data/Pulled Data/Cosmos ED/Chronic Disease/Raw/',epic_ed_all_latest_file)) %>%
  dplyr::select(geography, Level, Number_Patients) %>%
  mutate(
    suppressed_flag = if_else(is.na(Number_Patients),1,0),
    outcome_name='N_patients',
    outcome_type = 'Count',
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
    Outcome_value1 = Number_Patients,
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
    outcome_label1 = 'Number Base Patients',
    Outcome_value1 = as.numeric(Outcome_value1)
  )

epic_chronic <- bind_rows(epic_obesity,epic_diabetes,epic_patients)

write_parquet(epic_chronic,
              './Data/Plot Files/Cosmos ED/diabetes_obesity.parquet')
write_csv(epic_chronic,
              './Data/Plot Files/Cosmos ED/diabetes_obesity.csv')

#Dataset for county and state level obesity, diabetes and percent of population captured
source('./R/Epic Cleaning/county_state_coverage.R')

##################################
# Viral Testing
##################################
source('./R/Epic Cleaning/epic_rsv_testing_import.R')

epic_rsv_testing <- epic_viral_testing(ds_name = './Data/Pulled Data/Cosmos ED/rsv_testing/2025_06_04_Pneumonia_with_RSV_test_US_Month_Age_2018-2025.csv') %>%
  dplyr::select( Level, pct_tested_rsv, N_J12_J18, date) %>%
  mutate(
    suppressed_flag = if_else(is.na(pct_tested_rsv),1,0),
    outcome_name='pct_tested_rsv',
    outcome_type = 'testing',
    domain = 'Respiratory infections',
    date_resolution = 'monthly',
    update_frequency = 'annual',
    source = 'Epic Cosmos',
    url = 'https://www.epicresearch.org/',
    geo_strata = 'none',
    age_strata = 'age_scheme_2',
    race_strata = 'none',
    race_level = NA_character_,
    additional_strata1 = 'none',
    additional_strata_level = NA_character_,
    sex_strata = 'none',
    sex_level = NA_character_
    
  )  %>%
  ungroup() %>%
  rename(
    Outcome_value1 = pct_tested_rsv,
    age_level = Level
  ) %>%
  dplyr::select(
    'date',
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
    "Outcome_value1",
    'N_J12_J18',
    'suppressed_flag'
  ) %>%
  mutate(
    outcome_label1 = 'testing_pct'
  )

write_csv(epic_rsv_testing,
          './Data/Plot Files/Cosmos ED/rsv_testing_freq.csv')


# epic_rsv_testing %>%
#   filter(!is.na(age_level)) %>%
# ggplot() +
#   geom_line(aes(x=date, y=Outcome_value1))+
#   facet_wrap(~age_level, scales='free')+
#   ylim(0,NA)+
#   theme_minimal()+
#   ggtitle('% of J12-J18 receiving a test for RSV')+
#   ylab('% Tested')
