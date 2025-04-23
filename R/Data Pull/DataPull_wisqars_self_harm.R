
#######################################
###WISQARS self harm
#######################################


wis1 <- read_csv('./Data/Pulled Data/wisqars_self_harm/Raw/2025_04_15_self_harm.csv') %>%
  mutate(Outcome_value1=as.numeric(`Crude Rate`), age_level=`Age Group`) %>%
  dplyr::select(age_level,Sex, Year,Outcome_value1) %>%
  mutate(outcome_type='Self harm',
         outcome_label1 = 'Self-harm',
         domain = 'Injury and overdose',
         date_resolution = 'year',
         update_frequency = 'yearly',
         source = 'WISQARS (CDC)',
         url = 'https://wisqars.cdc.gov/',
         geo_strata = 'none',
         age_strata = 'Age.Group',
         race_strata = 'none',
         race_level = NA_character_,
         additional_strata1 = 'Sex',
         additional_strata_level = Sex,
         sex_strata = 'none',
         sex_level = NA_character_) 

write.csv(wis1,'./Data/Plot Files/wisqars/wisqars_self_harm.csv')
