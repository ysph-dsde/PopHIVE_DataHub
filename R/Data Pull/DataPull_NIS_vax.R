#library(parquetize )
#pediatric vaccine yptake
#https://data.cdc.gov/Child-Vaccinations/Vaccination-Coverage-among-Young-Children-0-35-Mon/fhky-rtsk/about_data

# csv_to_parquet('./Data/vax/Vaccination_Coverage_among_Young_Children__0-35_Months__20250204.csv',path_to_parquet ='./Data/vax/peds_vax.parquet')

vax_age <- read_parquet('https://github.com/ysph-dsde/PopHIVE_DataHub/raw/refs/heads/main/Data/Pulled%20Data/vax/peds_vax.parquet') %>%
  rename(birth_year = `Birth Year/Birth Cohort`, dim1=`Dimension Type`, age=Dimension,vax_uptake=`Estimate (%)`, samp_size_vax=`Sample Size`) %>%
  collect() %>%
  #filter(birth_year==2021 & dim1=='Age') %>%
  dplyr::select(Vaccine,Geography, Dose, dim1, vax_uptake,samp_size_vax, age,birth_year ) %>%
  filter(grepl('MMR',Vaccine)|grepl('Varicella',Vaccine)|  grepl('DTaP',Vaccine)|  grepl('Hep A',Vaccine)|  
     grepl('Hep B',Vaccine)| grepl('Hib',Vaccine)|  grepl('PCV',Vaccine) 
  ) %>%
  mutate(outcome_type='percent immunized',
         outcome_label1 = 'Immunization (NIS)',
         domain = 'Childhood Immunizations',
         date_resolution = 'year',
         update_frequency = 'yearly',
         source = 'CDC National Immunization Survey',
         url = 'https://data.cdc.gov/Child-Vaccinations/Vaccination-Coverage-among-Young-Children-0-35-Mon/fhky-rtsk/about_data',
         geo_strata = 'state',
         age_strata = 'age_level',
         race_strata = 'none',
         race_level = NA_character_,
         additional_strata1 = 'none',
         additional_strata_level = NA_character_,
         sex_strata = 'none',
         sex_level = NA_character_) 

write_parquet(vax_age, './Data/Plot Files/vax_age_nis.parquet')
     
     
##################Urbanicity:
# 
# vax_urban <- read_parquet('https://github.com/ysph-dsde/PopHIVE_DataHub/raw/refs/heads/main/Data/Pulled%20Data/vax/peds_vax.parquet')  %>%
#   rename(birth_year = `Birth Year/Birth Cohort`, dim1=`Dimension Type`, urban=Dimension,vax_uptake=`Estimate (%)`, samp_size_vax=`Sample Size`) %>%
#   filter(birth_year=='2016-2019' & dim1=='Urbanicity') %>%
#   filter(grepl('MMR',Vaccine)|grepl('Varicella',Vaccine)|  grepl('DTaP',Vaccine)|  grepl('Hep A',Vaccine)|  
#            grepl('Hep B',Vaccine)| grepl('Hib',Vaccine)|  grepl('PCV',Vaccine)) %>%
#            mutate(outcome_type='Immunizations',
#                   outcome_label1 = 'Immunization (NIS)',
#                   domain = 'Childhood Immunizations',
#                   date_resolution = 'year',
#                   update_frequency = 'yearly',
#                   source = 'CDC National Immunization Survey',
#                   url = 'https://data.cdc.gov/Child-Vaccinations/Vaccination-Coverage-among-Young-Children-0-35-Mon/fhky-rtsk/about_data',
#                   geo_strata = 'state',
#                   age_strata = 'none',
#                   race_strata = 'none',
#                   race_level = NA_character_,
#                   additional_strata1 = 'none',
#                   additional_strata_level = NA_character_,
#                   sex_strata = 'none',
#                   sex_level = NA_character_) %>%
#   collect() 
#   