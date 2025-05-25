#https://data.cdc.gov/resource/ijqb-a7ye.csv


#Kindergarten vaccine uptake from SchoolVaxView.
#Surveys of schools; methodology and completeness vary by state
#https://data.cdc.gov/resource/ijqb-a7ye.csv

#a1 <- read.csv('https://data.cdc.gov/resource/ijqb-a7ye.csv')
#write_parquet(a1, './Data/Pulled Data/vax/school_vax_view.parquet' )


a1 <- read_parquet('./Data/Pulled Data/vax/school_vax_view.parquet')

exemptions <- a1 %>%
  filter(grepl('Exemption',dose)) %>%
  mutate( vax = if_else(dose=='Any Exemption', 'full_exempt',
                        if_else(dose=='Medical Exemption', 'medical_exempt',
                                if_else(dose=='Non-Medical Exemption', 'personal_exempt',
                                        NA_character_
                        ))),
          grade='Kindergarten') %>%
  rename(year=year_season,N=population_sample_size , value=coverage_estimate) %>%
  dplyr::select(year, geography, grade,N, vax,value, percent_surveyed)

write.csv(exemptions,'./Data/Plot Files/childhood_immunizations/exemption_state_kg_school_vax_view.csv')
write_parquet(exemptions,'./Data/Webslim/childhood_immunizations/exemption_state_kg_school_vax_view.parquet')
