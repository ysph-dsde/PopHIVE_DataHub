#https://data.cdc.gov/resource/ijqb-a7ye.csv


#Kindergarten vaccine uptake from SchoolVaxView.
#Surveys of schools; methodology and completeness vary by state
#https://data.cdc.gov/resource/ijqb-a7ye.csv

#a1 <- read.csv('https://data.cdc.gov/resource/ijqb-a7ye.csv')
#write_parquet(a1, './Data/Pulled Data/vax/school_vax_view.parquet' )


a1 <- read_parquet('./Data/Pulled Data/vax/school_vax_view.parquet')

#what is MMR (PAC) category?
vax1 <- a1 %>%
  #filter(!grepl('Exemption',dose)) %>%
  mutate( 
       vaccine = tolower(vaccine),
       vax = if_else(dose=='Any Exemption', 'full_exempt',
                        if_else(dose=='Medical Exemption', 'medical_exempt',
                                if_else(dose=='Non-Medical Exemption', 'personal_exempt',
                                
                                        vaccine
                                )) ),
          vax=if_else(vaccine == "dtp, dtap, or dt"  ,'dtap',
                  if_else(vaccine== "hepatitis b" ,'hep_b',
                          vax)),
          grade='Kindergarten') %>%
  rename(year=year_season,N=population_sample_size , value=coverage_estimate) %>%
  dplyr::select(year, geography, grade,N, vax,value, percent_surveyed)


 exemptions <- vax1 %>%
   filter(grepl('Exemption',vax)) 
   
write.csv(exemptions,'./Data/Plot Files/childhood_immunizations/exemption_state_kg_school_vax_view.csv')
write_parquet(exemptions,'./Data/Webslim/childhood_immunizations/exemption_state_kg_school_vax_view.parquet')


vax2 <- vax1 %>%
  filter(!grepl('exempt',vax)  ) %>%
  filter(!grepl('pac' ,vax)) 

write.csv(vax2,'./Data/Plot Files/childhood_immunizations/state_kg_school_vax_view.csv')
write_parquet(vax2,'./Data/Webslim/childhood_immunizations/state_kg_school_vax_view.parquet')


#View(vax2 %>% filter(geography=='Arizona'))
