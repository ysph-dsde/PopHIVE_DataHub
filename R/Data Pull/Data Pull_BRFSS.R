#a1 <- read.socrata('https://data.cdc.gov/resource/dttw-5yxu.csv')
#write_parquet(a1, './Data/Pulled Data/BRFSS/brfss_prevalence.parquet' )

'Have you ever been told by a doctor that you have diabetes?'

diabetes <- open_dataset('./Data/Pulled Data/BRFSS/brfss_prevalence.parquet') %>%
  filter(class== "Chronic Health Indicators" & topic=='Diabetes') %>%
  filter(break_out_category %in% c( "Age Group",'Overall') & response=='Yes') %>%
  dplyr::select(locationdesc,year, break_out,  data_value, sample_size) %>%
  collect() %>%
  mutate(outcome_name='Diabetes') %>%
  rename(age=break_out, value=data_value, geography=locationdesc) %>%
  mutate( age = if_else(age=='Overall', 'Total', paste(age, 'Years')),
          geography = if_else(geography=='All States and DC (median) **', 'United States', geography)) %>%
  filter(geography !='All States, DC and Territories (median) **')

#########################################
#Weight classification by Body Mass Index (BMI) (variable calculated from one or more BRFSS questions)
#Obese (BMI 30.0 - 99.8)
#########################################

obesity <- open_dataset('./Data/Pulled Data/BRFSS/brfss_prevalence.parquet') %>%
  filter(class== "Overweight and Obesity (BMI)" & topic=='BMI Categories') %>%
  filter(break_out_category %in% c( "Age Group",'Overall') & grepl('Obese', response)) %>%
  dplyr::select(locationdesc,year, break_out,  data_value, sample_size) %>%
  collect() %>%
  mutate(outcome_name='Obesity') %>%
  rename(age=break_out, value=data_value, geography=locationdesc) %>%
  mutate( age = if_else(age=='Overall', 'Total', paste(age, 'Years')),
          geography = if_else(geography=='All States and DC (median) **', 'United States', geography)) %>%
  filter(geography != 'All States, DC and Territories (median) **')

combined <-   bind_rows(obesity, diabetes)

write_parquet(combined,'./Data/Webslim/chronic_diseases/brfss_prevalence_by_geography.parquet')



epic_compare <-read_parquet('./Data/Webslim/chronic_diseases/prevalence_by_geography.parquet') %>%
  rename(value_epic=value, epic_pct_captured=pct_captured) %>%
  left_join(combined, by=c('geography', 'age', 'outcome_name')) %>%
  filter(year==2023)

write_parquet(epic_compare,'./Data/Webslim/chronic_diseases/brfss_cosmos_prevalence_compared.parquet')



