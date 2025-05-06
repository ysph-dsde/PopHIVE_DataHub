##Take the previous 'plot files' format and slim down for the final
#Dataface website

library(tidyverse)
library(arrow)

url_files <- 'https://raw.githubusercontent.com/ysph-dsde/PopHIVE_DataHub/refs/heads/main/Data/Plot%20Files/'


#######################################
## RSV
#######################################

#NREVSS data

nrevss_rsv_ts <- read_csv(paste0(url_files,'NREVSS/rsv_ts_nrevss_test_rsv.csv')) %>%
  filter(!is.na(hhs_abbr)) %>%
  dplyr::select(hhs_abbr, MMWRweek, MMWRyear,pcr_detections) %>%
  rename(geography=hhs_abbr, week=MMWRweek, year=MMWRyear,value=pcr_detections)

write_parquet(nrevss_rsv_ts,'./Data/Webslim/respiratory_diseases/rsv/positive_tests.parquet')

##RSV all indicators, all ages

rsv_all_indicators_state <- read_csv(paste0(url_files,'Comparisons/rsv_combined_all_outcomes_state.csv')) %>%
  filter(outcome_label1 != 'Google Searches 1') %>% #only keep definition #2 for google searches
  dplyr::select(geography, date, source, suppressed_flag,Outcome_value1,outcome_3m) %>%  #note this is RAW data; needs 3 week ave and scaling for plot
  rename(value=Outcome_value1, value_smooth=outcome_3m) %>%
  mutate( source = if_else(source=="Epic Cosmos" , "Epic Cosmos, ED",source ),
          source = if_else(source=="CDC RSV-NET (RespNet)" , "CDC RSV-NET",source ))

write_parquet(rsv_all_indicators_state,'./Data/Webslim/respiratory_diseases/rsv/overall_trends.parquet')


##RSV Cosmos age and RSV-Net by age
epic_ed_combo_rsv <- read_csv(paste0(url_files,'Cosmos%20ED/rsv_flu_covid_epic_cosmos_age_state.csv')) %>%
  mutate(source= "Epic Cosmos (ED)") %>%
  filter(outcome_name=='RSV') %>%
    dplyr::select(date, geography, age_level, source,suppressed_flag, Outcome_value1,Outcome_value2) %>%
  rename(value=Outcome_value1, value_smooth=Outcome_value2)

rsvnet_age <- read_csv(paste0(url_files,'RESP-NET%20Programs/rsv_hosp_age_respnet.csv')) %>%
  mutate(source= "CDC RSV-NET (Hospitalization)",
         suppressed_flag=0) %>%
  dplyr::select(date, state, Level, source,suppressed_flag, hosp_rate,hosp_rate_3m) %>%
  rename(value=hosp_rate,
         value_smooth=hosp_rate_3m,
         geography=state,
         age=Level)

age_trends_rsv <- bind_rows(epic_ed_combo_rsv,rsvnet_age)

write_parquet(age_trends_rsv,'./Data/Webslim/respiratory_diseases/rsv/trends_by_age.parquet')

##RSV by county and week

rsv_county <- read_csv(paste0(url_files,'Cosmos%20ED/rsv_flu_covid_county_filled_map_nssp.csv')) %>%
  dplyr::select(fips,week_end,percent_visits_rsv)

write_parquet(rsv_county,'./Data/Webslim/respiratory_diseases/rsv/ed_visits_by_county.parquet')


######################################
##INFLUENZA
######################################

##flu all indicators, all ages

flu_all_indicators_state <- read_csv(paste0(url_files,'Comparisons/flu_combined_all_outcomes_state.csv')) %>%
  filter(outcome_label1 != 'Google Searches 1') %>% #only keep definition #2 for google searches
  dplyr::select(geography, date, source,suppressed_flag, Outcome_value1,Outcome_value2) %>%  #note this is RAW data; needs 3 week ave and scaling for plot
  rename(value=Outcome_value1, value_smooth=Outcome_value2) %>%
  mutate( source = if_else(source=="Epic Cosmos" , "Epic Cosmos, ED",source ))

write_parquet(flu_all_indicators_state,'./Data/Webslim/respiratory_diseases/influenza/overall_trends.parquet')


##flu Cosmos age 
epic_ed_combo_flu <- read_csv(paste0(url_files,'Cosmos%20ED/rsv_flu_covid_epic_cosmos_age_state.csv')) %>%
  mutate(source= "Epic Cosmos (ED)") %>%
  filter(outcome_name=='FLU') %>%
  dplyr::select(date, geography, age_level, source,suppressed_flag, Outcome_value1,Outcome_value2) %>%
  rename(value=Outcome_value1, value_smooth=Outcome_value2)

write_parquet(epic_ed_combo_flu,'./Data/Webslim/respiratory_diseases/influenza/trends_by_age.parquet')


##flu by county and week

flu_county <- read_csv(paste0(url_files,'Cosmos%20ED/rsv_flu_covid_county_filled_map_nssp.csv')) %>%
  dplyr::select(fips,week_end,percent_visits_flu)

write_parquet(flu_county,'./Data/Webslim/respiratory_diseases/influenza/ed_visits_by_county.parquet')


######################################
##COVID
######################################

##covid all indicators, all ages

covid_all_indicators_state <- read_csv(paste0(url_files,'Comparisons/covid_combined_all_outcomes_state.csv')) %>%
  filter(outcome_label1 != 'Google Searches 1') %>% #only keep definition #2 for google searches
  dplyr::select(geography, date, source,suppressed_flag, Outcome_value1,Outcome_value2) %>%  #note this is RAW data; needs 3 week ave and scaling for plot
  rename(value=Outcome_value1, value_smooth=Outcome_value2) %>%
  mutate( source = if_else(source=="Epic Cosmos" , "Epic Cosmos, ED",source ))

write_parquet(covid_all_indicators_state,'./Data/Webslim/respiratory_diseases/covid/overall_trends.parquet')


##flu Cosmos age 
epic_ed_combo_covid <- read_csv(paste0(url_files,'Cosmos%20ED/rsv_flu_covid_epic_cosmos_age_state.csv')) %>%
  mutate(source= "Epic Cosmos (ED)") %>%
  filter(outcome_name=='COVID') %>%
  dplyr::select(date, geography, age_level, source,suppressed_flag, Outcome_value1,Outcome_value2) %>%
  rename(value=Outcome_value1, value_smooth=Outcome_value2)

write_parquet(epic_ed_combo_covid,'./Data/Webslim/respiratory_diseases/covid/trends_by_age.parquet')


##flu by county and week

covid_county <- read_csv(paste0(url_files,'Cosmos%20ED/rsv_flu_covid_county_filled_map_nssp.csv')) %>%
  dplyr::select(fips,week_end,percent_visits_covid)

write_parquet(covid_county,'./Data/Webslim/respiratory_diseases/covid/ed_visits_by_county.parquet')


#################################################################
## Pneumococcus
################################################################

#IPD serotype trends
ipd_serotype_age <- read_csv(paste0(url_files,'./pneumococcus/ipd_serotype_age_year.csv')) %>%
  dplyr::select(st, agec2, year, N_IPD) %>%
  rename(serotype=st,
         age=agec2,
         value=N_IPD)

write_parquet(ipd_serotype_age,'./Data/Webslim/respiratory_diseases/pneumococcus/serotype_trends.parquet')


#Geographic variation (based on genomic data)

ipd_serotype_state <- read_csv(paste0(url_files,'./pneumococcus/ipd_serotype_state_pct.csv')) %>%
  dplyr::select(sero, State, pct) %>%
  rename(serotype=sero, geography=State, value=pct)

write_parquet(ipd_serotype_state,'./Data/Webslim/respiratory_diseases/pneumococcus/by_geography.parquet')


##comparison of IPD and pneumonia

ipd1 <- readRDS(url('https://github.com/ysph-dsde/PopHIVE_DataHub/raw/refs/heads/main/Data/Pulled%20Data/pneumococcus/ABCs_st_1998_2023.rds')) %>%
  rename(agec = "Age.Group..years.",
         year=Year,
         st=IPD.Serotype,
         N_IPD = Frequency.Count) %>%
  mutate( st= if_else(st=='16','16F', st),
          st = if_else(st %in% c('15B','15C'), '15BC',st),
          if_else(st %in% c('6A','6C'), '6AC',st)
  ) %>%
  filter(year %in% c(2019,2020) & agec %in% c('Age 50-64','Age 65+')) %>%
  group_by(st) %>%
  summarize( N_IPD= sum(N_IPD))

uad <- read_csv('https://github.com/ysph-dsde/PopHIVE_DataHub/raw/refs/heads/main/Data/Pulled%20Data/pneumococcus/ramirez_ofid_2025_ofae727.csv') %>%
  mutate(N_SSUAD= over65 + a50_64_with_indication + a50_64_no_indication ) %>%
  full_join(ipd1, by='st') %>%
  filter(!is.na(N_SSUAD) & !is.na(N_IPD)) %>%
  dplyr::select(st, N_SSUAD, N_IPD) %>%
  rename(serotype=st, ipd=N_IPD, pneumonia=N_SSUAD )

write_parquet(uad,'./Data/Webslim/respiratory_diseases/pneumococcus/comparison.parquet')



####################################################
##Immunization
####################################################
vax_age <- read_parquet('https://github.com/ysph-dsde/PopHIVE_DataHub/raw/refs/heads/main/Data/Plot%20Files/vax_age_nis.parquet') %>%
  filter(Geography %in% c(state.name,'District of Columbia', 'United States') & birth_year=='2021' & dim1=='Age') %>%
  mutate(vax_order=as.numeric(as.factor(Vaccine)), Vaccine_dose=as.factor(paste(Vaccine,Dose)) ,
         Vaccine_dose = gsub('NA','',Vaccine_dose)) %>%
  dplyr::select(Geography,birth_year,age,Vaccine_dose, Outcome_value1) %>%
  rename(geography=Geography,vaccine=Vaccine_dose, value=Outcome_value1)

write_parquet(vax_age,'./Data/Webslim/childhood_immunizations/overall_rates.parquet')

vax_urban <- read_parquet('https://github.com/ysph-dsde/PopHIVE_DataHub/raw/refs/heads/main/Data/Pulled%20Data/vax/peds_vax.parquet')  %>%
  rename(birth_year = `Birth Year/Birth Cohort`, dim1=`Dimension Type`, urban=Dimension,vax_uptake=`Estimate (%)`, samp_size_vax=`Sample Size`) %>%
  collect() %>%
  mutate( Vaccine_dose=as.factor(paste(Vaccine,Dose))) %>%
  filter(birth_year=='2016-2019' & dim1=='Urbanicity') %>%
  dplyr::select(Vaccine_dose,Geography, Dose, dim1, vax_uptake,samp_size_vax, urban,birth_year) %>%
  filter( Geography %in% c(state.name,'District of Columbia', 'United States') 
            )  %>%
  mutate(urban= factor(urban, levels= c("Living In a Non-MSA", "Living In a MSA Non-Principal City","Living In a MSA Principal City"),labels=c('Rural','Smaller City', 'Larger City') )) %>%
  dplyr::select(Geography,urban,birth_year,Vaccine_dose,vax_uptake) %>%
  rename(geography=Geography,vaccine=Vaccine_dose, value=vax_uptake)

write_parquet(vax_urban,'./Data/Webslim/childhood_immunizations/rates_by_urbanicity.parquet')

vax_insurance <- read_parquet('https://github.com/ysph-dsde/PopHIVE_DataHub/raw/refs/heads/main/Data/Pulled%20Data/vax/peds_vax.parquet') %>%
  rename(birth_year = `Birth Year/Birth Cohort`, dim1=`Dimension Type`, insurance=Dimension,vax_uptake=`Estimate (%)`, samp_size_vax=`Sample Size`) %>%
  collect() %>%
  mutate( Vaccine_dose=as.factor(paste(Vaccine,Dose))) %>%
  filter(birth_year=='2016-2019' & dim1=='Insurance Coverage') %>%
  dplyr::select(Vaccine_dose,Geography, Dose, dim1, vax_uptake,samp_size_vax, insurance,birth_year) %>%
  filter( Geography %in% c(state.name,'District of Columbia', 'United States')) %>%
  mutate(insurance= factor(insurance, levels= c("Uninsured", "Any Medicaid","Private Insurance Only","Other"),labels=c('Uninsured','Medicaid', 'Private','Other') )) %>%
  dplyr::select(Geography,insurance,birth_year,Vaccine_dose,vax_uptake) %>%
    rename(geography=Geography,vaccine=Vaccine_dose, value=vax_uptake)

write_parquet(vax_insurance,'./Data/Webslim/childhood_immunizations/rates_by_insurance.parquet')

vax_epic <- read_parquet('https://github.com/ysph-dsde/PopHIVE_DataHub/raw/refs/heads/main/Data/Plot%20Files/vax_age_cosmos.parquet') %>%
  filter( geography %in% c(state.name,'District of Columbia', 'United States')) %>%
  dplyr::select(geography,age_level,Outcome_value1) %>%
  rename(value=Outcome_value1, age=age_level)
  
write_parquet(vax_epic,'./Data/Webslim/childhood_immunizations/mmr_rates_epic.parquet')


#################################################
## Chronic diseases
#################################################
diab_obesity <- read_parquet('https://github.com/ysph-dsde/PopHIVE_DataHub/raw/refs/heads/main/Data/Plot%20Files/Cosmos%20ED/diabetes_obesity.parquet') %>%
  dplyr::select(geography,age_level,outcome_name,Outcome_value1) %>%
  rename(value=Outcome_value1, age=age_level) %>%
  filter(!is.na(age)) %>%
  filter( geography %in% c(state.name,'District of Columbia', 'United States')) 
  
write_parquet(diab_obesity,'./Data/Webslim/chronic_diseases/prevalence_by_geography.parquet')

