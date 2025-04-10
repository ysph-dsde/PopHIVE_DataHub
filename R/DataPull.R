library(tidyverse)
library(RSocrata)
library(parquetize)
library(arrow)
library(MMWRweek)

source('./R/archiving_functions.R') #function for archiving
source('./R/epic_age_import.R')
source('./R/harmonize_epic.R')

#Creates a time/date stamped parquet file in the folder indicated in runIfExpired. 
#if a file has been downloaded within past week (24*7 hours), 
#it just reads in latest file, otherwise it downloads fresh copy
#Check that formatting is consistent between vintages. (1) check column names (2) check variable formats
#compare newest data to previous dataset


#######################################
###National syndromic surveillance data (CDC) county*week (no age stratification)
#######################################
url_nssp <- "https://data.cdc.gov/resource/rdmq-nq56.csv"

cdc_nssp_rsv_flu_covid_ed1 <- runIfExpired(source='nssp_ed1',storeIn='Raw',  basepath='./Data/Archive',
                            f=~ read.socrata(url_nssp),tolerance=(24*7)
)

nssp_harmonized_rsv <- cdc_nssp_rsv_flu_covid_ed1 %>%
  filter(county=='All'  ) %>%
  rename(state=geography, date='week_end') %>%
  dplyr::select(state, date, percent_visits_rsv) %>%
  collect() %>%
  as.data.frame() %>%
  rename(Outcome_value1=percent_visits_rsv,
         geography=state) %>%
  mutate(outcome_type='ED',
         outcome_label1 = 'Pct of ED visits',
         domain = 'Respiratory infections (RSV)',
         date_resolution = 'week',
         update_frequency = 'weekly',
         source = 'CDC NSSP',
         url = 'https://healthdata.gov/dataset/NSSP-Emergency-Department-Visit-Trajectories-by-St/hr4c-e7p6/',
         geo_strata = 'state',
         age_strata = 'none',
         race_strata = 'none',
         race_level = NA_character_,
         additional_strata1 = 'none',
         additional_strata_level = NA_character_,
         sex_strata = 'none',
         sex_level = NA_character_)

nssp_harmonized_flu <- cdc_nssp_rsv_flu_covid_ed1 %>%
  filter(county=='All'  ) %>%
  rename(state=geography, date='week_end') %>%
  dplyr::select(state, date, percent_visits_influenza) %>%
  collect() %>%
  as.data.frame() %>%
  rename(Outcome_value1=percent_visits_influenza,
         geography=state) %>%
  mutate(outcome_type='ED',
         outcome_label1 = 'Pct of ED visits',
         domain = 'Respiratory infections (flu)',
         date_resolution = 'week',
         update_frequency = 'weekly',
         source = 'CDC NSSP',
         url = 'https://healthdata.gov/dataset/NSSP-Emergency-Department-Visit-Trajectories-by-St/hr4c-e7p6/',
         geo_strata = 'state',
         age_strata = 'none',
         race_strata = 'none',
         race_level = NA_character_,
         additional_strata1 = 'none',
         additional_strata_level = NA_character_,
         sex_strata = 'none',
         sex_level = NA_character_)

nssp_harmonized_covid <- cdc_nssp_rsv_flu_covid_ed1 %>%
  filter(county=='All'  ) %>%
  rename(state=geography, date='week_end') %>%
  dplyr::select(state, date, percent_visits_covid) %>%
  collect() %>%
  as.data.frame() %>%
  rename(Outcome_value1=percent_visits_covid,
         geography=state) %>%
  mutate(outcome_type='ED',
         outcome_label1 = 'Pct of ED visits',
         domain = 'Respiratory infections (flu)',
         date_resolution = 'week',
         update_frequency = 'weekly',
         source = 'CDC NSSP',
         url = 'https://healthdata.gov/dataset/NSSP-Emergency-Department-Visit-Trajectories-by-St/hr4c-e7p6/',
         geo_strata = 'state',
         age_strata = 'none',
         race_strata = 'none',
         race_level = NA_character_,
         additional_strata1 = 'none',
         additional_strata_level = NA_character_,
         sex_strata = 'none',
         sex_level = NA_character_)

#######################################
###RespNet (CDC) Flu, COVID RSV, by week, state, race, etc. For some reason age*state missing for RSV
#######################################
url_resp_net <- "https://data.cdc.gov/resource/kvib-3txy.csv"

cdc_respnet <- runIfExpired(source='respnet',storeIn='Raw',  basepath='./Data/Archive',
                            ~ read.socrata(url_resp_net),tolerance=(24*7)
)

h1_harmonized_rsv <- cdc_respnet %>%
  filter( site != 'Overall' & 
            surveillance_network=='RSV-NET' & type=='Unadjusted Rate' & age_group=='Overall' &
            sex=='Overall' & race_ethnicity=='Overall') %>%
  rename(state=site, hosp_rate=weekly_rate, date=X_weekenddate) %>%
  mutate(date=as.Date(substr(date,1,10)) ) %>%
  dplyr::select(state, date, hosp_rate) %>%
  as.data.frame() %>% 
  rename(Outcome_value1=hosp_rate,
         geography=state) %>%
  mutate(outcome_type='Inpatient Hospitalization',
         outcome_label1 = 'Hospitalization Rate',
         domain = 'Respiratory infections',
         date_resolution = 'week',
         update_frequency = 'weekly',
         source = 'CDC RSV-NET (RespNet)',
         url = 'https://data.cdc.gov/resource/kvib-3txy.csv',
         geo_strata = 'state',
         age_strata = 'none',
         race_strata = 'none',
         race_level = NA_character_,
         additional_strata1 = 'none',
         additional_strata_level = NA_character_,
         sex_strata = 'none',
         sex_level = NA_character_)

h1_harmonized_flu <- cdc_respnet %>%
  filter( site != 'Overall' & 
            surveillance_network=='FluSurv-NET' & type=='Unadjusted Rate' & age_group=='Overall' &
            sex=='Overall' & race_ethnicity=='Overall') %>%
  rename(state=site, hosp_rate=weekly_rate, date=X_weekenddate) %>%
  mutate(date=as.Date(substr(date,1,10)) ) %>%
  dplyr::select(state, date, hosp_rate) %>%
  as.data.frame() %>% 
  rename(Outcome_value1=hosp_rate,
         geography=state) %>%
  mutate(outcome_type='Inpatient Hospitalization',
         outcome_label1 = 'Hospitalization Rate',
         domain = 'Respiratory infections',
         date_resolution = 'week',
         update_frequency = 'weekly',
         source = 'CDC FluSurv-NET',
         url = 'https://data.cdc.gov/resource/kvib-3txy.csv',
         geo_strata = 'state',
         age_strata = 'none',
         race_strata = 'none',
         race_level = NA_character_,
         additional_strata1 = 'none',
         additional_strata_level = NA_character_,
         sex_strata = 'none',
         sex_level = NA_character_)

h1_harmonized_covid <- cdc_respnet %>%
  filter( site != 'Overall' & 
            surveillance_network=='COVID-NET' & type=='Unadjusted Rate' & age_group=='Overall' &
            sex=='Overall' & race_ethnicity=='Overall') %>%
  rename(state=site, hosp_rate=weekly_rate, date=X_weekenddate) %>%
  mutate(date=as.Date(substr(date,1,10)) ) %>%
  dplyr::select(state, date, hosp_rate) %>%
  as.data.frame() %>% 
  rename(Outcome_value1=hosp_rate,
         geography=state) %>%
  mutate(outcome_type='Inpatient Hospitalization',
         outcome_label1 = 'Hospitalization Rate',
         domain = 'Respiratory infections',
         date_resolution = 'week',
         update_frequency = 'weekly',
         source = 'CDC COVID-NET',
         url = 'https://data.cdc.gov/resource/kvib-3txy.csv',
         geo_strata = 'state',
         age_strata = 'none',
         race_strata = 'none',
         race_level = NA_character_,
         additional_strata1 = 'none',
         additional_strata_level = NA_character_,
         sex_strata = 'none',
         sex_level = NA_character_)

#######################################
###RSVNet (CDC) by week, state, age, etc
#######################################
url_rsv_net <- "https://data.cdc.gov/resource/29hc-w46k.csv"

cdc_rsvnet <- runIfExpired(source='rsvnet',storeIn='Raw',  basepath='./Data/Archive',
                           ~ read.socrata(url_rsv_net),tolerance=(24*7)
)


h1.age.rsv <- cdc_rsvnet %>%
  filter(state!="RSV-NET" & sex=='All' & race=='All' & type=='Crude Rate') %>%
  rename( hosp_rate=rate, date=week_ending_date, Level=age_category) %>%
  filter(Level %in% c('1-4 years', '0-<1 year','5-17 years', '18-49 years' ,
                      "≥65 years" ,"50-64 years" )) %>%
  mutate( Level = if_else(Level=='0-<1 year',"<1 Years",
                          if_else( Level=='1-4 years', "1-4 Years",
                                   if_else(Level=="5-17 years" ,"5-17 Years",
                                           if_else(Level=="18-49 years" ,"18-49 Years",
                                                   if_else(Level=="50-64 years" ,"50-64 Years",
                                                           if_else(Level=="≥65 years",'65+ Years', 'other'               
                                                           ))))))
  ) %>%
  dplyr::select(state, date, hosp_rate, Level) %>%
  ungroup() %>%
  filter( date >=as.Date('2023-07-01')) %>%
  arrange(state, Level, date) %>%
  group_by(state, Level) %>%
  mutate(hosp_rate_3m=zoo::rollapplyr(hosp_rate,3,mean, partial=T, na.rm=T),
         hosp_rate_3m = if_else(is.nan(hosp_rate_3m), NA, hosp_rate_3m),
         
         scale_age=hosp_rate_3m/max(hosp_rate_3m, na.rm=T )*100,
         
  ) %>%
  as.data.frame()

write.csv(h1.age.rsv,'./Data/plot_files/rsv_hosp_age_respnet.csv')



#######################################
###Google searches for RSV vaccination
#######################################
g_states <- paste('US',state.abb,sep='-')

url2 <- "https://github.com/DISSC-yale/gtrends_collection/raw/refs/heads/main/data/term=%252Fg%252F11j30ybfx6/part-0.parquet" #rsv vaccination category

temp_file2 <- tempfile(fileext = ".parquet")
download.file(url2, temp_file2, mode = "wb")

google_rsv_vax <- runIfExpired(source='google_rsv_vax', storeIn='Raw',  basepath='./Data/Archive',
                               ~ read_parquet(temp_file2),
                               tolerance=(24*7)
)

g1_vax_state <- google_rsv_vax %>%
  filter( location %in% g_states ) %>%
    mutate(date=as.Date(date),
         date = as.Date(ceiling_date(date, 'week'))-1,
         stateabb= gsub('US-','', location),
         state=state.name[match(stateabb,state.abb)],
         value=round(value,2)) %>%
  rename(search_volume_vax=value) %>%
  dplyr::select(state, date, search_volume_vax) %>%
  distinct() %>%
  filter(date>='2014-01-01') %>%
  group_by(state,date) %>%
  summarize(search_volume_vax=mean(search_volume_vax, na.rm=T))

#######################################
###Google searches for 'RSV' 
#######################################

url1 <- "https://github.com/DISSC-yale/gtrends_collection/raw/refs/heads/main/data/term=rsv/part-0.parquet"
temp_file1 <- tempfile(fileext = ".parquet")
download.file(url1, temp_file1, mode = "wb")

google_rsv <- runIfExpired(source='google_rsv',storeIn='Raw',  basepath='./Data/Archive',
                           ~ read_parquet(temp_file1),
                           tolerance=(24*7)
)

google_merged_rsv = google_rsv %>%
  filter(location %in% g_states) %>%
  mutate(date=as.Date(date),
         date = as.Date(ceiling_date(date, 'week'))-1,
         stateabb= gsub('US-','', location),
         state=state.name[match(stateabb,state.abb)],
         value=round(value,2)) %>%
  rename(search_volume=value) %>%
  dplyr::select(state, date, search_volume) %>%
  group_by(state,date) %>%
  summarize(search_volume=mean(search_volume, na.rm=T)) %>%
  filter(date>='2014-01-01') %>%
  full_join(g1_vax_state, by=c('state', 'date') ) %>%
  mutate(month=month(date),
         season = if_else(month>=7 & month <=10,1,0),
         rsv_novax = search_volume - search_volume_vax ,
         rsv_novax = rsv_novax / max(rsv_novax, na.rm=T),
         rsv_novax2 = search_volume - season*(4.41-1.69)*search_volume_vax - (1-season)*3.41*search_volume_vax,  #2.655 based on the regression below
         rsv_novax2 = if_else(rsv_novax2<0,0,rsv_novax2),
         rsv_novax2 = rsv_novax2 / max(rsv_novax2, na.rm=T),
         search_volume = search_volume/ max(search_volume, na.rm=T),
         search_volume_vax = search_volume_vax/ max(search_volume_vax, na.rm=T)
  ) 
  
g1_state_harmonized_v1 <- google_merged_rsv %>%
  rename(Outcome_value1=rsv_novax,
         geography=state) %>%
  mutate(outcome_type='Google Searches',
         outcome_label1 = 'Google Searches 1',
         domain = 'Respiratory infections',
         date_resolution = 'week',
         update_frequency = 'weekly',
         source = 'Google Health Trends',
         url = 'https://dissc-yale.github.io/gtrends_collection/',
         geo_strata = 'state',
         age_strata = 'none',
         race_strata = 'none',
         race_level = NA_character_,
         additional_strata1 = 'none',
         additional_strata_level = NA_character_,
         sex_strata = 'none',
         sex_level = NA_character_) 


g1_state_harmonized_v2 <- google_merged_rsv %>%
  rename(Outcome_value1=rsv_novax2,
         geography=state) %>%
  mutate(outcome_type='Google Searches',
         outcome_label1 = 'Google Searches 2',
         domain = 'Respiratory infections',
         date_resolution = 'week',
         update_frequency = 'weekly',
         source = 'Google Health Trends',
         url = 'https://dissc-yale.github.io/gtrends_collection/',
         geo_strata = 'state',
         age_strata = 'none',
         race_strata = 'none',
         race_level = NA_character_,
         additional_strata1 = 'none',
         additional_strata_level = NA_character_,
         sex_strata = 'none',
         sex_level = NA_character_) 

#od1 <- lme4::lmer(search_volume~ 1 + season*search_volume_vax +  (1|state), data=g1_state[g1_state$date>=as.Date('2023-07-01'),])
# coef.vax <- summary(mod1)$coefficients['search_volume_vax','Estimate']

#######################################
###NREVSS viral testing data for rsv
#######################################

url_nrevss_rsv <- "https://data.cdc.gov/resource/3cxc-4k8q.csv"

cdc_nrevss_rsv <- runIfExpired(source='nrevss_rsv',storeIn='Raw',  basepath='./Data/Archive',
                               ~ read.socrata(url_nrevss_rsv),tolerance=(24*7)
)


key <- readRDS('./Data/other_data/hhs_regions.rds')

rsv1_tests <- cdc_nrevss_rsv %>%
  as.data.frame()

rsv_ts <- rsv1_tests %>%
  mutate(date = as.Date(mmwrweek_end),
         postdate = as.Date(posted) ) %>%
  filter(postdate==max(postdate)) %>%
  ungroup() %>%
  filter(level != 'National') %>%
  group_by(level ) %>%
  left_join(key, by=c('level'='Group.1')) %>%
  mutate(scaled_cases = pcr_detections/max(pcr_detections)*100,
         hhs_abbr = x  ) %>%
  ungroup()

dates2_rsv_ts <- MMWRweek(as.Date(rsv_ts$date))

max.wk.yr <- max(dates2_rsv_ts$MMWRweek[dates2_rsv_ts$MMWRyear==max(dates2_rsv_ts$MMWRyear)])

rsv_ts <- cbind.data.frame(rsv_ts,dates2_rsv_ts[,c('MMWRyear', 'MMWRweek')]) %>%
  mutate( epiyr = MMWRyear, 
          epiyr = if_else(MMWRweek<=26,MMWRyear - 1 ,MMWRyear),
          epiwk  = if_else( MMWRweek<=26, MMWRweek+52, MMWRweek  ),
          epiwk=epiwk-26
  )

write.csv(rsv_ts,'./Data/plot_files/rsv_ts_nrevss_test_rsv.csv')

#######################################
###Wastewater for RSV
#######################################

url_ww_rsv <- "https://www.cdc.gov/wcms/vizdata/NCEZID_DIDRI/RSVStateLevelDownloadCSV.csv"

cdc_ww_rsv <- runIfExpired(source='wastewater_rsv',storeIn='Raw',  basepath='./Data/Archive',
                           ~ read_csv(url_ww_rsv),tolerance=(24*7)
)


ww1_rsv_harmonized <- cdc_ww_rsv%>%
  mutate(date=as.Date(Week_Ending_Date)) %>%
  filter(Data_Collection_Period=='All Results') %>%
  rename(state="State/Territory", rsv_ww="State/Territory_WVAL") %>%
  arrange(state, date) %>%
  dplyr::select(state, date, rsv_ww) %>%
  arrange(state, date) %>%
  collect() %>%
  rename(Outcome_value1=rsv_ww,
         geography=state) %>%
  mutate(outcome_type='WasteWater',
         outcome_label1 = 'Waste Water',
         domain = 'Respiratory infections',
         date_resolution = 'week',
         update_frequency = 'weekly',
         source = 'CDC NWWS',
         url = 'https://www.cdc.gov/wcms/vizdata/NCEZID_DIDRI/RSVStateLevelDownloadCSV.csv',
         geo_strata = 'state',
         age_strata = 'none',
         race_strata = 'none',
         race_level = NA_character_,
         additional_strata1 = 'none',
         additional_strata_level = NA_character_,
         sex_strata = 'none',
         sex_level = NA_character_) 

##same for flu A
url_ww_flu <- "https://www.cdc.gov/wcms/vizdata/NCEZID_DIDRI/FluA/FluAStateMapDownloadCSV.csv"

cdc_ww_flu <- runIfExpired(source='wastewater_flu_a',storeIn='Raw',  basepath='./Data/Archive',
                           ~ read_csv(url_ww_flu),tolerance=(24*7)
)


ww1_flu_harmonized <- cdc_ww_flu %>%
  mutate(date=as.Date(Week_Ending_Date)) %>%
  filter(Data_Collection_Period=='All Results') %>%
  rename(state="State/Territory", flu_ww="State/Territory_WVAL") %>%
  arrange(state, date) %>%
  dplyr::select(state, date, flu_ww) %>%
  arrange(state, date) %>%
  collect() %>%
  rename(Outcome_value1=flu_ww,
         geography=state) %>%
  mutate(outcome_type='WasteWater',
         outcome_label1 = 'Waste Water (flu)',
         domain = 'Respiratory infections',
         date_resolution = 'week',
         update_frequency = 'weekly',
         source = 'CDC NWWS',
         url = 'https://www.cdc.gov/wcms/vizdata/NCEZID_DIDRI/FluA/FluAStateMapDownloadCSV.csv',
         geo_strata = 'state',
         age_strata = 'none',
         race_strata = 'none',
         race_level = NA_character_,
         additional_strata1 = 'none',
         additional_strata_level = NA_character_,
         sex_strata = 'none',
         sex_level = NA_character_) 

##same for covid
url_ww_covid <- "https://www.cdc.gov/wcms/vizdata/NCEZID_DIDRI/SC2StateLevelDownloadCSV.csv"

cdc_ww_covid <- runIfExpired(source='wastewater_covid',storeIn='Raw',  basepath='./Data/Archive',
                           ~ read_csv(url_ww_flu),tolerance=(24*7)
)


ww1_covid_harmonized <- cdc_ww_covid %>%
  mutate(date=as.Date(Week_Ending_Date)) %>%
  filter(Data_Collection_Period=='All Results') %>%
  rename(state="State/Territory", covid_ww="State/Territory_WVAL") %>%
  arrange(state, date) %>%
  dplyr::select(state, date, covid_ww) %>%
  arrange(state, date) %>%
  collect() %>%
  rename(Outcome_value1=covid_ww,
         geography=state) %>%
  mutate(outcome_type='WasteWater',
         outcome_label1 = 'Waste Water (covid)',
         domain = 'Respiratory infections',
         date_resolution = 'week',
         update_frequency = 'weekly',
         source = 'CDC NWWS',
         url = 'https://www.cdc.gov/wcms/vizdata/NCEZID_DIDRI/SC2StateLevelDownloadCSV.csv',
         geo_strata = 'state',
         age_strata = 'none',
         race_strata = 'none',
         race_level = NA_character_,
         additional_strata1 = 'none',
         additional_strata_level = NA_character_,
         sex_strata = 'none',
         sex_level = NA_character_) 


#######################################
###Epic ED for RSV
#######################################

epic_ed_rsv_flu_covid <- open_dataset( './Data/Archive/Cosmos ED/epic_cosmos_flu_rsv_covid.parquet') %>%
  collect()

e1 <- epic_ed_rsv_flu_covid %>%
  mutate( geography= if_else(geography=='Total', 'United States', geography)
  )

e1 %>% 
  write.csv(., './Data/plot_files/rsv_flu_covid_epic_cosmos_age_state.csv')



#######################
###Combined file for overlaid time series RSV figure

combined_file_rsv <- bind_rows(nssp_harmonized_rsv, ww1_rsv_harmonized,h1_harmonized_rsv,e1,g1_state_harmonized_v1, g1_state_harmonized_v2) %>%
  filter(date >=as.Date('2023-07-01') & age_strata=='none' & !outcome_name %in% c('FLU','COVID')) %>%
  arrange(geography, outcome_label1,source,date) %>%
  group_by(geography,outcome_label1, source) %>%
  filter(date>='2023-07-01') %>%
  mutate(outcome_3m = zoo::rollapplyr(Outcome_value1,3,mean, partial=T, na.rm=T),
         outcome_3m = if_else(is.nan(outcome_3m), NA, outcome_3m),
         outcome_3m_scale = outcome_3m / max(outcome_3m, na.rm=T)*100
  )

dates2 <- MMWRweek(as.Date(combined_file_rsv$date))

max.wk.yr <- max(dates2$MMWRweek[dates2$MMWRyear==max(dates2$MMWRyear)])

combined_file_rsv <- cbind.data.frame(combined_file_rsv,dates2[,c('MMWRyear', 'MMWRweek')]) %>%
  mutate( epiyr = MMWRyear, 
          epiyr = if_else(MMWRweek<=26,MMWRyear - 1 ,MMWRyear),
          epiwk  = if_else( MMWRweek<=26, MMWRweek+52, MMWRweek  ),
          epiwk=epiwk-26
  )

write.csv(combined_file_rsv,'./Data/plot_files/rsv_combined_all_outcomes_state.csv')

## Same for flu

combined_file_flu <- bind_rows(nssp_harmonized_flu, ww1_flu_harmonized,h1_harmonized_flu,e1) %>%
  filter(date >=as.Date('2023-07-01') & age_strata=='none' & !outcome_name %in% c('COVID','RSV')) %>%
  arrange(geography, outcome_label1,source,date) %>%
  group_by(geography,outcome_label1, source) %>%
  filter(date>='2023-07-01') %>%
  mutate(outcome_3m = zoo::rollapplyr(Outcome_value1,3,mean, partial=T, na.rm=T),
         outcome_3m = if_else(is.nan(outcome_3m), NA, outcome_3m),
         outcome_3m_scale = outcome_3m / max(outcome_3m, na.rm=T)*100
  )

dates2 <- MMWRweek(as.Date(combined_file_flu$date))

max.wk.yr <- max(dates2$MMWRweek[dates2$MMWRyear==max(dates2$MMWRyear)])

combined_file_flu <- cbind.data.frame(combined_file_flu,dates2[,c('MMWRyear', 'MMWRweek')]) %>%
  mutate( epiyr = MMWRyear, 
          epiyr = if_else(MMWRweek<=26,MMWRyear - 1 ,MMWRyear),
          epiwk  = if_else( MMWRweek<=26, MMWRweek+52, MMWRweek  ),
          epiwk=epiwk-26
  )

write.csv(combined_file_flu,'./Data/plot_files/flu_combined_all_outcomes_state.csv')

## Same for covid

combined_file_covid <- bind_rows(nssp_harmonized_covid, ww1_covid_harmonized,h1_harmonized_covid,e1) %>%
  filter(date >=as.Date('2023-07-01') & age_strata=='none' & !outcome_name %in% c('FLU','RSV')) %>%
  arrange(geography, outcome_label1,source,date) %>%
  group_by(geography,outcome_label1, source) %>%
  filter(date>='2023-07-01') %>%
  mutate(outcome_3m = zoo::rollapplyr(Outcome_value1,3,mean, partial=T, na.rm=T),
         outcome_3m = if_else(is.nan(outcome_3m), NA, outcome_3m),
         outcome_3m_scale = outcome_3m / max(outcome_3m, na.rm=T)*100
  )

dates2 <- MMWRweek(as.Date(combined_file_covid$date))

max.wk.yr <- max(dates2$MMWRweek[dates2$MMWRyear==max(dates2$MMWRyear)])

combined_file_covid <- cbind.data.frame(combined_file_covid,dates2[,c('MMWRyear', 'MMWRweek')]) %>%
  mutate( epiyr = MMWRyear, 
          epiyr = if_else(MMWRweek<=26,MMWRyear - 1 ,MMWRyear),
          epiwk  = if_else( MMWRweek<=26, MMWRweek+52, MMWRweek  ),
          epiwk=epiwk-26
  )

write.csv(combined_file_covid,'./Data/plot_files/covid_combined_all_outcomes_state.csv')


##############################################################

#################################################
### State map NSSP
################################################
dates <- seq.Date(from=as.Date('2022-10-01'), to=Sys.Date(),by='week')

i=length(dates)-1

d1_state <- cdc_nssp_rsv_flu_covid_ed1 %>%
  filter(county=='All'  ) %>%
  rename(percent_visits_rsv_state =percent_visits_rsv ) %>%
  # percent_visits_rsv_state=if_else(percent_visits_rsv>1,1,percent_visits_rsv) ) %>%
  rename(state=geography) %>%
  dplyr::select(state,week_end, percent_visits_rsv_state) 

d1_all <- cdc_nssp_rsv_flu_covid_ed1 %>%
  filter(county!='All' ) %>%
  rename(state=geography) %>%
  dplyr::select(state, county, fips, week_end, percent_visits_rsv) %>%
  left_join(d1_state, by=c('week_end', 'state')) %>%
  mutate(percent_visits_rsv = if_else(is.na(percent_visits_rsv),percent_visits_rsv_state,percent_visits_rsv ),
         #fix CT county coding
         fips = if_else(state=='Connecticut' & county=='Fairfield',9001 ,
                        if_else(state=='Connecticut' &  county=='Hartford', 9003,
                                if_else(state=='Connecticut'& county=='Litchfield', 9005 ,
                                        if_else(state=='Connecticut' & county=='Middlesex',9007 ,
                                                if_else(state=='Connecticut' & county=='New Haven', 9009 ,
                                                        if_else(state=='Connecticut' & county=='New London',9011 ,
                                                                if_else(state=='Connecticut' & county=='Tolland',9013 ,
                                                                        if_else(state=='Connecticut' & county=='Windham', 9015, fips)))))))
         ) ) %>%
  as.data.frame()

write.csv(d1_all,'./Data/plot_files/rsv_county_filled_map_nssp.csv')

#fluA
#ww_flu1 <- read.csv('https://www.cdc.gov/wcms/vizdata/NCEZID_DIDRI/FluA/FluAStateMapDownloadCSV.csv')%>%
#write.csv(ww_flu1,'./Data/FluStateLevelDownloadCSV_ww.csv')



##Metro; Crosswalk the DMA to counties FIPS codes
#https://www.kaggle.com/datasets/kapastor/google-trends-countydma-mapping?resource=download
# cw1 <- read.csv('./Data/GoogleTrends_CountyDMA_Mapping.csv') %>%
#   mutate(GOOGLE_DMA=toupper(GOOGLE_DMA))
# 
# #Metro region
# #https://stackoverflow.com/questions/61213647/what-do-gtrendsr-statistical-areas-correlate-with
# #Nielsen DMA map: http://bl.ocks.org/simzou/6459889
# data("countries")
# 
# metros <- countries[countries$country_code == 'US', ]
# 
# metros <-
#   metros[grep("[[:digit:]]", substring(metros$sub_code, first = 4)), ]
# 
# metros$numeric.sub.area <- gsub('US-', '', metros$sub_code)
# 
# 
# dma_link1 <- cbind.data.frame('DMA_name'=metros$name,'DMA'=metros$numeric.sub.area) %>%
#   rename(DMA_ID=DMA) %>%
#   full_join(cw1, by=c("DMA_name"="GOOGLE_DMA"))
# 


##Google metro data
# g1_metro <- read_parquet(temp_file) %>%
#   filter(!(location %in% g_states) ) %>%
#   collect() %>%
#   mutate(date2=as.Date(date, '%b %d %Y'),
#          date = as.Date(ceiling_date(date2, 'week'))-1) %>%
#   filter(date>='2021-03-01') %>%
#   rename(search_volume=value) %>%
#   left_join(dma_link1, by=c('location'='DMA_ID')) %>% #many to many join by date and counties
#    group_by(STATEFP,CNTYFP) %>%
#    mutate(fips=paste0(STATEFP,sprintf("%03d", CNTYFP)),
#           fips=as.numeric(fips),
#           
#           search_volume_scale = search_volume/max(search_volume,na.rm=T)*100) %>%
#    ungroup() 