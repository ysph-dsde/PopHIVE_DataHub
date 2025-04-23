suppressPackageStartupMessages({
  library("arrow")
  library("parquetize")
  library("readxl")
  library("assertthat")
  library("tidyverse")
  library("MMWRweek")
  library("lubridate")
  library("RSocrata")
})

"%!in%" <- function(x,y)!("%in%"(x,y))

#source('./R/Support Functions/archiving_functions.R') #function for archiving
source('./R/Support Functions/API Interaction.R')


#################################
#Overview
#################################

#Creates a time/date stamped parquet file in the folder indicated in runIfExpired. 
#if a file has been downloaded within past week (24*7 hours), 
#it just reads in latest file, otherwise it downloads fresh copy
#Check that formatting is consistent between vintages. (1) check column names (2) check variable formats
#compare newest data to previous dataset

#######################################
#Read in and archive latest Epic Files
#####################################
source('./R/Cleaning and Harmonization/EpicClean/harmonize_epic.R')


#############################################
#Read in and archive latest files from APIs
#############################################

lapply(list.files('./R/Data Pull/', full.names=T), function(X){
  print(X)
  source(X)
} )


#######################################
###Prepare Epic ED files for plots 
#######################################

epic_ed_rsv_flu_covid <- open_dataset( './Data/Pulled Data/Cosmos ED/flu_rsv_covid_epic_cosmos_ed.parquet') %>%
  collect()

e1 <- epic_ed_rsv_flu_covid %>%
  mutate( geography= if_else(geography=='Total', 'United States', geography)
  )

e1 %>% 
  write.csv(., './Data/Plot Files/Cosmos ED/rsv_flu_covid_epic_cosmos_age_state.csv')



#########################################################
###Combined file for overlaid time series RSV figure
#########################################################

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

write.csv(combined_file_rsv,'./Data/Plot Files/Cosmos ED/rsv_combined_all_outcomes_state.csv')

#########################################################
###Combined file for overlaid time series flu figure
#########################################################

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

write.csv(combined_file_flu,'./Data/Plot Files/Cosmos ED/flu_combined_all_outcomes_state.csv')

#########################################################
###Combined file for overlaid time series COVID-19 figure
#########################################################

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

write.csv(combined_file_covid,'./Data/Plot Files/Cosmos ED/covid_combined_all_outcomes_state.csv')


##############################################################

#################################################
### State map NSSP for flu. RSV, COVID
################################################
dates <- seq.Date(from=as.Date('2022-10-01'), to=Sys.Date(),by='week')

i=length(dates)-1

d1_state_rsv_flu_covid <- cdc_nssp_rsv_flu_covid_ed1 %>%
  filter(county=='All'  ) %>%
  rename(percent_visits_rsv_state =percent_visits_rsv,
         percent_visits_covid_state =percent_visits_covid,
         percent_visits_flu_state =percent_visits_influenza) %>%
  # percent_visits_rsv_state=if_else(percent_visits_rsv>1,1,percent_visits_rsv) ) %>%
  rename(state=geography) %>%
  dplyr::select(state,week_end, percent_visits_rsv_state,percent_visits_covid_state, percent_visits_flu_state) 

d1_all <- cdc_nssp_rsv_flu_covid_ed1 %>%
  filter(county!='All' ) %>%
  rename(state=geography) %>%
  dplyr::select(state, county, fips, week_end, percent_visits_rsv,percent_visits_covid,percent_visits_influenza) %>%
  left_join(d1_state_rsv_flu_covid, by=c('week_end', 'state')) %>%
  mutate(percent_visits_covid = if_else(is.na(percent_visits_covid),percent_visits_covid_state,percent_visits_covid),
         percent_visits_flu = if_else(is.na(percent_visits_influenza),percent_visits_flu_state,percent_visits_influenza ),
         percent_visits_rsv = if_else(is.na(percent_visits_rsv),percent_visits_rsv_state,percent_visits_rsv ),
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
  dplyr::select(state, county, fips, week_end,percent_visits_covid, percent_visits_flu, percent_visits_rsv) %>%
  as.data.frame() 
  

write.csv(d1_all,'./Data/Plot Files/Cosmos ED/rsv_flu_covid_county_filled_map_nssp.csv')



# 
# ##Metro; Crosswalk the DMA to counties FIPS codes
# #https://www.kaggle.com/datasets/kapastor/google-trends-countydma-mapping?resource=download
# cw1 <- read.csv('./Data/other_data/GoogleTrends_CountyDMA_Mapping.csv') %>%
#   mutate(GOOGLE_DMA=toupper(GOOGLE_DMA))

#Metro region
#https://stackoverflow.com/questions/61213647/what-do-gtrendsr-statistical-areas-correlate-with
#Nielsen DMA map: http://bl.ocks.org/simzou/6459889
#read in 'countries' file from gtrendsR
# countries <- read.csv('./Data/other_data/countries_gtrendsR.csv')
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
#   full_join(cw1, by=c("DMA_name"="GOOGLE_DMA")) %>%
#   dplyr::select(STATE , COUNTY, STATEFP, CNTYFP, DMA_ID) %>%
#   mutate(DMA_ID = as.numeric(DMA_ID)) %>%
#   filter(!is.na(DMA_ID))
# 
# 
# 
# ##Google metro data
# url1 <- "https://github.com/DISSC-yale/gtrends_collection/raw/refs/heads/main/data/term=rsv/part-0.parquet"
# temp_file1 <- tempfile(fileext = ".parquet")
# download.file(url1, temp_file1, mode = "wb")
# 
# g1_metro <- read_parquet(temp_file1) %>%
#   filter(!(location %in% g_states)) %>%
#   group_by(date, location, term) %>%
#   summarize(value=mean(value)) %>% #averages over duplicate pulls
#   ungroup() %>%
#   collect() %>%
#   mutate(date2=as.Date(date),
#          date = as.Date(ceiling_date(date2, 'week'))-1) %>%
#   mutate(location = as.numeric(location)) %>%
#   filter(!is.na(location)) %>%
#   rename(search_volume=value) %>%
#   filter(date == as.Date('2024-12-7')) %>%
#   left_join(dma_link1, by=c('location'='DMA_ID')) %>% #many to many join by date and counties
#    group_by(STATEFP,CNTYFP) %>%
#    mutate(fips=paste0(STATEFP,sprintf("%03d", CNTYFP)),
#           fips=as.numeric(fips)) %>%
#   ungroup() %>%
#          mutate( search_volume_scale = search_volume/max(search_volume,na.rm=T)*100) %>%
#    ungroup() %>%
#   dplyr::select(date, term, STATE, COUNTY, fips,search_volume_scale)
# 
# 
# usmap::plot_usmap(data=g1_metro,regions='county', values='search_volume_scale',
#                   color = NA,    # Faint border color
#                   size = 0     )+      # Thin border lines)   +
#   theme(panel.background = element_rect(color = "white", fill = "white")) +
#   scale_fill_gradientn(
#     scaletitle,
#     colors = pal1,
#     values = scales::rescale(c(0, 25,50, 75, 100)),
#     limits = c(0, 100),
#     na.value = "darkgray"
#   )

################
##Pneumococcal disease 
################
#csv_to_parquet('https://data.cdc.gov/resource/qvzb-qs6p.csv',path_to_parquet ='./Data/ipd_cdc1998.parquet')

ipd1 <- readRDS('./Data/Pulled Data/pneumococcus/ABCs_st_1998_2023.rds') %>%
  rename(agec = "Age.Group..years.",
         year=Year,
         st=IPD.Serotype,
         N_IPD = Frequency.Count) %>%
  mutate( st= if_else(st=='16','16F', st),
          agec1 = if_else(agec %in% c("Age <2","Age 2-4") ,1,2 ),
          agec=gsub('Age ', '', agec),
          agec2 = if_else( agec %in% c('<2','2-4'), '<5',
                           if_else( agec %in% c('5-17','18-49'), '5-49',
                                    if_else( agec %in% c('50-64','65+'), '50+',NA))),
          agec2 = factor(agec2, levels=c('<5','5-49','50+'), labels=c('<5 years', '5-49 years', '50+ years') )
  ) %>%
  group_by(st,agec2, year) %>%
  summarize(N_IPD=sum(N_IPD)) %>%
  ungroup()

write.csv(ipd1, './Data/Plot Files/pneumococcus/ipd_serotype_age_year.csv')



# pneumococcal serotype by state
b2019 <- read.csv('./Data/Pulled Data/pneumococcus/jiac058_suppl_supplementary_table_s2.csv') %>%
  group_by(State, sero) %>%
  summarize(N_cases=n()) %>%
  mutate(sero=as.factor(sero)) %>%
  ungroup() %>%
  group_by(State, sero) %>%
  mutate(mean_cases=max(N_cases,na.rm=T)
  ) %>%
  group_by(State) %>%
  mutate(         pct = N_cases/sum(N_cases, na.rm=T)*100) %>%
  ungroup() %>%
  tidyr::complete(sero,State , fill=list(pct=0))  #fills 0

write.csv(b2019, './Data/Plot Files/pneumococcus/ipd_serotype_state_pct.csv')

