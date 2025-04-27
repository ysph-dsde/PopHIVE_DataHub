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
# Overview
#################################

#Creates a time/date stamped parquet file in the folder indicated in runIfExpired. 
#if a file has been downloaded within past week (24*7 hours), 
#it just reads in latest file, otherwise it downloads fresh copy
#Check that formatting is consistent between vintages. (1) check column names (2) check variable formats
#compare newest data to previous dataset

#######################################
#Read in and archive latest Epic Files
#####################################
source('./R/Epic Cleaning/harmonize_epic.R')


#############################################
#Read in and archive latest files from APIs
#############################################

lapply(list.files('./R/Data Pull/', full.names=T), function(X){
  print(X)
  source(X)
} )


#######################################
# Prepare Epic ED files for plots 
#######################################

epic_ed_rsv_flu_covid <- open_dataset( './Data/Pulled Data/Cosmos ED/flu_rsv_covid_epic_cosmos_ed.parquet') %>%
  collect()

e1 <- epic_ed_rsv_flu_covid %>%
  mutate( geography= if_else(geography=='Total', 'United States', geography)
  )

e1 %>% 
  write.csv(., './Data/Plot Files/Cosmos ED/rsv_flu_covid_epic_cosmos_age_state.csv')


####################################################################
# prepare population size estimate 
####################################################################
# state level popsize estimate (latest available: 2023) obtained from CDC wonder
pop <- read.csv("./Data/other_data/Single-Race Population Estimates 2020-2023 by State and Single-Year Age.csv") %>%
  dplyr::rename(char = Notes.States.States.Code.Single.Year.Ages.Single.Year.Ages.Code.Population) %>%
  mutate(geography   = str_extract(char, "^[^0-9]+") %>% str_trim(),
         age = str_extract(char, "\\d{1,2} (year|years)"),
         popsize   = as.numeric(str_extract(char, "\\d+$")))  %>% 
  mutate(age = as.numeric(str_replace(ifelse(grepl("<", char), 0, # age = 0 for < 1 
                                             ifelse(grepl("85\\+", char), 85, age)), "year", ""))) %>%  # age = 85 for 85+ 
  filter(!is.na(age)) %>% dplyr::select(-char)


# pop of Puerto Rico (2023) obtained from https://www.census.gov/data/tables/time-series/demo/popest/2020s-detail-puerto-rico.html
pop_PR <- data.frame(geography = "Puerto Rico", 
                     age_level = c("Total", "<1 Years", "1-4 Years", "5-17 Years", "18-49 Years", "65+ Years"),
                     popsize = c(3205691, 18682, 78297, 401700, 1295060, 770855))

# pop of Virgin islands obtained from: https://www.census.gov/data/tables/2020/dec/2020-us-virgin-islands.html
# (only 2020 data can be found on census.gov, and only data unstratified by age can be found)
pop_VI <- data.frame(geography = "Virgin Islands", age_level = "Total", popsize = 87146)

# pop of Guam obtained from: https://www.census.gov/newsroom/press-releases/2023/2020-dhc-summary-file-guam.html
# (only 2020 data can be found on census.gov, and only data unstratified by age can be found)
pop_GU <- data.frame(geography = "Guam", age_level = "Total", popsize = 153836)


# population size by state (unstratified by age)
pop_unstra <- pop %>% group_by(geography) %>% summarize(popsize = sum(popsize)) %>% mutate(age_level = "Total") %>% 
  rbind(pop_PR %>% filter(age_level == "Total"), pop_VI, pop_GU) %>% arrange(geography) 
pop_unstra <- pop_unstra %>% rbind(data.frame(geography = "United States", popsize = sum(pop_unstra$popsize), age_level = "Total"))

# # population size by state (stratified by age) 
# pop_stra <- pop %>% 
#   mutate(age_level = case_when(age == 0 ~ "<1 Years",
#                                age >= 1 & age <= 4 ~ "1-4 Years",
#                                age >= 5 & age <= 17 ~ "5-17 Years", 
#                                age >= 18 & age <= 49 ~ "18-49 Years",
#                                age >= 50 & age <= 64 ~ "50-64 Years",
#                                age >= 65 ~ "65+ Years")) %>% 
#   group_by(geography, age_level) %>% summarize(popsize = sum(popsize)) %>% 
#   rbind(pop_PR %>% filter(age_level != "Total")) %>% arrange(geography)


#########################################################
### Combined file for overlaid time series RSV figure
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


# =========== Calculate population-weighted national average for each week (RSV) ==========
national_popwgted_avg_rsv <- combined_file_rsv %>% 
  # remove national level and two other geographies before calculating state weights (for latter two, we don't know popsize)
  filter(!geography %in% c("United States", "Armed Forces Africa", "None of the above")) %>% 
  left_join(pop_unstra %>% dplyr::select(-age_level), by = "geography") %>% 
  group_by(date, outcome_label1) %>% 
  mutate(wgt = ifelse(is.na(Outcome_value1), NA, popsize / sum(popsize[!is.na(Outcome_value1)]))) %>% # some states are missing (ED/ww) data for some dates, thus they are not included in the calculation
  mutate(across(c(starts_with("Outcome_value1")), ~ sum(.x * wgt, na.rm = T)
                # .names = "{.col}_wgtavg") 
  )) %>%
  mutate(across(c("Outcome_value2", "Outcome_value3", "Outcome_value4", "Outcome_value5", 
                  "search_volume", "search_volume_vax",
                  "rsv_novax2", "rsv_novax", "outcome_3m", "outcome_3m_scale"), ~ NA)) %>%
  dplyr::select(-popsize, -wgt, -geography) %>% distinct() %>% 
  mutate(geography = "United States", geo_strata = "national_pop_wgted_avg") %>% # to distinguish the calculated average vs original national data, thus changed geo_strata of the calculated average to "national_pop_wgted_avg" (original national level data was marked as "state")
  # recalculate moving average using the calculated average data
  group_by(geography, outcome_label1, source) %>%
  mutate(outcome_3m = zoo::rollapplyr(Outcome_value1,3,mean, partial=T, na.rm=T),
         outcome_3m = if_else(is.nan(outcome_3m), NA, outcome_3m),
         outcome_3m_scale = outcome_3m / max(outcome_3m, na.rm=T)*100) 


# ========== Append the calculated national average to the combined datasets (RSV) ==========
# In nssp_harmonized_XXX and e1 (epic ed) dataset, national-level data already existed originally, so the calculated average was not added back
combined_file_rsv_addavg <- combined_file_rsv %>% 
  rbind(national_popwgted_avg_rsv %>% filter(!source %in% c("CDC NSSP", "Epic Cosmos"))) %>% arrange(geography, outcome_label1, source, date) %>%
  mutate(geo_strata = ifelse(geography == "United States" & geo_strata != "national_pop_wgted_avg", "national", geo_strata)) %>%
  arrange(geography, outcome_label1, source, date) 
combined_file_rsv <- combined_file_rsv_addavg


# ========== add MMWR week (RSV) ==========
dates2 <- MMWRweek(as.Date(combined_file_rsv$date))

max.wk.yr <- max(dates2$MMWRweek[dates2$MMWRyear==max(dates2$MMWRyear)])

combined_file_rsv <- cbind.data.frame(combined_file_rsv,dates2[,c('MMWRyear', 'MMWRweek')]) %>%
  mutate( epiyr = MMWRyear, 
          epiyr = if_else(MMWRweek<=26,MMWRyear - 1 ,MMWRyear),
          epiwk  = if_else( MMWRweek<=26, MMWRweek+52, MMWRweek  ),
          epiwk=epiwk-26
  )

write.csv(combined_file_rsv,'./Data/Plot Files/Comparisons/rsv_combined_all_outcomes_state.csv')


#########################################################
### Combined file for overlaid time series flu figure
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

# ========== Calculate population-weighted national average for each week (FLU) ==========
national_popwgted_avg_flu <- combined_file_flu %>% 
  # remove national level and two other geographies before calculating state weights (for latter two, we don't know popsize)
  filter(!geography %in% c("United States", "Armed Forces Africa", "None of the above")) %>% 
  left_join(pop_unstra %>% dplyr::select(-age_level), by = "geography") %>% 
  group_by(date, outcome_label1) %>% 
  mutate(wgt = ifelse(is.na(Outcome_value1), NA, popsize / sum(popsize[!is.na(Outcome_value1)]))) %>% # some states are missing (ED/ww) data for some dates, thus they are not included in the calculation
  mutate(across(c(starts_with("Outcome_value1")), ~ sum(.x * wgt, na.rm = T)
                # .names = "{.col}_wgtavg") 
  )) %>%
  mutate(across(c("Outcome_value2", "Outcome_value3", "Outcome_value4", "Outcome_value5", 
                  "outcome_3m", "outcome_3m_scale"), ~ NA)) %>%
  dplyr::select(-popsize, -wgt, -geography) %>% distinct() %>% 
  mutate(geography = "United States", geo_strata = "national_pop_wgted_avg") %>% # to distinguish the calculated average vs original national data, thus changed geo_strata of the calculated average to "national_pop_wgted_avg" (original national level data was marked as "state")
  # recalculate moving average using the calculated average data
  group_by(geography, outcome_label1, source) %>%
  mutate(outcome_3m = zoo::rollapplyr(Outcome_value1,3,mean, partial=T, na.rm=T),
         outcome_3m = if_else(is.nan(outcome_3m), NA, outcome_3m),
         outcome_3m_scale = outcome_3m / max(outcome_3m, na.rm=T)*100) 


# ========== Append the calculated national average to the combined datasets (FLU) ==========
# In nssp_harmonized_XXX and e1 (epic ed) dataset, national-level data already existed originally, so the calculated average was not added back
combined_file_flu_addavg <- combined_file_flu %>% 
  rbind(national_popwgted_avg_flu %>% filter(!source %in% c("CDC NSSP", "Epic Cosmos"))) %>% arrange(geography, outcome_label1, source, date) %>%
  mutate(geo_strata = ifelse(geography == "United States" & geo_strata != "national_pop_wgted_avg", "national", geo_strata)) %>%
  arrange(geography, outcome_label1, source, date) 
combined_file_flu <- combined_file_flu_addavg

# ========== add MMWR week (FLU) ==========
dates2 <- MMWRweek(as.Date(combined_file_flu$date))

max.wk.yr <- max(dates2$MMWRweek[dates2$MMWRyear==max(dates2$MMWRyear)])

combined_file_flu <- cbind.data.frame(combined_file_flu,dates2[,c('MMWRyear', 'MMWRweek')]) %>%
  mutate( epiyr = MMWRyear, 
          epiyr = if_else(MMWRweek<=26,MMWRyear - 1 ,MMWRyear),
          epiwk  = if_else( MMWRweek<=26, MMWRweek+52, MMWRweek  ),
          epiwk=epiwk-26
  )

write.csv(combined_file_flu,'./Data/Plot Files/Comparisons/flu_combined_all_outcomes_state.csv')

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


# ========== Calculate population-weighted national average for each week (COVID) ==========
# In nssp_harmonized_XXX and e1 (epic ed) dataset, national-level data already existed originally, so the calculated average was not added back
national_popwgted_avg_covid <- combined_file_covid %>% 
  # remove national level and two other geographies before calculating state weights (for latter two, we don't know popsize)
  filter(!geography %in% c("United States", "Armed Forces Africa", "None of the above")) %>% 
  left_join(pop_unstra %>% dplyr::select(-age_level), by = "geography") %>% 
  group_by(date, outcome_label1) %>% 
  mutate(wgt = ifelse(is.na(Outcome_value1), NA, popsize / sum(popsize[!is.na(Outcome_value1)]))) %>% # some states are missing (ED/ww) data for some dates, thus they are not included in the calculation
  mutate(across(c(starts_with("Outcome_value1")), ~ sum(.x * wgt, na.rm = T)
                # .names = "{.col}_wgtavg") 
  )) %>%
  mutate(across(c("Outcome_value2", "Outcome_value3", "Outcome_value4", "Outcome_value5", 
                  "outcome_3m", "outcome_3m_scale"), ~ NA)) %>%
  dplyr::select(-popsize, -wgt, -geography) %>% distinct() %>% 
  mutate(geography = "United States", geo_strata = "national_pop_wgted_avg") %>% # to distinguish the calculated average vs original national data, thus changed geo_strata of the calculated average to "national_pop_wgted_avg" (original national level data was marked as "state")
  # recalculate moving average using the calculated average data
  group_by(geography, outcome_label1, source) %>%
  mutate(outcome_3m = zoo::rollapplyr(Outcome_value1,3,mean, partial=T, na.rm=T),
         outcome_3m = if_else(is.nan(outcome_3m), NA, outcome_3m),
         outcome_3m_scale = outcome_3m / max(outcome_3m, na.rm=T)*100) 


# ========== Append the calculated national average to the combined datasets (COVID) ==========
combined_file_covid_addavg <- combined_file_covid %>% 
  rbind(national_popwgted_avg_covid %>% filter(!source %in% c("CDC NSSP", "Epic Cosmos"))) %>% arrange(geography, outcome_label1, source, date) %>%
  mutate(geo_strata = ifelse(geography == "United States" & geo_strata != "national_pop_wgted_avg", "national", geo_strata)) %>%
  arrange(geography, outcome_label1, source, date) 
combined_file_covid <- combined_file_covid_addavg

# ========== add MMWR week (COVID) ==========
dates2 <- MMWRweek(as.Date(combined_file_covid$date))

max.wk.yr <- max(dates2$MMWRweek[dates2$MMWRyear==max(dates2$MMWRyear)])

combined_file_covid <- cbind.data.frame(combined_file_covid,dates2[,c('MMWRyear', 'MMWRweek')]) %>%
  mutate( epiyr = MMWRyear, 
          epiyr = if_else(MMWRweek<=26,MMWRyear - 1 ,MMWRyear),
          epiwk  = if_else( MMWRweek<=26, MMWRweek+52, MMWRweek  ),
          epiwk=epiwk-26
  )

write.csv(combined_file_covid,'./Data/Plot Files/Comparisons/covid_combined_all_outcomes_state.csv')


##################################################################################################


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

