
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
         outcome_label1 = 'Waste Water wval (RSV)',
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
         outcome_label1 = 'Waste Water wval (Influenza)',
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
                             ~ read_csv(url_ww_covid),tolerance=(24*7)
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
         outcome_label1 = 'Waste Water wval (SARS-CoV-2)',
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

