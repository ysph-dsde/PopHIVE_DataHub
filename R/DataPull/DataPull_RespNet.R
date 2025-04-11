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
