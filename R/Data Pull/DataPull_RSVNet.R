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

write.csv(h1.age.rsv,'./Data/Plot Files/rsv_hosp_age_respnet.csv')
