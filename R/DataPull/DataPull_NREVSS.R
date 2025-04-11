
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