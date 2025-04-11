
#######################################
###Google searches for RSV vaccination
#######################################
#https://dissc-yale.github.io/gtrends_collection/

g_states <- paste('US',state.abb,sep='-')

url2 <- "https://github.com/DISSC-yale/gtrends_collection/raw/refs/heads/main/data/term=Naloxone/part-0.parquet" #rsv vaccination category

temp_file2 <- tempfile(fileext = ".parquet")
download.file(url2, temp_file2, mode = "wb")

google_naloxone <- runIfExpired(source='google_naloxone', storeIn='Raw',  basepath='./Data/Archive',
                               ~ read_parquet(temp_file2),
                               tolerance=(24*7)
)

g1_naloxone <- google_naloxone %>%
  filter( location %in% g_states ) %>%
  mutate(date=as.Date(date),
         date = as.Date(ceiling_date(date, 'week'))-1,
         stateabb= gsub('US-','', location),
         state=state.name[match(stateabb,state.abb)],
         value=round(value,2)) %>%
  rename(search_volume_naloxone=value) %>%
  dplyr::select(state, date, search_volume_naloxone) %>%
  group_by(state, date) %>%
  summarize(search_volume_naloxone=mean(search_volume_naloxone, na.rm=T))

