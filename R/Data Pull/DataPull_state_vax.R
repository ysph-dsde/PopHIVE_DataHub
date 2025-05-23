

az.ls <- lapply(list.files('./Data/Pulled Data/vax/arizona', full.names=T), function(X){
#  print(X)
  read_csv(X) %>%
    mutate(Enrolled=as.numeric(Enrolled)) %>%
    reshape2::melt(., id.vars=c("School Year","County","Grade", "Enrolled"  )) %>%
    mutate(value=as.numeric(value))
} ) 


fips <- pop_county_age <- read.csv("Data/other_data/diabetes_obesity/population_county_age_censusgov_2021.csv", skip = 1) %>% 
  dplyr::select(c(1,2,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39)) %>%
  pivot_longer(cols = 3:21, names_to = "agegroup", values_to = "pop_2021", names_prefix = "Estimate..Total..Total.population") %>% 
  mutate(fips = str_replace(Geography, "0500000US", ""))

# url <- "https://www2.census.gov/geo/docs/reference/codes/files/national_county.txt"
# fips_df <- read.csv(url, header = FALSE) %>%
#   rename(county_name=V4,
#          county_fips=V3,
#          state_fips=V2,
#          state_code=V1) %>%
#    mutate(fips =paste0(sprintf("%02d", state_fips), sprintf("%03d", fips_df$county_fips)),
#                        county_name=gsub(' County', '',county_name)
#    ) %>%
#   dplyr::select(-V5)
# 
# write.csv(fips_df,'./Data/other_data/county_fips.csv')

fips_df <- read_csv('./Data/other_data/county_fips.csv')
fips_df_az <- fips_df %>% filter(state_code=='AZ')

az.ds <- az.ls %>%
  bind_rows() %>%
  rename(year='School Year',
         county=County,
         grade=Grade,
         N = Enrolled) %>%
  left_join(fips_df_az, by= c('county'='county_name')) %>%
  mutate( variable = gsub('% ','', variable),
         doses = substr(variable,1,1),
          vax= tolower(variable),
          vax= gsub("[^A-Za-z]", "_", vax),
          vax= gsub('__','', vax),
         county = if_else(county %in% c('Total','State Totals'), 'Total', county),
         fips = if_else(county=='Total', '04', fips),
         vax = if_else(vax=='exempt_from_every_req_d_vaccine','full_exempt',vax),
         vax = gsub('_mmr','mmr', vax)
                            ) %>%
  dplyr::select(year, county, fips, grade, N, vax, value)

write_parquet(az.ds,'./Data/Webslim/childhood_immunizations/az_vaccines.parquet')
write_csv(az.ds,'./Data/Plot Files/childhood_immunizations/az_vaccines.csv')

az.ds %>%
  filter(vax=='mmr') %>%
ggplot(aes(x=year, y=value, group=county,color=county)) +
  geom_line()+
  theme_classic()+
  ggtitle('MMR 2 dose coverage in Kindergarten')

az.ds %>%
  filter(vax=='full_exempt') %>%
  ggplot(aes(x=year, y=value, group=county,color=county)) +
  geom_line()+
  theme_classic()+
  ggtitle('Exemption % in Kindergarten')

