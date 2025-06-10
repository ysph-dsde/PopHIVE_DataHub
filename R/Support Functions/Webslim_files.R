##Take the previous 'plot files' format and slim down for the final
#Dataface website

library(tidyverse)
library(arrow)
library(sf)
base_dir <- './Data/Plot Files/'

log_file <- "./Data/Webslim/file_log.json"
log_write <- function(data, path) {
  write_parquet(data, path)
  log <- jsonlite::read_json(log_file)
  file_name <- sub("./Data/Webslim/", "", path, fixed = TRUE)
  entry <- list(updated = Sys.time(), md5 = tools::md5sum(path))
  last_entry <- log[[file_name]]
  if (is.null(last_entry) || last_entry$md5 != entry$md5) {
    log[[file_name]] <- entry
    jsonlite::write_json(log, log_file, auto_unbox = TRUE)
  }
}


#######################################
## RSV
#######################################

#NREVSS data

nrevss_rsv_ts <- read_csv(paste0(
  base_dir,
  'NREVSS/rsv_ts_nrevss_test_rsv_nat.csv'
)) %>%
  filter(!is.na(level)) %>%
  rename(
    geography = level,
    week = MMWRweek,
    year = MMWRyear,
    value = pcr_detections
  ) %>%
  mutate(
    geography = gsub('NA,', '', geography),
    geography = gsub(',NA', '', geography)
  )

log_write(
  nrevss_rsv_ts,
  './Data/Webslim/respiratory_diseases/rsv/positive_tests.parquet'
)

##RSV all indicators, all ages

rsv_all_indicators_state <- read_csv(paste0(
  base_dir,
  'Comparisons/rsv_combined_all_outcomes_state.csv'
)) %>%
  filter(outcome_label1 != 'Google Searches 1') %>% #only keep definition #2 for google searches
  dplyr::select(
    geography,
    date,
    source,
    suppressed_flag,
    Outcome_value1,
    outcome_3m,
    outcome_3m_scale
  ) %>% #note this is RAW data; needs 3 week ave and scaling for plot
  rename(
    value = Outcome_value1,
    value_smooth = outcome_3m,
    value_smooth_scale = outcome_3m_scale
  ) %>%
  mutate(
    source = if_else(source == "Epic Cosmos", "Epic Cosmos, ED", source),
    source = if_else(source == "CDC RSV-NET (RespNet)", "CDC RespNET", source)
  )

log_write(
  rsv_all_indicators_state,
  './Data/Webslim/respiratory_diseases/rsv/overall_trends.parquet'
)


##RSV Cosmos age and RSV-Net by age
epic_ed_combo_rsv <- read_csv(paste0(
  base_dir,
  'Cosmos ED/rsv_flu_covid_epic_cosmos_age_state.csv'
)) %>%
  mutate(source = "Epic Cosmos (ED)") %>%
  filter(outcome_name == 'RSV') %>%
  dplyr::select(
    date,
    geography,
    age_level,
    source,
    suppressed_flag,
    Outcome_value1,
    Outcome_value2
  ) %>%
  rename(value = Outcome_value1, value_smooth = Outcome_value2, age = age_level)

rsvnet_age <- read_csv(paste0(
  base_dir,
  'RESP-NET Programs/rsv_hosp_age_respnet.csv'
)) %>%
  mutate(source = "CDC RSV-NET (Hospitalization)", suppressed_flag = 0) %>%
  dplyr::select(
    date,
    state,
    Level,
    source,
    suppressed_flag,
    hosp_rate,
    hosp_rate_3m
  ) %>%
  rename(
    value = hosp_rate,
    value_smooth = hosp_rate_3m,
    geography = state,
    age = Level
  )

age_trends_rsv <- bind_rows(epic_ed_combo_rsv, rsvnet_age) %>%
  group_by(geography, age, source) %>%
  mutate(
    value_smooth_scale = value_smooth - min(value_smooth, na.rm = T),
    value_smooth_scale = value_smooth_scale / max(value_smooth_scale, na.rm = T)
  ) %>%
  mutate(
    geography = if_else(geography == 'All states', 'United States', geography)
  )

log_write(
  age_trends_rsv,
  './Data/Webslim/respiratory_diseases/rsv/trends_by_age.parquet'
)

##RSV by county and week

rsv_county <- read_csv(paste0(
  base_dir,
  'Cosmos ED/rsv_flu_covid_county_filled_map_nssp.csv'
)) %>%
  dplyr::select(fips, week_end, percent_visits_rsv)

log_write(
  rsv_county,
  './Data/Webslim/respiratory_diseases/rsv/ed_visits_by_county.parquet'
)

## RSV testing
rsv_testing <- read_csv(paste0(
  base_dir,
  'Cosmos ED/rsv_testing_freq.csv'
)) %>%
  rename(age=age_level,
         value=Outcome_value1,
         ) %>%
  dplyr::select(
    age,
    date,
    value,
    N_J12_J18,
    suppressed_flag
  )

log_write(
  rsv_testing,
  './Data/Webslim/respiratory_diseases/rsv/rsv_testing_pct.parquet'
)

######################################
##INFLUENZA
######################################

##flu all indicators, all ages

flu_all_indicators_state <- read_csv(paste0(
  base_dir,
  'Comparisons/flu_combined_all_outcomes_state.csv'
)) %>%
  filter(outcome_label1 != 'Google Searches 1') %>% #only keep definition #2 for google searches
  dplyr::select(
    geography,
    date,
    source,
    suppressed_flag,
    Outcome_value1,
    Outcome_value2,
    outcome_3m_scale
  ) %>% #note this is RAW data; needs 3 week ave and scaling for plot
  rename(
    value = Outcome_value1,
    value_smooth = Outcome_value2,
    value_smooth_scale = outcome_3m_scale
  ) %>%
  mutate(source = if_else(source == "Epic Cosmos", "Epic Cosmos, ED", source),
         source = if_else(source == "CDC FluSurv-NET", 'CDC RespNET', source))

log_write(
  flu_all_indicators_state,
  './Data/Webslim/respiratory_diseases/influenza/overall_trends.parquet'
)


##flu Cosmos age
epic_ed_combo_flu <- read_csv(paste0(
  base_dir,
  'Cosmos ED/rsv_flu_covid_epic_cosmos_age_state.csv'
)) %>%
  mutate(source = "Epic Cosmos (ED)") %>%
  filter(outcome_name == 'FLU') %>%
  dplyr::select(
    date,
    geography,
    age_level,
    source,
    suppressed_flag,
    Outcome_value1,
    Outcome_value2
  ) %>%
  rename(
    value = Outcome_value1,
    value_smooth = Outcome_value2,
    age = age_level
  ) %>%
  group_by(geography, age, source) %>%
  mutate(
    value_smooth_scale = value_smooth - min(value_smooth, na.rm = T),
    value_smooth_scale = value_smooth_scale / max(value_smooth_scale, na.rm = T)
  )


log_write(
  epic_ed_combo_flu,
  './Data/Webslim/respiratory_diseases/influenza/trends_by_age.parquet'
)


##flu by county and week

flu_county <- read_csv(paste0(
  base_dir,
  'Cosmos ED/rsv_flu_covid_county_filled_map_nssp.csv'
)) %>%
  dplyr::select(fips, week_end, percent_visits_flu)

log_write(
  flu_county,
  './Data/Webslim/respiratory_diseases/influenza/ed_visits_by_county.parquet'
)


######################################
##COVID
######################################

##covid all indicators, all ages

covid_all_indicators_state <- read_csv(paste0(
  base_dir,
  'Comparisons/covid_combined_all_outcomes_state.csv'
)) %>%
  filter(outcome_label1 != 'Google Searches 1') %>% #only keep definition #2 for google searches
  dplyr::select(
    geography,
    date,
    source,
    suppressed_flag,
    Outcome_value1,
    Outcome_value2,
    outcome_3m_scale
  ) %>% #note this is RAW data; needs 3 week ave and scaling for plot
  rename(
    value = Outcome_value1,
    value_smooth = Outcome_value2,
    value_smooth_scale = outcome_3m_scale
  ) %>%
  mutate(source = if_else(source == "Epic Cosmos", "Epic Cosmos, ED", source),
         source = if_else(source== "CDC COVID-NET", 'CDC RespNET', source))

log_write(
  covid_all_indicators_state,
  './Data/Webslim/respiratory_diseases/covid/overall_trends.parquet'
)


##covid Cosmos age
epic_ed_combo_covid <- read_csv(paste0(
  base_dir,
  'Cosmos ED/rsv_flu_covid_epic_cosmos_age_state.csv'
)) %>%
  mutate(source = "Epic Cosmos (ED)") %>%
  filter(outcome_name == 'COVID') %>%
  dplyr::select(
    date,
    geography,
    age_level,
    source,
    suppressed_flag,
    Outcome_value1,
    Outcome_value2,
    Outcome_value3
  ) %>%
  rename(
    value = Outcome_value1,
    value_smooth = Outcome_value2,
    value_smooth_scale=Outcome_value3,
    age = age_level
  ) %>%
  group_by(geography, age, source) %>%
  mutate(
    value_smooth_scale = value_smooth - min(value_smooth, na.rm = T),
    value_smooth_scale = value_smooth_scale / max(value_smooth_scale, na.rm = T)
  )


log_write(
  epic_ed_combo_covid,
  './Data/Webslim/respiratory_diseases/covid/trends_by_age.parquet'
)


##flu by county and week

covid_county <- read_csv(paste0(
  base_dir,
  'Cosmos ED/rsv_flu_covid_county_filled_map_nssp.csv'
)) %>%
  dplyr::select(fips, week_end, percent_visits_covid)

log_write(
  covid_county,
  './Data/Webslim/respiratory_diseases/covid/ed_visits_by_county.parquet'
)




#################################################################
## Pneumococcus
################################################################

#IPD serotype trends
ipd_serotype_age <- read_csv(paste0(
  base_dir,
  './pneumococcus/ipd_serotype_age_year.csv'
)) %>%
  dplyr::select(st, agec2, year, N_IPD) %>%
  mutate(agec2 = gsub('year', 'Year', agec2)) %>%
  rename(serotype = st, age = agec2, value = N_IPD)

log_write(
  ipd_serotype_age,
  './Data/Webslim/respiratory_diseases/pneumococcus/serotype_trends.parquet'
)


#Geographic variation (based on genomic data)

ipd_serotype_state <- read_csv(paste0(
  base_dir,
  './pneumococcus/ipd_serotype_state_pct.csv'
)) %>%
  dplyr::select(sero, State, pct) %>%
  rename(serotype = sero, geography = State, value = pct)

log_write(
  ipd_serotype_state,
  './Data/Webslim/respiratory_diseases/pneumococcus/by_geography.parquet'
)


##comparison of IPD and pneumonia

ipd1 <- readRDS(
  './Data/Pulled Data/pneumococcus/ABCs_st_1998_2023.rds'
) %>%
  rename(
    agec = "Age.Group..years.",
    year = Year,
    st = IPD.Serotype,
    N_IPD = Frequency.Count
  ) %>%
  mutate(
    st = if_else(st == '16', '16F', st),
    st = if_else(st %in% c('15B', '15C'), '15BC', st),
    if_else(st %in% c('6A', '6C'), '6AC', st)
  ) %>%
  filter(year %in% c(2019, 2020) & agec %in% c('Age 50-64', 'Age 65+')) %>%
  group_by(st) %>%
  summarize(N_IPD = sum(N_IPD))

uad <- read_csv(
  './Data/Pulled Data/pneumococcus/ramirez_ofid_2025_ofae727.csv'
) %>%
  mutate(N_SSUAD = over65 + a50_64_with_indication + a50_64_no_indication) %>%
  full_join(ipd1, by = 'st') %>%
  filter(!is.na(N_SSUAD) & !is.na(N_IPD)) %>%
  dplyr::select(st, N_SSUAD, N_IPD) %>%
  rename(serotype = st, ipd = N_IPD, pneumonia = N_SSUAD)

log_write(
  uad,
  './Data/Webslim/respiratory_diseases/pneumococcus/comparison.parquet'
)


####################################################
##Immunization
####################################################
vax_age <- read_parquet(
  paste0(base_dir, 'childhood_immunizations/vax_age_nis.parquet')
) %>%
  filter(
    Geography %in%
      c(state.name, 'District of Columbia', 'United States') &
      birth_year %in%
        c(
          '2011',
          '2012',
          '2013',
          '2014',
          '2015',
          '2016',
          '2017',
          '2018',
          '2019',
          '2020',
          '2021',
          '2022',
          '2023',
          '2024',
          '2025'
        ) &
      dim1 == 'Age'
  ) %>%
  mutate(
    vax_order = as.numeric(as.factor(Vaccine)),
    Vaccine_dose = as.factor(paste(Vaccine, Dose)),
    Vaccine_dose = gsub('NA', '', Vaccine_dose),
    Vaccine_dose = trimws(Vaccine_dose)
  ) %>%
  dplyr::select(Geography, birth_year, age, Vaccine_dose, Outcome_value1) %>%
  rename(geography = Geography, vaccine = Vaccine_dose, value = Outcome_value1)

log_write(
  vax_age,
  './Data/Webslim/childhood_immunizations/overall_rates.parquet'
)

vax_urban <- read_parquet(
  './Data/Pulled Data/vax/nis_peds_vax.parquet'
) %>%
  rename(
    birth_year = `Birth Year/Birth Cohort`,
    dim1 = `Dimension Type`,
    urban = Dimension,
    vax_uptake = `Estimate (%)`,
    samp_size_vax = `Sample Size`
  ) %>%
  collect() %>%
  mutate(
    Vaccine_dose = as.factor(paste(Vaccine, Dose)),
    Vaccine_dose = gsub('NA', '', Vaccine_dose),
    Vaccine_dose = trimws(Vaccine_dose)
    
  ) %>%
  filter(birth_year == '2016-2019' & dim1 == 'Urbanicity') %>%
  dplyr::select(
    Vaccine_dose,
    Geography,
    Dose,
    dim1,
    vax_uptake,
    samp_size_vax,
    urban,
    birth_year
  ) %>%
  filter(
    Geography %in% c(state.name, 'District of Columbia', 'United States')
  ) %>%
  mutate(
    urban = factor(
      urban,
      levels = c(
        "Living In a Non-MSA",
        "Living In a MSA Non-Principal City",
        "Living In a MSA Principal City"
      ),
      labels = c('Rural', 'Smaller City', 'Larger City')
    )
  ) %>%
  dplyr::select(Geography, urban, birth_year, Vaccine_dose, vax_uptake) %>%
  rename(geography = Geography, vaccine = Vaccine_dose, value = vax_uptake)

log_write(
  vax_urban,
  './Data/Webslim/childhood_immunizations/rates_by_urbanicity.parquet'
)

vax_insurance <- read_parquet(
  './Data/Pulled Data/vax/nis_peds_vax.parquet'
) %>%
  rename(
    birth_year = `Birth Year/Birth Cohort`,
    dim1 = `Dimension Type`,
    insurance = Dimension,
    vax_uptake = `Estimate (%)`,
    samp_size_vax = `Sample Size`
  ) %>%
  collect() %>%
  mutate(Vaccine_dose = as.factor(paste(Vaccine, Dose))) %>%
  filter(birth_year == '2016-2019' & dim1 == 'Insurance Coverage') %>%
  dplyr::select(
    Vaccine_dose,
    Geography,
    Dose,
    dim1,
    vax_uptake,
    samp_size_vax,
    insurance,
    birth_year
  ) %>%
  filter(
    Geography %in% c(state.name, 'District of Columbia', 'United States')
  ) %>%
  mutate(
    insurance = factor(
      insurance,
      levels = c(
        "Uninsured",
        "Any Medicaid",
        "Private Insurance Only",
        "Other"
      ),
      labels = c('Uninsured', 'Medicaid', 'Private', 'Other')
    ),
    Vaccine_dose =gsub('NA','', Vaccine_dose),
    Vaccine_dose = trimws(Vaccine_dose)
    
  ) %>%
  dplyr::select(Geography, insurance, birth_year, Vaccine_dose, vax_uptake) %>%
  rename(geography = Geography, vaccine = Vaccine_dose, value = vax_uptake)

log_write(
  vax_insurance,
  './Data/Webslim/childhood_immunizations/rates_by_insurance.parquet'
)

vax_epic <- read_parquet(
  paste0(base_dir, '/vax_age_cosmos.parquet')
) %>%
  filter(
    geography %in% c(state.name, 'District of Columbia', 'United States')
  ) %>%
  dplyr::select(geography, age_level, Outcome_value1, Outcome_value2) %>%
  rename(value = Outcome_value1, age = age_level, N_patients = Outcome_value2)

log_write(
  vax_epic,
  './Data/Webslim/childhood_immunizations/mmr_rates_epic.parquet'
)

files <- c(
  "childhood_immunizations/az_vaccines.csv",
  "childhood_immunizations/exemption_state_kg_school_vax_view.csv",
  "childhood_immunizations/state_kg_school_vax_view.csv"
)
for (file in files) {
  data <- read.csv(paste0(base_dir, file))
  log_write(data, paste0("./Data/Webslim/", sub("csv$", "parquet", file)))
}

############################################################
#Compare Epic Cosmos ( 1dose), NIS (1+ doses),and Epic (1+ dose)
vaxview <- read_parquet(
  './Data/Webslim/childhood_immunizations/state_kg_school_vax_view.parquet'
) %>%
  filter(vax == 'mmr' & year == '2023-24') %>%
  rename(value_vaxview = value) %>%
  dplyr::select(value_vaxview, geography) %>%
  mutate(value_vaxview = as.numeric(value_vaxview))

nis <- read_parquet(
  './Data/Webslim/childhood_immunizations/overall_rates.parquet'
) %>%
  filter(
    vaccine == '≥1 Dose MMR ' & age == "35 Months" & birth_year == 2021
  ) %>%
  rename(value_nis = value) %>%
  dplyr::select(value_nis, geography)

vax_epic <- read_parquet(
  './Data/Webslim/childhood_immunizations/mmr_rates_epic.parquet'
) %>%
  rename(value_epic = value) %>%
  filter(age == '3-4 Years') %>%
  dplyr::select(value_epic, geography)

vax_compare <- nis %>%
  full_join(vaxview, by = 'geography') %>%
  full_join(vax_epic, by = 'geography') %>%
  dplyr::select(geography, value_nis, value_vaxview, value_epic)

log_write(
  vax_compare,
  './Data/Webslim/childhood_immunizations/state_compare.parquet'
)

#################################################
## Chronic diseases
#################################################
#saveRDS(diabetes_obesity_state, "Data/Plot Files/Cosmos ED/diabetes_obesity_state.rds")

#diab_obesity <- read_parquet('https://github.com/ysph-dsde/PopHIVE_DataHub/raw/refs/heads/main/Data/Plot Files/Cosmos ED/diabetes_obesity.parquet') %>%
diabetes_obesity_state1 <- readRDS(
  "Data/Plot Files/Cosmos ED/diabetes_obesity_state.rds"
) %>%
  st_drop_geometry() %>%
  as.data.frame() %>%
  dplyr::select(
    geography,
    age_level,
    outcome_name,
    Outcome_value1,
    pct_captured
  ) %>%
  rename(value = Outcome_value1, age = age_level) %>%
  filter(!is.na(age))

diabetes_obesity_state2 <- diabetes_obesity_state1 %>%
  filter(outcome_name != 'N_patients') %>%
  dplyr::select(-pct_captured) %>%
  mutate(outcome_name = tools::toTitleCase(outcome_name)) %>%
  filter(geography %in% c(state.name, 'District of Columbia', 'United States'))

diabetes_obesity_state3 <- diabetes_obesity_state1 %>%
  filter(outcome_name == 'N_patients') %>%
  dplyr::select(-outcome_name) %>%
  rename(N_patients = value) %>%
  full_join(diabetes_obesity_state2, by = c('geography', 'age')) %>%
  filter(!is.na(outcome_name)) %>%
  mutate(suppressed_flag = if_else(is.na(value), 1, 0)) %>%
  dplyr::select(geography, age, outcome_name, value, pct_captured)


log_write(
  diabetes_obesity_state3,
  './Data/Webslim/chronic_diseases/prevalence_by_geography.parquet'
)


diab_obesity_county1 <- readRDS(
  "Data/Plot Files/Cosmos ED/diabetes_obesity_county.rds"
) %>%
  st_drop_geometry() %>%
  as.data.frame() %>%
  dplyr::select(fips, age, pct_diabetes, pct_obesity, pct_captured) %>%
  reshape2::melt(., id.vars = c('fips', 'age'))


diab_obesity_county2 <- diab_obesity_county1 %>%
  filter(variable != 'pct_captured') %>%
  mutate(
    outcome_name = if_else(
      variable == 'pct_diabetes',
      'Diabetes',
      if_else(variable == 'pct_obesity', 'Obesity', NA_character_)
    )
  ) %>%
  dplyr::select(fips, age, outcome_name, value)

diab_obesity_county3 <- diab_obesity_county1 %>%
  filter(variable == 'pct_captured') %>%
  rename(pct_captured = value) %>%
  dplyr::select(fips, age, pct_captured) %>%
  full_join(diab_obesity_county2, by = c('fips', 'age')) %>%
  mutate(suppressed_flag = if_else(is.na(value), 1, 0))


log_write(
  diab_obesity_county3,
  './Data/Webslim/chronic_diseases/prevalence_by_geography_county.parquet'
)

'Have you ever been told by a doctor that you have diabetes?'

diabetes <- open_dataset(
  './Data/Pulled Data/BRFSS/brfss_prevalence.parquet'
) %>%
  filter(class == "Chronic Health Indicators" & topic == 'Diabetes') %>%
  filter(
    break_out_category %in% c("Age Group", 'Overall') & response == 'Yes'
  ) %>%
  dplyr::select(locationdesc, year, break_out, data_value, sample_size) %>%
  collect() %>%
  mutate(outcome_name = 'Diabetes') %>%
  rename(age = break_out, value = data_value, geography = locationdesc) %>%
  mutate(
    age = if_else(age == 'Overall', 'Total', paste(age, 'Years')),
    geography = if_else(
      geography == 'All States and DC (median) **',
      'United States',
      geography
    )
  ) %>%
  filter(geography != 'All States, DC and Territories (median) **')

#########################################
#Weight classification by Body Mass Index (BMI) (variable calculated from one or more BRFSS questions)
#Obese (BMI 30.0 - 99.8)
#########################################

obesity <- open_dataset('./Data/Pulled Data/BRFSS/brfss_prevalence.parquet') %>%
  filter(
    class == "Overweight and Obesity (BMI)" & topic == 'BMI Categories'
  ) %>%
  filter(
    break_out_category %in% c("Age Group", 'Overall') & grepl('Obese', response)
  ) %>%
  dplyr::select(locationdesc, year, break_out, data_value, sample_size) %>%
  collect() %>%
  mutate(outcome_name = 'Obesity') %>%
  rename(age = break_out, value = data_value, geography = locationdesc) %>%
  mutate(
    age = if_else(age == 'Overall', 'Total', paste(age, 'Years')),
    geography = if_else(
      geography == 'All States and DC (median) **',
      'United States',
      geography
    )
  ) %>%
  filter(geography != 'All States, DC and Territories (median) **')

combined <- bind_rows(obesity, diabetes)

log_write(
  combined,
  './Data/Webslim/chronic_diseases/brfss_prevalence_by_geography.parquet'
)


epic_compare <- read_parquet(
  './Data/Webslim/chronic_diseases/prevalence_by_geography.parquet'
) %>%
  rename(value_epic = value, epic_pct_captured = pct_captured) %>%
  left_join(combined, by = c('geography', 'age', 'outcome_name')) %>%
  filter(year == 2023)

log_write(
  epic_compare,
  './Data/Webslim/chronic_diseases/brfss_cosmos_prevalence_compared.parquet'
)
