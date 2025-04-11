#Function to format the data from epic cosmos, which is in a long format

#ds_name='RSV/07042025_RSV_EDvisits_ICD10s_State_Week_Age_2023-2025.csv'
epic_age_import <- function(ds_name, skipN=13) {
  
  ds_out <- readr::read_csv(ds_name, skip=skipN, col_names=T) %>%
    rename(geography=`State of Residence (U.S.)`, age=`Age at Time of Visit`, year=Year, week=Week, N_cases="...5") %>%
    tidyr::fill( geography, age, year, week, .direction = 'down') %>%
    mutate(year_start= sub("^(.*?)\\s*–.*$", "\\1", year),
           day_month_start= sub("^(.*?)\\s*–.*$", "\\1", week),
           week_start = as.Date(paste(year_start, day_month_start, sep='-'), "%Y-%b %d"),
           week_end= lubridate::ceiling_date(week_start, unit='week') -1
    ) %>%
    dplyr::select(geography, age, week_end, N_cases )%>%
    arrange(geography, age, week_end) %>%
    #week END date
    ungroup() %>%
    mutate( N_cases = if_else(N_cases=='10 or fewer',NA_character_, N_cases),
            N_cases = as.numeric(N_cases),
            geography= if_else(geography=='Total', 'United States', geography)) %>%
    mutate( Level = if_else(toupper(age) %in% toupper(c('Less than 1 years', 'Less than 1 year')), '<1 Years',
                            if_else( toupper(age) %in%  toupper(c('≥ 1 and < 5 years','1 year or more and less than 5 years')),'1-4 Years',
                                     if_else(toupper(age) %in% toupper(c('≥ 5 and < 18 years','5 years or more and less than 18 years (1)')) , "5-17 Years",
                                             if_else( toupper(age) %in% toupper(c("≥ 18 and < 50 years",'18 years or more and less than 50 years')) ,"18-49 Years" ,         
                                                      if_else( toupper(age) %in% toupper(c("≥ 50 and < 64 years",'50 years or more and less than 64 years')) ,"50-64 Years" ,         
                                                               if_else( toupper(age) %in% toupper(c("65 years or more","≥ 65 and < 110 years")) , "65+ Years" , 
                                                                        if_else(age=='Total','Total',NA_character_        
                                                                                
                                                                        ) ))))))) %>%
    dplyr::select(-age) %>%
    arrange( geography,Level, week_end) %>%
    group_by( geography,Level) %>%
    rename(N_cases_epic=N_cases) %>%
    filter(week_end>='2023-07-01') %>%
    ungroup() %>%
    rename(date=week_end) %>%
    filter(!is.na(geography))
  
  return(ds_out)
}