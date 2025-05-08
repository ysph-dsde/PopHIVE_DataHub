#Function to format the data from epic cosmos, which is in a long format

#ds_name <- './Data/Archive/Cosmos ED/Immunizations/mmr_age_state_2025_04_18.csv'
#ds_name='RSV/07042025_RSV_EDvisits_ICD10s_State_Week_Age_2023-2025.csv'
epic_vax_import <- function(ds_name, skipN=12) {
  
  ds_out <- readr::read_csv(ds_name, skip=skipN, col_names=T) %>%
    rename(geography=`State of Residence (U.S.)`, age=`Age in Years (Current)`, N_patients='...3', pct_vax="...4") %>%
    tidyr::fill( geography, age,  .direction = 'down') %>%
 
    dplyr::select(geography, age,  N_patients,pct_vax )%>%
    arrange(geography, age) %>%
    #week END date
    ungroup() %>%
    mutate( pct_vax = gsub('%','', pct_vax),
            pct_vax = if_else(pct_vax=='-',NA_character_, pct_vax),
            pct_vax = as.numeric(pct_vax),
            geography= if_else(geography=='Total', 'United States', geography)) %>%
    mutate( Level = if_else(toupper(age) %in% toupper(c('? 1 and < 2 years')), '<1 Years',
                            if_else( toupper(age) %in%  toupper(c('? 1 and < 2 years')),'1-2 Years',
                                     if_else(toupper(age) %in% toupper(c('? 2 and < 3 years')) , "2-3 Years",
                                             if_else( toupper(age) %in% toupper(c("? 3 and < 4 years")) ,"3-4 Years" ,         
                                                      if_else( toupper(age) %in% toupper(c("? 4 and < 5 years")) ,"4-5 Years" ,         
                                                               if_else( toupper(age) %in% toupper(c("? 5 and < 6 years")) , "5-6 Years" , 
                                                                        if_else(age=="6 years or more",'6+ years',NA_character_        
                                                                                
                                                                        ) ))))))) %>%
    arrange( geography,Level) %>%
    group_by( geography,Level) %>%
    rename(pct_vax_epic=pct_vax) %>%
    filter(!is.na(Level)) %>%
    dplyr::select(-age) %>%
    ungroup() %>%
    filter(!is.na(geography))
  
  return(ds_out)
}
