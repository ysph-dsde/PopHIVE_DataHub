
#########################
#Chronic
#########################
epic_import_chronic <- function(ds_name, skipN=12) {
  
  ds_out <- readr::read_csv(ds_name, skip=skipN, col_names=T) %>%
    rename(geography=`State of Residence (U.S.)`, age=`Age in Years (Current)`,  
           Number_Patients="...3",
           pct_diabetes ="...4",
           pct_obesity ="...5",
           
           ) %>%
    tidyr::fill( geography, age,  .direction = 'down') %>%
    dplyr::select(geography, age, Number_Patients,pct_diabetes,pct_obesity )%>%
    arrange(geography, age) %>%
    #week END date
    ungroup() %>%
    mutate( pct_diabetes = if_else(pct_diabetes=='-',NA_character_, pct_diabetes),
            pct_diabetes=gsub('%','', pct_diabetes),
            pct_diabetes = as.numeric(pct_diabetes),
            
            pct_obesity = if_else(pct_obesity=='-',NA_character_, pct_obesity),
            pct_obesity=gsub('%','', pct_obesity),
            pct_obesity = as.numeric(pct_obesity),
            
            geography= if_else(geography=='Total', 'United States', geography)) %>%
    mutate( Level = if_else(toupper(age) %in% toupper(c('Less than 10 Years')), '<10 Years',
                            if_else( toupper(age) %in%  toupper(c('≥ 10 and < 15 Years','? 10 and < 15 years')),'10-14 Years',
                                     if_else(toupper(age) %in% toupper(c('≥ 15 and < 20 Years','? 15 and < 20 years')) , "15-19 Years",
                                             if_else( toupper(age) %in% toupper(c("≥ 20 and < 40 Years",'? 20 and < 40 years')) ,"20-39 Years" ,         
                                                      if_else( toupper(age) %in% toupper(c("≥ 40 and < 65 Years",'? 40 and < 65 years')) ,"40-64 Years" ,         
                                                               if_else( toupper(age) %in% toupper(c('65 years or more')) , "65+ Years" , 
                                                                        if_else(age=='Total','Total',NA_character_        
                                                                                
                                                                        ) ))))))
    ) %>%
    dplyr::select(-age) %>%
    arrange( geography,Level) %>%
    filter(!is.na(geography))
  
  return(ds_out)
}
