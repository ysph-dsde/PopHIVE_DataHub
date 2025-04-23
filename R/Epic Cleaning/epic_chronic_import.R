
#########################
#Chronic
#########################
epic_import_chronic <- function(ds_name, skipN=12) {
  
  ds_out <- readr::read_csv(ds_name, skip=skipN, col_names=T) %>%
    rename(geography=`State of Residence (U.S.)`, age=`Age in Years (Current)`,  pct="...4") %>%
    tidyr::fill( Measures,geography, age,  .direction = 'down') %>%
    dplyr::select(Measures,geography, age, pct )%>%
    arrange(geography, age) %>%
    #week END date
    ungroup() %>%
    mutate( pct = if_else(pct=='-',NA_character_, pct),
            pct=gsub('%','', pct),
            pct = as.numeric(pct),
            geography= if_else(geography=='Total', 'United States', geography)) %>%
    mutate( Level = if_else(toupper(age) %in% toupper(c('Less than 10 Years')), '<10 Years',
                            if_else( toupper(age) %in%  toupper(c('≥ 10 and < 15 Years','? 10 and < 15 years')),'10-14 Years',
                                     if_else(toupper(age) %in% toupper(c('≥ 15 and < 20 Years','? 15 and < 20 years')) , "15-19 Years",
                                             if_else( toupper(age) %in% toupper(c("≥ 20 and < 40 Years",'? 20 and < 40 years')) ,"20-39 Years" ,         
                                                      if_else( toupper(age) %in% toupper(c("≥ 40 and < 65 Years",'? 40 and < 65 years')) ,"40-64 Years" ,         
                                                               if_else( toupper(age) %in% toupper(c('65 years or more')) , "65+ Years" , 
                                                                        if_else(age=='Total','Total',NA_character_        
                                                                                
                                                                        ) )))))),
            Measures = if_else(grepl('BMI',Measures),'obesity',
                               if_else(grepl('A1C',Measures) , 'diabetes','none' ))
    ) %>%
    dplyr::select(-age) %>%
    arrange(Measures, geography,Level) %>%
    filter(!is.na(geography))
  
  return(ds_out)
}
