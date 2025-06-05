
#########################
#testing
#########################
epic_viral_testing <- function(ds_name, skipN=11) {
  
  ds_out <- readr::read_csv(ds_name, skip=skipN, col_names=T) %>%
    rename(age=`Age in Years`,  
           N_J12_J18="...4",
           pct_tested_rsv ="...5"
           
           ) %>%
    tidyr::fill( Year, Month, age,  .direction = 'down') %>%
    arrange(age, Year, Month) %>%
    #week END date
    ungroup() %>%
    mutate( date= as.Date(paste(Year, Month, '01', sep='-'), '%Y-%b-%d'),
            #date= paste(Year, Month, '01', sep='-'),
            pct_tested_rsv = if_else(pct_tested_rsv=='-',NA_character_, pct_tested_rsv),
            pct_tested_rsv=gsub('%','', pct_tested_rsv),
            pct_tested_rsv = as.numeric(pct_tested_rsv),
            
     Level = if_else(toupper(age) %in% toupper(c('Less than 5 years')), '<5 Years',
                            if_else( toupper(age) %in%  toupper(c('≥ 5 and < 18 years')),'5-17 Years',
                                     if_else(toupper(age) %in% toupper(c("≥ 18 and < 40 years")) , "18-39 Years",
                                             if_else( toupper(age) %in% toupper(c("≥ 40 and < 65 years" )) ,"40-64 Years" ,         
                                                      if_else( toupper(age) %in% toupper(c( "65 years or more" )) ,"65+ Years" ,         
                                                                        if_else(age=='Total','Total',NA_character_        
                                                                                
                                                                        ) )))))
    ) %>%
    dplyr::select(-age) %>%
    arrange( Level, date) 

  return(ds_out)
}
