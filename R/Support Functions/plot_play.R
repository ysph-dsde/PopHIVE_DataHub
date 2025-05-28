library(tidyverse)
library(arrow)
library(patchwork)

vax_compare <- read_parquet( './Data/Webslim/childhood_immunizations/state_compare.parquet') 
  


p1 <- ggplot(vax_compare,aes(x=value_nis, y=value_vaxview)) +
  geom_point() +
  geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed")+
  theme_classic() +
  ylim(75, 100)+
  xlim(75,100) +
  ggtitle('Comparison of uptake from NIS and SchoolVaxView')


p2 <- ggplot(vax_compare,aes(x=value_nis, y=value_epic)) +
  geom_point() +
  geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed")+
  theme_classic() +
  ylim(75, 100)+
  xlim(75,100)+
  ggtitle('Comparison of uptake from NIS and Epic')

p3 <- ggplot(vax_compare,aes(x=value_vaxview, y=value_epic)) +
  geom_point() +
  geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed")+
  theme_classic() +
  ylim(75, 100)+
  xlim(75,100)+
  ggtitle('Comparison of uptake from Epic and SchoolVaxView')

p1+p2+p3



#Trends in vaxview data

vaxview <- read_parquet('./Data/Webslim/childhood_immunizations/state_kg_school_vax_view.parquet') %>%
  filter(vax=='mmr' )

vaxview %>%
  rename(schoolyear=year) %>%
  mutate(year=as.numeric(substr(schoolyear,1,4)),
         value=as.numeric(value)) %>%
  filter(geography %in% c('United States', 'Texas', 'Arizona', 'Idaho', "Massachusetts")) %>%
ggplot(aes(x=year, y=value, group=geography, color=geography)) +
  geom_line() +
  theme_classic() +
  ylab('uptake')



#######################################
#Chronic disease compare
#######################################

epic_compare <- read_parquet('./Data/Webslim/chronic_diseases/brfss_cosmos_prevalence_compared.parquet')


epic_compare %>%
  filter(outcome_name=='Obesity') %>%
  ggplot(aes(x=value_epic, y=value, group=age, color=age, size=epic_pct_captured, alpha=0.5))+
  geom_point() +
  theme_classic() +
  ylim(15,70) +
  xlim(15,70) +
  ylab('% BRFSS')+
  xlab('% Epic Cosmos')+
  geom_abline(intercept=0, slope=1) +
  ggtitle('Obesity: Epic Cosmos vs BRFSS')

epic_compare %>%
  filter(outcome_name=='Diabetes') %>%
  ggplot(aes(x=value_epic, y=value, group=age, color=age, size=epic_pct_captured, alpha=0.5))+
  geom_point() +
  theme_classic() +
  ylim(0,40) +
  xlim(0,40) +
  ylab('% BRFSS')+
  xlab('% Epic Cosmos')+
  geom_abline(intercept=0, slope=1) +
  ggtitle('Diabetes: Epic Cosmos vs BRFSS')
