library(tidyverse)
library(arrow)

vax_compare <- read_parquet( './Data/Webslim/childhood_immunizations/state_compare.parquet') 
  


ggplot(vax_compare,aes(x=value_nis, y=value_vaxview)) +
  geom_point() +
  geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed")+
  theme_classic() +
  ylim(75, 100)+
  xlim(75,100)


ggplot(vax_compare,aes(x=value_nis, y=value_epic)) +
  geom_point() +
  geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed")+
  theme_classic() +
  ylim(75, 100)+
  xlim(75,100)

ggplot(vax_compare,aes(x=value_vaxview, y=value_epic)) +
  geom_point() +
  geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed")+
  theme_classic() +
  ylim(75, 100)+
  xlim(75,100)
