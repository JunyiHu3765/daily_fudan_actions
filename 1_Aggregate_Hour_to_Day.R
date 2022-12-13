#-----------------------------------------------------------------------------------------#
#---------This script is used to aggregate the hour-level data to the day-level-----------#
#-----------------------------------------------------------------------------------------#

getwd()
setwd("..\\")

# Install and import packages
packages <- c("readr", "ggplot2", "colorspace", "lubridate", "forecast",
              "astsa", "xts", "stringr", "tibble", "tidyverse", "data.table",
              "R.utils", "scales", "TTR", "tidyr", "parallel")
ipak <- function(pkg){
  new.pkg <- pkg[!(pkg %in% installed.packages()[, "Package"])]
  if (length(new.pkg)) 
    install.packages(new.pkg, dependencies = TRUE)
  sapply(pkg, require, character.only = TRUE)
}
ipak(packages)

 temp <- str_c("Data/Raw_2", list.files(path = "Data/Raw_2", pattern = ".gz", recursive = T), sep = "/")

# Aggregate data
## how many days will the aggregated dataset contain
patch <- 30
## merge
for (j in seq(0, 35)){
  merge_data <- fread(temp[1 + patch*j]) %>%
    select(symbol_id, time_period_start, px_open, px_high, px_low, sx_cnt, sx_sum) %>%
    mutate(day = floor_date(time_period_start, "day"),
           volume = px_open * sx_sum) %>%
    group_by(symbol_id, day) %>%
    summarize(price = mean(px_open),
              price_high = max(px_high),
              price_low = min(px_low),
              n_trade = sum(sx_cnt),
              volume = sum(volume))
  
  for (i in seq(2 + patch*j, patch + patch*j)){
    merge_data <- fread(temp[i]) %>%
      select(symbol_id, time_period_start, px_open, px_high, px_low, sx_cnt, sx_sum) %>%
      mutate(day = floor_date(time_period_start, "day"),
             volume = px_open * sx_sum) %>%
      group_by(symbol_id, day) %>%
      summarize(price = mean(px_open),
                price_high = max(px_high),
                price_low = min(px_low),
                n_trade = sum(sx_cnt),
                volume = sum(volume)) %>%
      rbind(merge_data)
  }
  write.csv(merge_data, file = str_c("Data/Day/final_", j, ".csv"))
}