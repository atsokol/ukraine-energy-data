library(httr)
library(jsonlite)

source("download_functions.R")

# Generate a sequence of dates for the desired year
dates <- seq(as.Date("2025-01-01"), Sys.Date(), by = "day")

# Loop through the dates and download the data
# gen = "1" is solar and gen = "2" is wind
solar_list <- download_data_list(dates, gen = "1")
data_solar <- do.call(rbind, solar_list)

wind_list <- download_data_list(dates, gen = "2")
data_wind <- do.call(rbind, wind_list)

