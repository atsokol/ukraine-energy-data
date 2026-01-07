# Load required packages and functions
library(dplyr)
library(tidyr)
library(purrr)
library(readr)
library(lubridate)
library(entsoeapi)

# Read ENTSOE_PAT from environment variable
Sys.setenv(ENTSOE_PAT = Sys.getenv("ENTSOE_PAT"))

source("src/helper_func_EU.R")
source("src/helper_func_BM_entsoe.R")

# Define ENTSO-E zones and generation types
zones <- c(
  PL = "10YPL-AREA-----S",
  RO = "10YRO-TEL------P",
  HU = "10YHU-MAVIR----U",
  SK = "10YSK-SEPS-----K"
)

gen_types <- c("B16", "B19") # Solar and Wind onshore

# Define end date (as Date object)
end_date <- floor_date(today(), "month") - days(1)  # Last day of previous month

# Determine start dates for each dataset (as Date objects)
gen_start <- get_start_date("data/data_raw/yield_RES_EU.csv")
price_start <- get_start_date("data/data_raw/DAM_EU.csv")
load_start <- get_start_date("data/data_raw/load_EU.csv")
bm_start <- get_start_date("data/data_raw/BM_EU.csv", date_col = "datetime")

# Download and update RES generation data
if (gen_start <= end_date) {
  # Convert Date to datetime for API calls
  gen_start_dt <- ymd_hms(paste(gen_start, "00:00:00"), tz = "UTC")
  end_date_dt <- ymd_hms(paste(end_date + days(1), "00:00:00"), tz = "UTC")
  
  new_gen <- download_gen_eu(zones, gen_types, gen_start_dt, end_date_dt)
  update_csv_file(new_gen, "data/data_raw/yield_RES_EU.csv", gen_start, end_date)
} else {
  message("RES generation data is up to date")
}

# Download and update DAM price data
if (price_start <= end_date) {
  # Convert Date to datetime for API calls
  price_start_dt <- ymd_hms(paste(price_start, "00:00:00"), tz = "UTC")
  end_date_dt <- ymd_hms(paste(end_date + days(1), "00:00:00"), tz = "UTC")
  
  new_price <- download_price_eu(zones, price_start_dt, end_date_dt)
  update_csv_file(new_price, "data/data_raw/DAM_EU.csv", price_start, end_date)
} else {
  message("DAM price data is up to date")
}

# Download and update load data
if (load_start <= end_date) {
  # Convert Date to datetime for API calls
  load_start_dt <- ymd_hms(paste(load_start, "00:00:00"), tz = "UTC")
  end_date_dt <- ymd_hms(paste(end_date + days(1), "00:00:00"), tz = "UTC")
  
  new_load <- download_load_eu(zones, load_start_dt, end_date_dt)
  update_csv_file(new_load, "data/data_raw/load_EU.csv", load_start, end_date)
} else {
  message("Load data is up to date")
}

# Download and update balancing market price data
if (bm_start <= end_date) {
  # Convert Date to datetime for API calls
  bm_start_dt <- ymd_hms(paste(bm_start, "00:00:00"), tz = "UTC")
  end_date_dt <- ymd_hms(paste(end_date + days(1), "00:00:00"), tz = "UTC")
  
  new_bm <- download_balancing_prices_eu(zones, bm_start_dt, end_date_dt, chunk_days = 90)
  update_csv_file(new_bm, "data/data_raw/BM_EU.csv", bm_start, end_date)
} else {
  message("Balancing market price data is up to date")
}

# bm_start_dt <- ymd_hms(paste("2024-09-17", "00:00:00"), tz = "UTC")
# end_date_dt <- ymd_hms(paste("2025-12-31", "23:45:00"), tz = "UTC")
# new_bm <- download_balancing_prices_eu(zones["PL"], bm_start_dt, end_date_dt, chunk_days = 60)
