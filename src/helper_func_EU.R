#============================================================================
# Functions to download data for EU countries from ENTSO-E
#============================================================================

# Function to determine start date based on existing file (returns Date)

get_start_date <- function(filepath, date_col = "hour", default_start = as.Date("2022-01-01")) {
  if (file.exists(filepath)) {
    existing_data <- read_csv(filepath, show_col_types = FALSE)
    
    # Parse ISO 8601 datetime format and convert to Date
    parsed_dates <- as.Date(ymd_hms(existing_data[[date_col]], tz = "UTC"))
    last_date <- max(parsed_dates, na.rm = TRUE)
    
    # Check if parsing was successful
    if (is.na(last_date) || !is.finite(last_date)) {
      warning("Could not parse dates from ", filepath, ". Using default start date.")
      return(default_start)
    }
    
    return(last_date + days(1))
  } else {
    return(default_start)
  }
}

# Download RES generation data
download_gen_eu <- function(zones, gen_types, start_datetime, end_datetime, chunk_days = 365) {
  date_chunks <- create_date_chunks(start_datetime, end_datetime, chunk_days = chunk_days)
  
  gen_all <- map_df(names(zones), function(z) {
    map_df(date_chunks, function(chunk) {
      
      map_df(gen_types, possibly(function(gen_type) {
        gen_raw <- gen_per_prod_type(
          eic = zones[z],
          period_start = chunk$start,
          period_end   = chunk$end,
          gen_type     = gen_type,
          tidy_output  = TRUE
        )
        
        gen_raw |>
          mutate(
            country = z,
            tech = recode(ts_mkt_psr_type,
                          "B16" = "Solar", 
                          "B19" = "Wind onshore"),
            hour = floor_date(ts_point_dt_start, unit = "hour")
          ) |>
          group_by(country, hour, tech) |>
          summarise(
            gen_mw = mean(ts_point_quantity, na.rm = TRUE),
            .groups = "drop"
          )
      }, otherwise = tibble()))
    })
  })
  
  # Complete missing combinations AFTER collecting all data
  gen_all |>
    complete(
      country, 
      hour, 
      tech, 
      fill = list(gen_mw = 0)
    )
}

# Download DAM price data 
download_price_eu <- function(zones, start_datetime, end_datetime, chunk_days = 365) {
  date_chunks <- create_date_chunks(start_datetime, end_datetime, chunk_days = chunk_days)
  
  map_df(names(zones), function(country) {
    map_df(date_chunks, function(chunk) {
      px_raw <- transm_day_ahead_prices(
        eic = zones[country],
        period_start = chunk$start,
        period_end   = chunk$end,
        tidy_output  = TRUE
      )
      
      px_raw |>
        mutate(
          country = country,
          hour = floor_date(ts_point_dt_start, unit = "hour")
        ) |>
        group_by(country, hour) |>
        summarise(
          price_eur = mean(ts_point_price_amount, na.rm = TRUE),
          .groups = "drop"
        )
    })
  }) |>
    filter(
      hour >= start_datetime,
      hour < end_datetime
    )
}

# Download total load data
download_load_eu <- function(zones, start_datetime, end_datetime, chunk_days = 365) {
  date_chunks <- create_date_chunks(start_datetime, end_datetime, chunk_days = chunk_days)
  
  map_df(names(zones), function(country) {
    map_df(date_chunks, function(chunk) {
      load_raw <- load_actual_total( 
        eic = zones[country],
        period_start = chunk$start,
        period_end   = chunk$end,
        tidy_output  = TRUE
      )
      
      load_raw |>
        mutate(
          country = country,
          hour = floor_date(ts_point_dt_start, unit = "hour")
        ) |>
        group_by(country, hour) |>
        summarise(
          volume = mean(ts_point_quantity, na.rm = TRUE),
          .groups = "drop"
        )
    })
  }) |>
    filter(
      hour >= start_datetime,
      hour < end_datetime
    )
}


# Create date chunks for API requests
create_date_chunks <- function(start_datetime, end_datetime, chunk_days = 365) {
  date_chunks <- list()
  current_start <- start_datetime
  
  while (current_start < end_datetime) {
    chunk_end <- min(current_start + days(chunk_days), end_datetime)
    date_chunks[[length(date_chunks) + 1]] <- list(
      start = current_start,
      end = chunk_end
    )
    current_start <- chunk_end
  }
  
  return(date_chunks)
}

# Update CSV file with new data
update_csv_file <- function(new_data, filepath, start_date, end_date) {
  if (nrow(new_data) > 0) {
    if (file.exists(filepath)) {
      existing_data <- read_csv(filepath, show_col_types = FALSE)
      combined_data <- bind_rows(existing_data, new_data)
    } else {
      combined_data <- new_data
    }
    
    write_csv(combined_data, filepath)
    message(basename(filepath), " updated from ", start_date, " to ", end_date)
  } else {
    message("No new data retrieved for ", basename(filepath))
  }
}

# Function to get ECB exchange rates and convert prices to EUR
convert_to_eur <- function(df, date_col = "hour", currency_col = "currency", 
                          price_col = "price", start_date = "2022-01-01", 
                          end_date = "2025-12-31") {
  
  currencies_to_convert <- unique(df[[currency_col]])
  currencies_to_convert <- currencies_to_convert[currencies_to_convert != "EUR"]
  
  # Download rates starting 5 days earlier to handle holidays at year start
  fx_start_date <- as.character(as.Date(start_date) - days(5))
  
  exchange_rates <- map_df(currencies_to_convert, function(curr) {
    message("Downloading exchange rates for ", curr)
    get_ecb_exchange_rate(curr, fx_start_date, end_date) |>
      mutate(currency = curr)
  }) |>
    # Fill forward rates for weekends/holidays
    group_by(currency) |>
    complete(date = seq(min(date), max(date), by = "1 day")) |>
    fill(rate, .direction = "down") |>
    ungroup()
  
  df |>
    mutate(date = as.Date(.data[[date_col]])) |>
    left_join(exchange_rates, by = c("currency", "date")) |>
    mutate(
      price_eur = if_else(
        .data[[currency_col]] == "EUR",
        .data[[price_col]],
        .data[[price_col]] * rate
      )
    ) |>
    select(-date, -rate)
}
