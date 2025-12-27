
#============================================================================
# Functions to download data for EU countries from ENTSO-E
#============================================================================

# Download RES generation data
download_gen_eu <- function(start_date, end_date) {
  
  year_starts <- seq(start_date, end_date, by = "1 year")
  year_ends <- lead(year_starts, default = end_date)
  
  gen_all <- map_df(names(zones), function(country) {
    map_df(seq_along(year_starts), function(i) {
      
      map_df(gen_types, possibly(function(gen_type) {
        gen_raw <- gen_per_prod_type(
          eic = zones[country],
          period_start = year_starts[i],
          period_end   = year_ends[i],
          gen_type     = gen_type,
          tidy_output  = TRUE
        )
        
        gen_raw |>
          mutate(
            country = country,
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
  
  gen_all |>
    complete(
      country, 
      hour, 
      tech, 
      fill = list(gen_mw = 0)
    )
}

# Download DAM price data 
download_price_eu <- function(start_date, end_date) {
  
  year_starts <- seq(start_date, end_date, by = "1 year")
  year_ends <- lead(year_starts, default = end_date)
  
  map_df(names(zones), function(country) {
    map_df(seq_along(year_starts), function(i) {
      px_raw <- transm_day_ahead_prices(
        eic = zones[country],
        period_start = year_starts[i],
        period_end   = year_ends[i],
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
        ) |> 
        filter(
          hour >= start_date,
          hour < end_date
        )
    })
  })
}

# Download total load data
download_load_eu <- function(start_date, end_date) {
  
  year_starts <- seq(start_date, end_date, by = "1 year")
  year_ends <- lead(year_starts, default = end_date)
  
  map_df(names(zones), function(country) {
    map_df(seq_along(year_starts), function(i) {
      load_raw <- load_actual_total( 
        eic = zones[country],
        period_start = year_starts[i],
        period_end   = year_ends[i],
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
        ) |> 
        filter(
          hour >= start_date,
          hour < end_date
        )
    })
  })
}