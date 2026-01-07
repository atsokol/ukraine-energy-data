#============================================================================
# Functions to download balancing market data
#============================================================================

library(chromote)
library(rvest)
library(readxl)
library(stringr)
library(dplyr)
library(tidyr)
library(lubridate)

#============================================================================
# Ukraine Balancing Market Functions
#============================================================================

# Function to get all BM file URLs from the website
get_all_bm_urls <- function() {
  # Use chromote to bypass Cloudflare protection
  b <- ChromoteSession$new()
  
  # Navigate to the page
  b$Page$navigate("https://ua.energy/uchasnikam_rinku/rezultaty-balansuyuchogo-rynku-2/")
  Sys.sleep(5)  # Wait for page to load
  
  # Get HTML content
  html_content <- b$Runtime$evaluate("document.documentElement.outerHTML")$result$value
  b$close()
  
  # Parse HTML
  page <- read_html(html_content)
  
  # Use XPath to find the specific div
  bm_div <- page |>
    html_node(xpath = '//*[@id="1590479495940-174989ce-bac9"]')
  
  if (is.na(bm_div)) {
    message("Specific div not found, searching entire page...")
    bm_div <- page
  }
  
  # Extract all xlsx links
  all_links <- bm_div |>
    html_nodes("a") |>
    html_attr("href") |>
    str_subset("\\.xlsx$")
  
  return(all_links)
}

# Function to extract date from BM filename
extract_bm_date <- function(filename) {
  # Extract month name from filename
  month_match <- str_extract(filename, "(sichen|lyutyj|berezen|kviten|traven|cherven|lypen|serpen|veresen|zhovten|lystopad|gruden)")
  
  # Month name mapping (Ukrainian to number)
  month_map <- c(
    "sichen" = 1, "lyutyj" = 2, "berezen" = 3, "kviten" = 4,
    "traven" = 5, "cherven" = 6, "lypen" = 7, "serpen" = 8,
    "veresen" = 9, "zhovten" = 10, "lystopad" = 11, "gruden" = 12
  )
  
  # Extract year
  year_match <- str_extract(filename, "20\\d{2}")
  
  if (!is.na(month_match) && !is.na(year_match)) {
    month_num <- month_map[month_match]
    return(as.Date(paste(year_match, month_num, "01", sep = "-")))
  }
  
  return(NA)
}

# Function to download and process a single BM file
download_bm_file <- function(url) {
  filename <- basename(url)
  temp_file <- file.path(tempdir(), filename)
  
  message("Downloading: ", filename)
  
  tryCatch({
    # Use chromote to download
    b <- ChromoteSession$new()
    
    b$Browser$setDownloadBehavior(
      behavior = "allow",
      downloadPath = tempdir()
    )
    
    b$Page$navigate(url)
    Sys.sleep(10)
    b$close()
    
    # Find downloaded file
    files <- list.files(tempdir(), pattern = basename(url), full.names = TRUE)
    
    if (length(files) == 0) {
      message("  Download failed")
      return(tibble())
    }
    
    # Read Excel file, skip first 2 rows
    bm_data <- read_excel(files[1], skip = 2, col_names = c(
      "date", "time", "volume_up", "price_up", "volume_down", "price_down"
    ))
    
    # Process the data
    result <- bm_data |>
      fill(date, .direction = "down") |>  # Fill down the date column
      mutate(
        country = "UA",
        # Extract hour from time range (e.g., "00:00 - 01:00" -> 0)
        hour_num = as.numeric(str_extract(time, "^\\d+")),
        # Combine date and hour
        hour = ymd_h(paste(format(date, "%Y-%m-%d"), hour_num), tz = "UTC"),
        # Convert volumes and prices to numeric
        volume_up = as.numeric(volume_up),
        volume_down = as.numeric(volume_down),
        price_up = as.numeric(price_up),
        price_down = as.numeric(price_down)
      ) |>
      filter(!is.na(date), !is.na(hour)) |>
      select(country, hour, volume_up, price_up, volume_down, price_down)
    
    file.remove(files[1])
    message("  Processed ", nrow(result), " rows")
    return(result)
    
  }, error = function(e) {
    message("  Error: ", e$message)
    return(tibble())
  })
}
