# Define the target URL
url <- "https://www.gpee.com.ua/main/loadCharts" 

# Function to download data for a specific date
download_day_data <- function(date, gen = "1") {
  # Create the payload. Adjust keys and values as required.
  payload <- list(
    date = as.character(date),
    zona = "1",
    gen  = as.character(gen)
  )
  
  # Send the POST request
  response <- POST(url,
                   body = payload,
                   encode = "form")
  
  # Check if the request was successful
  if (status_code(response) != 200) {
    warning("Failed to retrieve data for ", date)
    return(NULL)
  }
  
  # Parse the response content assuming it's JSON.
  data_raw <- content(response, as = "text", encoding = "UTF-8")
  vec <- fromJSON(data_raw, flatten = TRUE)
  output <- data.frame(
    date = date,
    hour = seq(1,24),
    actual = vec[1:24],
    projected = vec[26:49]
  )
  
  return(output)
}

# Function to download data for a set of dates into a list
download_data_list <- function(dates, gen) { 
  lapply(dates, function(day) {
    tryCatch({
      download_day_data(day, gen)
    }, error = function(e) {
      message("Error downloading data for ", day, ": ", e)
      return(NULL)
    })
  })
}