# plumber.R

library(duckdb)
library(countrycode)
library(rlang)

con <- dbConnect(duckdb())
dbExecute(con, "INSTALL httpfs; LOAD httpfs;")

validate_date <- function(date) {
  expected_format <- "%Y-%m-%d"
  min_date <- as.Date("2012-10-01")
  max_date <- Sys.Date() - 3

  if (!grepl("^\\d{4}-\\d{2}-\\d{2}$", date)) {
    stop("Invalid date format. Expected format: ", expected_format)
  }

  date <- as.Date(date, format = expected_format)

  if (is.na(date)) {
    stop("Invalid date.")
  }

  if (date < min_date || date > max_date) {
    stop("Date out of range. Must be between ", min_date, " and ", max_date)
  }

  format(date, expected_format)
}

validate_country <- function(country) {
  if (!rlang::is_string(country)) {
    cli::cli_alert_info("{country} is not a valid value, ignoring.")
    return(NULL)
  }
  if (!identical(nchar(country), 2L)) {
    cli::cli_alert_info(
      "{.code country} should be the ISO 2-character country code. \n
       Ignoring the value."
    )
  }
  country <- toupper(country)
  if (!country %in% na.omit(countrycode::codelist$iso2c)) {
    cli::cli_alert_info(
      "{country} not found in country list. Ignoring the value."
    )
  }
  country
}

serializer_nanoparquet <- function(type = "application/vnd.apache.parquet") {
  serializer_write_file(
    fileext = ".parquet",
    type = type,
    write_fn = function(val, tmpfile) {
      nanoparquet::write_parquet(val, tmpfile)
    }
  )
}
register_serializer("nanoparquet", serializer_nanoparquet)

#* @serializer nanoparquet
#* @get /daily_r_downloads
daily_r_downloads <- function(date) {
  if (is.null(date)) return(NULL)

  date <- validate_date(date)
  year <- as.POSIXlt(date)$year + 1900

  if (date %in% dbListTables(con)) {
    cli::cli_alert_info("Data found in cache")
  } else {
    cli::cli_alert_info("Getting data and writing to in-memory database")
    url <- paste0("http://cran-logs.rstudio.com/", year, "/", date, "-r.csv.gz")
    qry <- paste0(
      "CREATE TABLE ",
      sQuote(date, q = FALSE),
      " AS ",
      "SELECT * FROM read_csv(",
      sQuote(url, q = FALSE),
      ");"
    )
    cli::cli_alert_info("Query: {qry}")
    dbExecute(con, qry)
  }
  res_qry <- paste0("SELECT * FROM ", sQuote(date, q = FALSE), ";")
  dbGetQuery(con, res_qry)
}

#* @get /daily_r_downloads_per_country
daily_r_downloads_per_country <- function(date, country = NULL) {
  if (is.null(date)) return(NULL)
  date <- validate_date(date)
  country <- validate_country(country)
  country_filter <- NULL

  daily_r_downloads(date)

  if (!is.null(country)) {
    country_filter <- paste0(" WHERE country = ", sQuote(country, q = FALSE))
  }

  dbGetQuery(
    con,
    paste0(
      "SELECT country, COUNT(country) AS download_per_country
       FROM ",
      sQuote(date, q = FALSE),
      country_filter,
      " GROUP BY country;"
    )
  )
}
