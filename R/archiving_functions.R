## ----------------------------------------------------------------
## Functions to deal with dataset overlapping.
##
## Date: March 29th, 2025
## Author: Shelby Golden, M.S.
## 
## Description: These are adaptations from the runIfExpired.R and fileCache.R
##              scripts that Dan provded, itself originally adapted from ExcessILI
##              data archiving functions by Marcus Russi. 
##              
##              The primary changes are:
##                 - Base path and source type is user provided
##                 - Source name is added the beginning of the file so that it
##                   has the format: "source_name_%Y_%m_%d_%H_%M.parquet"
##                 - 
## 

## ----------------------------------------------------------------
## SET UP THE ENVIRONMENT

suppressPackageStartupMessages({
  library("arrow")
  library("assertthat")
  library("tidyr")
  library("dplyr")
  library("stringr")
  library("MMWRweek")
  library("lubridate")
  library("RSocrata")
})

"%!in%" <- function(x,y)!("%in%"(x,y))


## ----------------------------------------------------------------
## FROM "fileCache.R"

timeStamp <- function(source, storeIn, basepath=".", goalDate = lubridate::now()) {
  # Previously was mostRecentTimestamp() and retrievePath() used by retrieveRDS().
  # 
  #  "basepath": path location following the getwd() result.
  #   "storeIn": archive directory name, immediately following basepath.
  # 
  # This function lists all of the dates a source was archived, and will
  # summarize the most recently received archive per source type detected.
  
  # Confirm that "goalDate" is a Date object.
  assertthat::assert_that(any("POSIXct" %in% class(goalDate)))
  
  
  # Construct the path to the folder where all copies of "storeIn" are stored.
  fullpath <- file.path(basepath,source, storeIn)
  
  # Confirm directory at the provided path exists.
  if (!dir.exists(fullpath)){
    stop(sprintf("Path '%s' doesn't exist, or isn't a directory", fullpath))
  }
  
  
  # Get all the files in this directory.
  dirListing <- list.files(fullpath)
  
  # If there were no files in the directory, throw an error.
  if (length(dirListing) == 0){
    out.list <- NULL
  }else{
  
  
  # For each source available, draw out the most recent time stamp recorded.
  # 
  # The original code was drawing the time information from the file metadata 
  # as opposed to the name. This script instead pulls that information from the
  # file name itself. It also keeps some details about the source type, which is
  # user provided. Notice this assumes files are stored 
  # "source_name_%Y_%m_%d_%H_%M.parquet", as used in storeRDS().
  
  # Assuming the time-stamp elements are the same, pull everything preceding
  # as the "source name" details.
  source <- str_replace(dirListing, "_[0-9]{4}_[0-9]{2}_[0-9]{2}_[0-9]{2}_[0-9]{2}.parquet", "")
  
  
  # Extract the time elements from all file names. Unfortunately, because
  # we're reading from right to left, each element needs to be extracted
  # individually. Repeat this for each source represented in the file.
  all_dates <- data.frame("Source" = source, 
                          "History" = rep( as.POSIXlt(NA), length(source) ),
                          "filePath" = dirListing)
  
  for(i in 1:length(source)) {
    time <- dirListing[i] %>%
      (\(y) {
        str_c(
          str_c(str_split_i(y, "_", -5), "-",
                str_split_i(y, "_", -4), "-",
                str_split_i(y, "_", -3)), " ",
          str_c(str_split_i(y, "_", -2), ":",
                str_split_i(y, "_", -1) %>% str_replace(., ".parquet", ""))
        )
      })
    
    all_dates[i, "History"] <- as.POSIXct(time)
  }
  
  # From the dates, save the most recently represented data pull for each source, 
  # according to the file name time stamp. Also find the file for each source that 
  # has the minimum time interval between the time stamp and goalDate (Delta).
  
  relative_reports <- all_dates %>%
    group_by(Source) %>%
    # Calculate the difference between the time stamp and goalDate.
    mutate(Delta = abs( purrr::map_dbl(History, ~lubridate::int_length(. %--% goalDate)) )) %>%
    # Store only the min Delta and max History.
    filter(History == max(History)|Delta == min(Delta)) %>%
    # Label the filtered rows by which condition it met, including a condition that
    # denotes if a row met both recent pull and min delta conditions.
    mutate("Status" = ifelse(History == max(History), "Recent Pull", "Min Delta")) %>%
    mutate("Status" = ifelse(Delta == min(Delta) & History == max(History), "Both", Status)) %>%
    ungroup() %>%
    as.data.frame()
  
  # Report the results.
  out.list <- list("Record History" = all_dates, "Report Relative to Date" = relative_reports)
  }
  return(out.list)
}

## ----------------------------------------------------------------
## modified FROM "fileCache.R"

datetimeStamp <- function( basepath=".", goalDate = lubridate::now()) {
  # Previously was mostRecentTimestamp() and retrievePath() used by retrieveRDS().
  # 
  #  "basepath": path location following the getwd() result.
  # 
  # This function lists all of the dates a source was archived, and will
  # summarize the most recently received archive per source type detected.
  
  # Confirm that "goalDate" is a Date object.
  assertthat::assert_that(any("POSIXct" %in% class(goalDate)))
  
  
  # Construct the path to the folder where all copies of "storeName" are stored.
  fullpath <- file.path(basepath)
  
  # Confirm directory at the provided path exists.
  if (!dir.exists(fullpath)){
    stop(sprintf("Path '%s' doesn't exist, or isn't a directory", fullpath))
  }
  
  
  # Get all the files in this directory.
  dirListing <- list.files(fullpath)
  
  # If there were no files in the directory, throw an error.
  if (length(dirListing) == 0){
    stop(sprintf("No files were found in dirctory '%s'", fullpath))
  }
  
  
  # For each source available, draw out the most recent time stamp recorded.
  # 
  # The original code was drawing the time information from the file metadata 
  # as opposed to the name. This script instead pulls that information from the
  # file name itself. It also keeps some details about the source type, which is
  # user provided. Notice this assumes files are stored 
  # "source_name_%Y_%m_%d_%H_%M.parquet", as used in storeRDS().
  
  # Assuming the time-stamp elements are the same, pull everything preceding
  # as the "source name" details.
  source <- str_replace(dirListing, "_[0-9]{4}_[0-9]{2}_[0-9]{2}_[0-9]{2}_[0-9]{2}.parquet", "")
  
  ##Extract the date from the file name
  
   extract_date <- function(filename) {
    # Use a regular expression to extract the date in YYYY_MM_DD format
    match <- regmatches(filename, regexpr("\\d{4}_\\d{2}_\\d{2}", filename))
    # Convert to Date format
    as.Date(match, format = "%Y_%m_%d")
  }
  
  #extract_date(all_dates)
 
  file_date <- extract_date(source)

  all_files <- data.frame("Source" = source, 
                          "History" = file_date,
                          "filePath" = dirListing)
  
  # From the dates, save the most recently represented data pull for each source, 
  # according to the file name time stamp. Also find the file for each source that 
  # has the minimum time interval between the time stamp and goalDate (Delta).
  
  all_dates <- data.frame("Source" = source, 
                          "History" = rep( as.POSIXlt(NA), length(source) ),
                          "filePath" = dirListing)
  
  relative_reports <- all_files %>%
    group_by(Source) %>%
    # Calculate the difference between the time stamp and goalDate.
    mutate(Delta = abs( purrr::map_dbl(History, ~lubridate::int_length(. %--% goalDate)) )/(60*60*24)  ) %>%
    # Store only the min Delta and max History.
    filter(History == max(History)|Delta == min(Delta)) %>%
    # Label the filtered rows by which condition it met, including a condition that
    # denotes if a row met both recent pull and min delta conditions.
    mutate("Status" = ifelse(History == max(History), "Recent Pull", "Min Delta")) %>%
    mutate("Status" = ifelse(Delta == min(Delta) & History == max(History), "Both", Status)) %>%
    ungroup() %>%
    as.data.frame()  # Report the results.
  list("Record History" = all_dates, "Report Relative to Date" = relative_reports)
  
}


storeParquet <- function(obj, source, storeIn, basepath='.', tolerance=24,mostRecent) {
  # Previously was storeRDS().
  # 
  #       "obj": the data recently queried from the API into memory.
  #    "source": the name of the source. Recommend using standard naming
  #              schemes to better manage archive. One "source" for one API.
  #  "basepath": path location following the getwd() result.
  #   "storeIn": archive directory name, immediately following basepath.
  # "tolerance": the difference (in hours) between the nearest recorded
  #              archival record for the same source.
  # 
  # 
  # This function will store the file called into memory from a specific API.
  # It checks that the file location exists and that it will not either
  # overwrite an existing file nor save a file within a span of time of another
  # file.
  
  if (!dir.exists(basepath)) {
    stop(sprintf("Basepath '%s' does not exist. Cannot write file.", basepath))
  }
  
  # Construct the path to the folder where all copies of "storeIn" are stored.
  fullpath <- file.path(basepath, source, storeIn)
  
  # Confirm directory at the provided path exists.
  if (!dir.exists(fullpath)){
    stop(sprintf("Path '%s' doesn't exist, or isn't a directory", fullpath))
  }
  
  
  if (nrow(mostRecent) == 0){
    message(sprintf("No existing files present in directory '%s'.", fullpath))
    
  } else{
    # If there are existing files, ensure that it is within tolerance.
    
    # Prepare the file details.
    time_now  <- Sys.time()
    name      <- format(time_now, paste(source, format="%Y_%m_%d_%H_%M.parquet", sep = "_"))
    writepath <- file.path(fullpath, name)
    
    all.files <- list.files(writepath)
    
    if(length(all.files)>0){
    
    # Check that a file with that exact name is not going to be overwritten.
    # Get all the files in this directory.
    dirListing <- list.files(fullpath)
    
    # If there were no files in the directory, throw an error.
    if (name %in% dirListing){
      stop(sprintf("A file for '%s' has already been recorded.", source))
    }
    
    
    # Even if the file name is not the exact same to the minute as a file
    # that exists in the record, we don't want to save a record within a
    # specified tolerance. This is set to 24hrs by default.
    
    proximity <- timeStamp(source, storeIn, basepath)$`Record History` %>%
      filter(Source == source) %>%
      # Find the difference between the nearest date and system time, 
      # rounded to hours.
      mutate(Delta = abs( purrr::map_dbl(History, ~lubridate::int_length(. %--% time_now))/3600 )) %>%
      filter(Delta < 24) %>%
      nrow()
    
    # If the newly queried file is within the tolerance level, throw an error.
    if (proximity != 0){
      stop(sprintf("A file for '%s' was recorded within the tolerance range.", source))
    }
  }
  
  }
  # If file creation conditions are met, then save as a new parquet file.
  write_parquet(obj, writepath)
  message(sprintf("Wrote object to '%s'", writepath))
  
}







## ----------------------------------------------------------------
## FROM "runIfExpired.R"

runIfExpired <- function(source, storeIn, f, tolerance = (24*7), return_recent = FALSE, basepath = "Data Pull") {
  # url_nssp <- "https://data.cdc.gov/resource/rdmq-nq56.csv"
  # source='nssp_ed1'
  # storeIn='Raw'
  # basepath='./Data/Archive'
  #   f=~ read.socrata(url_nssp)
  #  tolerance = 24
  #  return_recent = FALSE
  
  api_function <- rlang::as_function(f)
  
  mostRecent_check <- timeStamp(source,storeIn, basepath)
    
  if(!is.null(mostRecent_check)){
    mostRecent <- mostRecent_check$`Report Relative to Date` %>%
    filter(Source == source, Status %in% c("Recent Pull", "Both"))
  }
  data <- api_function()
  
  if(is.null(mostRecent_check)){
    mostRecent <- data.frame(History = 99999)
  }
  
  if ( nrow(mostRecent) == 0 ) {
    rm_recent_null()
    
    message(sprintf("NEW dataset for '%s' in this directory.", source))
    return(storeParquet(data, source, storeIn, basepath, tolerance))
    
  } else if(return_recent == TRUE & mostRecent$History %--% now() < hours(tolerance) ){
    message(sprintf("OLD dataset for '%s' is being called to memory. Save query is within the tolerance level.", source))
    return(read_parquet( file.path(basepath, storeIn, mostRecent$filePath) ))
    
  } else if(return_recent == FALSE & mostRecent$History %--% now() < hours(tolerance)) {
    message(sprintf("Request is too close to the most recent archive '%s'.", mostRecent$filePath))
    return(NULL)
    
  } else{
    message(sprintf("Added another dataset for '%s' in this directory.", source))
    return(storeParquet(data, source, storeIn, basepath, tolerance,mostRecent))
    
  }
  
}

# -----------------------------
# Remove missing entries from recently dated entries.
# 
# Developed in "Nuanced Pull Simulation/Testing Script.R"

rm_recent_null <- function(data, date_col, dictionary){
  # Some sources will add rows that are recent to the API call, but lack
  # any value entries. They're recorded as "NULL" or NA's. This function
  # will remove recently dated entries that have no value entered. It
  # references the Data Dictionary to find rows in the raw format that
  # are labeled as "Outcome", and uses all of these to search over.
  #
  #       "data": the data that was recently called in by the API.
  #   "date_col": specify the column with whole date associated. 
  #               with the data entry. Needs to be class type "Date".
  # "dictionary": the CSV containing information about the raw data
  #               columns that are expected.
  
  
  # Pull the column names and their use assignment from the data dictionary.
  col_type = raw_columns(dictionary)
  # Isolate the variables that are used for "Outcome". These are the
  # numeric columns of interest.
  variables <- col_type[col_type$Use == "Outcome", "Raw Data Columns"]
  
  filter_statement <- variables %>%
    str_c(., " %in% c('NULL', NA)") %>%
    str_flatten(collapse = " & ")
  
  # Confirm that date_col is a Date object.
  assertthat::assert_that(any("Date" %in% sapply(data[, colnames(data) %in% date_col], class)))
  
  
  # -----------------------------
  # Find and remove the recent date range that has NA's and NULL.
  
  # Group dates that are consecutive weeks.
  dates_na_null <- data %>%
    # Filter over the outcome columns.
    filter(eval(rlang::parse_expr(filter_statement))) %>%
    # Save and order the dates associated.
    select(week_ending_date) %>% pull() %>% unique() %>% sort() %>%
    # Evaluate which dates are not within 7 days of each other and
    # save in separate list entries.
    (\(x) { split(x, cumsum(c(TRUE, diff(x) != 7))) }) ()
  
  
  recent_date <- data[, colnames(data) %in% date_col] %>% pull() %>% max()
  
  # The list entry that represents the most recent set of entries.
  eval_recent_dates <- dates_na_null %>%
    # Search each list element for the max year in the consecutive
    # series of weeks.
    lapply(., function(x){
      any(x %in% recent_date)
    }) %>%
    unlist() %>% 
    # Report the index of the weeks that are most recent.
    (\(x) { which(x == TRUE) }) ()
  
  
  # Confirm the recent date of entry is included in the recent date
  # ranges where NULL and NA's were found for all variables. If not,
  # exit the function.
  if(length(eval_recent_dates) < 1){
    stop( sprintf("Most recent entries where %s have NULL or NA's does not include \nthe most recent date. Step not needed; abort operation.", str_flatten(variables, collapse = " and ")) )
  }
  
  # Create an argument that will filter out the recent dates with
  # no reported values.
  date_filter_argument <- 
    str_flatten(dates_na_null[[eval_recent_dates]], collapse = "', '") %>%
    str_c("c('", ., "')") %>%
    paste(date_col, ., sep = " %!in% ")
  
  # Remove these values.
  result <- data %>% 
    filter(eval(rlang::parse_expr(date_filter_argument))) %>%
    `rownames<-`(NULL)
  
  
  result
  
}
