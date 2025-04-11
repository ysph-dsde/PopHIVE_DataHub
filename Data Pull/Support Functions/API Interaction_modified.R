## ----------------------------------------------------------------
## Functions to pull in data from different API's.
##
##      Authors: Daniel Weinberger PhD and Shelby Golden, M.S.
## Date Created: March 29th, 2025
## 
##     Version: v2
## Last Edited: April 10th, 2025
## 
## Description: These are adaptations from the runIfExpired.R and fileCache.R
##              scripts that Dan provided, itself originally adapted from
##              ExcessILI data archiving functions by Marcus Russi. 
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
## modified FROM "fileCache.R"

datetimeStamp(storeIn = "RESP NET Archive", goalDate = as.POSIXct("2025-03-30 23:19:22 EDT"))

datetimeStamp <- function(storeIn, goalDate = lubridate::now(), basepath = "Data Pull", timeZone = "EST") {
  # Previously was mostRecentTimestamp() and retrievePath() used by retrieveRDS().
  # 
  #  "basepath": path location following the getwd() result.
  #  "goalDate": the relative date to find archival history. Can be set to
  #              any date prior to the current date.
  #  "timeZone": the lubridate time zone for the time stamp.
  # 
  # This function lists all of the dates a source was archived, and will
  # summarize the most recently received archive per source type detected.
  
  # Confirm that "goalDate" is a Date object.
  assertthat::assert_that(any("POSIXct" %in% class(goalDate)))
  
  
  # Construct the path to the folder where all copies of "storeName" are stored.
  fullpath <- file.path(basepath, storeIn)
  
  # Confirm directory at the provided path exists.
  if (!dir.exists(fullpath)){
    stop(sprintf("Path '%s' doesn't exist, or isn't a directory", fullpath))
  }
  
  
  # Get all the files in this directory.
  dirListing <- list.files(fullpath)
  
  # Exclude the data dictionary, which is expected to include the word
  # "dictionary" in the file name. Only retain files that represent archived
  # datasets. Inclusion of "(?i)" make the expression case agnostic.
  dirListing <- dirListing[!str_detect(dirListing, "(?i)dictionary")]
  
  
  # If there were no files in the directory, throw an error.
  if (length(dirListing) == 0){
    message(sprintf("No files were found in dirctory '%s'", fullpath))
    result <- "No History"
    
    return(result)
  }
  
  
  # For each source available, draw out the most recent time stamp recorded
  # and source details.
  # 
  # The original code was drawing the time information from the file metadata 
  # as opposed to the name. This script instead pulls that information from the
  # file name itself. It also keeps some details about the source type, which
  # is user provided.
  # 
  # Files are expected to include the time stamping at the beginning, and most
  # archived data will be parquet. This code is intended to cover file nomenclature
  # that has time stamping at the end of the file name and csv or xlsx files
  # to improve robust functionality. 
  #
  # The time stamp might also include the hour and minute or only the year, month, 
  # and day. This code will handle both scenarios, but assumes the patterns are
  # either "Ymd" or "Ymd HM".
  
  # Pull only the source name.
  source <- str_replace(dirListing, ".(parquet|csv|xlsx)", "") %>%
    (\(x) { str_replace(x, "[0-9]{4}_[0-9]{2}_[0-9]{2}(_[0-9]{2}_[0-9]{2}|)", "") }) () %>%
    (\(y) { str_replace(y, "_\\b|\\b_", "") }) ()
  
  # Pull the dates.
  time <- str_extract(dirListing, "[0-9]{4}_[0-9]{2}_[0-9]{2}(_[0-9]{2}_[0-9]{2}|)") %>%
    (\(x) { parse_date_time(x, c("Ymd HM", "Ymd"), tz = timeZone) }) ()
  
  
  ##Extract the date from the file name
  #extract_date <- function(filename) {
    # Use a regular expression to extract the date in YYYY_MM_DD format
  #  match <- regmatches(filename, regexpr("\\d{4}_\\d{2}_\\d{2}", filename))
    # Convert to Date format
  #  as.Date(match, format = "%Y_%m_%d")
  #}
  #extract_date(all_dates)
  #file_date <- extract_date(source)
  #all_files <- data.frame("Source" = source, 
  #                        "History" = file_date,
  #                        "filePath" = dirListing)
  
  
  # Save results in one dataframe.
  all_dates <- data.frame("Source" = source, 
                          "History" = time,
                          "filePath" = dirListing) %>%
    # Organize the rows to order them by source then by time archived.
    (\(x) { x[with(x, order(Source, History)), ] }) () %>% 
    `rownames<-`(NULL)
  
  # From the dates, save the most recently represented data pull for each source, 
  # according to the file name time stamp. Also find the file for each source that 
  # has the minimum time interval between the time stamp and goalDate (Delta).
  
  relative_reports <- all_dates %>%
    group_by(Source) %>%
    # Calculate the difference between the time stamp and goalDate.
    mutate(Delta = round(abs( purrr::map_dbl(History, ~lubridate::int_length(. %--% goalDate)) )/(60*60*24), 1)  ) %>%
    # Store only the min Delta and max History.
    filter(History == max(History)|Delta == min(Delta)) %>%
    # Label the filtered rows by which condition it met, including a condition that
    # denotes if a row met both recent pull and min delta conditions.
    mutate("Status" = ifelse(History == max(History), "Recent Pull", "Min Delta")) %>%
    mutate("Status" = ifelse(Delta == min(Delta) & History == max(History), "Both", Status)) %>%
    ungroup() %>%
    as.data.frame()
  
  
  # Report the results.
  result <- list("Record History" = all_dates, "Report Relative to Date" = relative_reports)
  
  result
}




storeFile(pulled, sourceName = "rsv-net", storeIn = "RESP NET Archive Test", fileType = "csv")

storeFile <- function(pulledData, sourceName, storeIn, fileType = "parquet", 
                      includeTime = TRUE, stampBeginning = TRUE, tolerance = 24, 
                      basepath = "Data Pull", timeZone = "EST") {
  # Previously was storeRDS().
  # 
  #     "pulledData": the data recently queried from the API into memory.
  #     "sourceName": the name of the source. Recommend using standard naming
  #                   schemes to better manage archive. One "source" for one API.
  #       "basepath": path location following the getwd() result.
  #        "storeIn": archive directory name, immediately following basepath.
  #       "fileType": choose the file type saved. Default is "parquet", alternative
  #                   is "csv".
  #    "includeTime": toggle to include or exclude the hour and minutes with
  #                   the date.
  #      "tolerance": the difference (in hours) between the nearest recorded
  #                   archival record for the same source.
  # "stampBeginning": toggle where to place time and date stamp.
  # 
  # 
  # This function will store the file called into memory from a specific API.
  # It checks that the file location exists and that it will not either
  # overwrite an existing file nor save a file within a defined span of time of 
  # another file. Can change the file naming convention and file type.
  
  if (!dir.exists(basepath)) {
    stop(sprintf("Basepath '%s' does not exist. Cannot write file.", basepath))
  }
  
  # Construct the path to the folder where all copies of "storeIn" are stored.
  fullpath <- file.path(basepath, storeIn)
  
  # Confirm directory at the provided path exists.
  if (!dir.exists(fullpath)){
    stop(sprintf("Path '%s' doesn't exist, or isn't a directory", fullpath))
  }
  
  
  # We want to ensure that the recently queried dataset is not going to overwrite
  # an existing file or is going to save something within a specified time frame 
  # since the recent pull.
  
  mostRecent <- datetimeStamp(storeIn = storeIn, basepath = basepath)
  
  # Current time translated into the timezone of the history record.
  time_now  <- Sys.time() %>% as.POSIXct(., tz = timeZone)
  
  # If there are no records in the directory do not check proximity.
  if (any(mostRecent == "No History")){
    message(sprintf("'%s' is empty.", fullpath))
    
  # If there are records, but none for the sourceName, do not check proximity.
  } else if (!any(mostRecent$`Report Relative to Date`$Source %in% sourceName)){
    message(sprintf("No existing files for the provided source is in directory '%s'.", fullpath))
    
  # If there are existing files, ensure that it is within tolerance.
  } else{
    mostRecent <- mostRecent$`Record History` %>% 
      filter(Source == sourceName)
    
    # Check that a file with that exact name is not going to be overwritten.
    if(any(mostRecent$History %in% time_now)) {
      stop(sprintf("A file for '%s' has already been recorded.", sourceName))
    }
      
    # Even if the file name is not the exact same to the minute as a file
    # that exists in the record, we don't want to save a record within a
    # specified tolerance. This is set to 24hrs by default.
    
    proximity <- mostRecent %>%
      # Find the difference between the nearest date and system time, 
      # rounded to hours.
      mutate(Delta = abs( purrr::map_dbl(History, ~lubridate::int_length(. %--% time_now))/3600 )) %>%
      filter(Delta <= tolerance) %>%
      nrow()
    
    # If the newly queried file is within the tolerance level, throw an error.
    if (proximity != 0){
      stop(sprintf("A file for '%s' was recorded within the tolerance range.", sourceName))
    }
  }
  
  
  # If file creation conditions are met, then save as a new parquet file. The
  # following three if statements will construct the desired file naming system.
  
  # Format the time to stamped.
  if(includeTime == TRUE) {
    addTime <- format(time_now,format="%Y_%m_%d_%H_%M")
  } else if(includeTime == FALSE) {
    addTime <- format(time_now,format="%Y_%m_%d")
  }
  
  # Add time with the source name.
  if(stampBeginning == TRUE) {
    fileName <- paste(addTime, sourceName, sep = "_")
  } else if(stampBeginning == FALSE) {
    fileName <- paste(sourceName, addTime, sep = "_")
  }
  
  
  if(fileType == "parquet") {
    writepath <- file.path(basepath, storeIn, paste(fileName, ".parquet", sep = ""))
    
    write_parquet(pulledData, writepath)
    message(sprintf("Wrote recently pulled data as parquet to '%s'", writepath))
    
  } else if(fileType == "csv") {
    writepath <- file.path(basepath, storeIn, paste(fileName, ".csv", sep = ""))
    
    write.csv(pulledData, writepath)
    message(sprintf("Wrote recently pulled data as csv to '%s'", file.path(basepath, storeIn)))
  }
  
}




## ----------------------------------------------------------------
## FROM "runIfExpired.R"

url_rsv_net <- "https://data.cdc.gov/resource/29hc-w46k.csv"


runIfExpired <- function(sourceName, storeIn, f, fileType = "parquet", returnRecent = TRUE,
                         includeTime = TRUE, stampBeginning = TRUE, tolerance = (24*7), 
                         basepath = "Data Pull", timeZone = "EST") {
  sourceName = "rsv-net"
  storeIn = "RESP NET Archive Test"
  f = ~ read.socrata(url_rsv_net)
  basepath = "Data Pull"
  fileType = "parquet"
  tolerance = 24
  returnRecent = TRUE
  includeTime = TRUE
  stampBeginning = TRUE
  timeZone = "EST"
  
  
  
  api_function <- rlang::as_function(f)
  
  mostRecent_check <- datetimeStamp(storeIn = storeIn, basepath = basepath)
  
  if(mostRecent_check) {
    mostRecent <- mostRecent_check$`Report Relative to Date` %>%
      filter(Source == source, Status %in% c("Recent Pull", "Both"))
  }
  
  if(is.null(mostRecent_check)){
    mostRecent <- data.frame(History = 99999)
  }
  
  if ( is.null(mostRecent_check) ) {
    #run this if the directory is empty
    #rm_recent_null()
    data <- api_function()
    
    message(sprintf("NEW dataset for '%s' in this directory.", source))
    storeParquet(data, source, storeIn, basepath, tolerance,mostRecent)
    full_path <- paste(basepath,source,storeIn, sep='/')
    new.file <- list.files(path=paste(basepath,source,storeIn, sep='/'))
    
    return(read_parquet( paste(full_path,new.file , sep='/')))
    
  } else if(returnRecent == TRUE & mostRecent$History %--% now() < hours(tolerance) ){
    message(sprintf("OLD dataset for '%s' is being called to memory. Save query is within the tolerance level.", source))
    full_path <- paste(basepath,source,storeIn, sep='/')
    
    return(read_parquet( file.path(basepath,  source, storeIn, mostRecent$filePath) ))
    
  } else if(returnRecent == FALSE & mostRecent$History %--% now() < hours(tolerance)) {
    message(sprintf("Request is too close to the most recent archive '%s'.", mostRecent$filePath))
    return(NULL)
    
  } else{
    data <- api_function()
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
