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

timeStamp <- function(storeName, basepath=".", goalDate = lubridate::now()) {
  # Previously was mostRecentTimestamp() and retrievePath() used by retrieveRDS().
  # 
  #  "basepath": path location following the getwd() result.
  # "storeName": archive directory name, immediately following basepath.
  # 
  # This function lists all of the dates a source was archived, and will
  # summarize the most recently received archive per source type detected.
    
  # Confirm that "goalDate" is a Date object.
  assertthat::assert_that(any("POSIXct" %in% class(goalDate)))
  
  
  # Construct the path to the folder where all copies of "storeName" are stored.
  fullpath <- file.path(basepath, storeName)
  
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
  list("Record History" = all_dates, "Report Relative to Date" = relative_reports)
  
}




storeParquet <- function(obj, source, storeName, basepath='.', tolerance=24) {
  # Previously was storeRDS().
  # 
  #       "obj": the data recently queried from the API into memory.
  #    "source": the name of the source. Recommend using standard naming
  #              schemes to better manage archive. One "source" for one API.
  #  "basepath": path location following the getwd() result.
  # "storeName": archive directory name, immediately following basepath.
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
  
  # Construct the path to the folder where all copies of "storeName" are stored.
  fullpath <- file.path(basepath, storeName)
  
  # Confirm directory at the provided path exists.
  if (!dir.exists(fullpath)){
    stop(sprintf("Path '%s' doesn't exist, or isn't a directory", fullpath))
  }
  
  
  # Prepare the file details.
  time_now  <- Sys.time()
  name      <- format(time_now, paste(source, format="%Y_%m_%d_%H_%M.parquet", sep = "_"))
  writepath <- file.path(basepath, storeName, name)
  
  
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
  
  proximity <- timeStamp(storeName, basepath)$`Record History` %>%
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
  
  
  # If file creation conditions are met, then save as a new parquet file.
  write_parquet(obj, writepath)
  message(sprintf("Wrote object to '%s'", writepath))
  
}




## ----------------------------------------------------------------
## FROM "runIfExpired.R"

runIfExpired <- function(source, storeName, basepath, f, tolerance=24) {
  api_function <- rlang::as_function(f)
  
  mostRecent <- timeStamp(storeName, basepath)$`Report Relative to Date` %>%
    filter(Source == source, Status %in% c("Recent Pull", "Both"))

  data <- api_function()
  
  
  if ( nrow(mostRecent) == 0 ) {
    # Here is where the cross-over check will be done. Save the appended
    # result that is read in memory.
    
    message(sprintf("NEW dataset for '%s' in this directory.", source))
    return(storeParquet(data, source, storeName, basepath, tolerance))
    
  } else if( mostRecent$History %--% now() < hours(tolerance) ){
    message(sprintf("OLD dataset for '%s' is being called to memory. Save query is within the tolerance level.", source))
    return(read_parquet( file.path(basepath, storeName, mostRecent$filePath) ))
    
  } else{
    message(sprintf("Added another dataset for '%s' in this directory.", source))
    return(storeParquet(data, source, storeName, basepath, tolerance))
    
  }
  
}



