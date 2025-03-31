# Using ExcessILI's data archiving functions, returns the most recent copy of
# output obtained by running a function or formula \code{f}, unless this 
# copy doesn't exist or is older (by modification time) than \code{maxage}.
# In that case, \code{f} is run and the output is archived into the folder
# Data/'storeName' as an RDS file, using the function ExcessILI::storeRDS.
#
# @param storeName A string. The name of the folder to store output in
# @param f A function or formula taking no arguments. Formulas are coerced to
#   functions.
# @param maxage How old can any existing archived file be before \code{f} is 
#   called again?

source('./Data Pull/fileCache.R')

runIfExpired <- function(storeName, basepath, source, f, maxage=hours(0)) {
  storeName = "RESP NET Archive"
  basepath = "Data Pull"
  f = ~ read.socrata(url_rsv_net)
  
  mostRecent <- mostRecentTimestamp(storeName, basepath=basepath)
  f <- rlang::as_function(f)
  
  runAndArchive <- function() {
    data <- f()
    storeRDS(data, storeName, basepath, source)
    data
  }
  
  if (is.na(mostRecent)) 
    return(runAndArchive())
  if (mostRecent %--% now() < maxage)
    return(retrieveRDS(storeName, basepath))
  runAndArchive()
}


####################################
#####################################
#Verification: check that the newest data matches the 
#column names and variable types from older data
#If it does, update data, if not fall back to most recent pull data

verify_update = function(test_file,  ds_path){
  
  file_list <- sort(list.files(path=ds_path))
  if(length(file_list)>1) {
   previous_file <- file_list[length(file_list)-1]
  }else{
    previous_file <- file_list[1]
  }
  
  ref_file <- read_parquet(paste0("./Data/nssp_ed1/", previous_file))
  
  check_names = names(ref_file) == names(test_file)
  
  test_file_types = sapply(test_file, function(x) class(x)[1]) 
  ref_file_types = sapply(ref_file, function(x) class(x)[1]) 
  
  check_types <- test_file_types==ref_file_types
  
  if(sum(check_names) ==length(check_names) & sum(check_types)==length(check_names)){
    current_data = test_file
    print('Updated')
  } else{
    current_data = ref_file
    print('FAILED TO UPDATE')
    
  }
  return(current_data)
}
