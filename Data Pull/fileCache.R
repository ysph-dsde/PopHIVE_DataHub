##FROM excessILI package, Marcus Russi
now <- lubridate::now()

retrievePath <- function(fname, basepath='.', goalDate=lubridate::now()) {
  
  # Construct the path to the folder where all copies of 'fname' should be 
  # stored
  fullpath <- file.path(basepath, fname)
  
  # Make sure that this path is a directory I.e., archive/fname.txt needs to 
  # be a folder, not regular file.
  if (!dir.exists(fullpath)){
    stop(sprintf("Path %s doesn't exist, or isn't a directory", fullpath))
  }
  
  # Be sure that 'goalDate' is a Date object
  assertthat::assert_that(any("POSIXct" %in% class(goalDate)))
  
  # Get all the files in this directory
  dirListing <- list.files(fullpath)
  
  # If there were no files in the directory, we can't retrieve them!
  if (length(dirListing) == 0){
    stop(sprintf("No files were found in dirctory %s", fullpath))
  }
  
  # Get a list of modification times for each file in the directory
  fullPaths <- file.path(fullpath, dirListing)
  mtimes <- purrr::lift_dl(file.mtime)(fullPaths)
  
  # The interval elapsed between the modified time of each file, and the 
  # goalDate. Our goal is to get the file with the smallest delta.
  absDeltas <-
    purrr::map_dbl(mtimes, ~lubridate::int_length(. %--% goalDate)) %>%
    abs
  
  # Find the index, and return it
  idx <- which(absDeltas == min(absDeltas))[1]
  
  fullPaths[idx]
}

#' Retrieve an RDS modified nearest to a specific date
#' 
#' Retrieves an RDS file stored in \code{basepath/fname} that has
#' the closest modification date to \code{goalDate}. If no such file exists, an
#' error will be thrown.
#'
#' @inheritParams retrievePath
#'
#' @examples
#' storeRDS(mtcars, 'mtcars')
#' identical(mtcars, retrieveRDS('mtcars'))
#'
#' storeRDS(mtcars, 'mtcars', basepath='.')
#' mtcars2 <- mtcars
#' mtcars2[,'cyl'] <- 1
#' storeRDS(mtcars2, 'mtcars')
#' mtcars_retrieved <- retrieveRDS('mtcars', goalDate = lubridate::now())
#' identical(mtcars, mtcars_retrieved)
#'
#' @export
retrieveRDS <- function(fname, basepath='.', goalDate=Sys.time())
  retrievePath(fname, basepath, goalDate) %>% read_parquet()

#' Store an R object into the file cache
#' 
#' Given an R object, attempts to store it in the directory
#' \code{basepath/fname}. The name given to the file will be of the form
#' \code{DATE.rds}, where \code{DATE} is of the format
#' \code{\%Y_\%m_\%d_\%H_\%M}.  An error will be thrown if \code{basepath} does
#' not exist. However, if \code{basepath/fname} does not exist, an attempt will
#' be made to create it. The \code{DATE} is the current time. Intended to be
#' used with \code{\link{retrieveRDS}}. See \code{\link{mostRecentTimestamp}}
#' for an usage example.
#' 
#' @param fname The name of the directory in \code{basepath} where various
#'   revisions of the file are stored. I.e., \code{file.txt} should be a
#'   directory, with revisions of the true \code{file.txt} stored inside of
#'   it.
#'
#' @param obj An R object
#'
#' @param basepath A string. The path which stores \code{fname}. Default '.'
#'
#' @return A message announcing the path the object has been written to
#' 
#' @examples
#' saveRDS(mtcars, 'cars')
#' saveRDS(mtcars, 'cars')
#' # Now the filesystem has, in '.':
#' # ├── mtcars
#' # │   ├── 2020_04_09_16_40.rds
#' # │   ├── 2020_04_09_16_41.rds
#'
#' @export
storeRDS <- function(obj, fname, basepath='.') {
  
  if (!dir.exists(basepath)){
    stop(sprintf("Basepath '%s' does not exist. Cannot write file.", basepath))
  }
  
  fullPath <- file.path(basepath, fname)
  
  # Create the directory for 'fname' if it doesn't exist. Notify the user.
  if (!dir.exists(fullPath)) {
    message(sprintf("Creating directory %s", fullPath))
    success <- dir.create(fullPath, recursive = FALSE, showWarnings = TRUE)
    
    if (any(!success))
      stop(sprintf("Failed to create directory %s", fullPath))
  }
  
  name <- format(Sys.time(), format="%Y_%m_%d_%H_%M.parquet")
  writepath <- file.path(basepath, fname, name)
  
  #saveRDS(obj, writepath)
  write_parquet(obj, writepath)
  
  message(sprintf("Wrote object to %s", writepath))
}

#' Identify the timestamp of the most recently modified file in the file cache
#' 
#' Returns the timestamp of the most recently modified file. If no such file
#' exists, or if the directory \code{basepath/fname} doesn't exist, returns NA.
#' 
#' @param fname The name of the directory in \code{basepath} where various
#'   revisions of the file are stored. I.e., \code{file.txt} should be a
#'   directory, with revisions of the true \code{file.txt} stored inside of
#'   it.
#'
#' @param basepath A string. The path which stores the \code{fname} directory.
#'   Default '.'
#'
#' @return A POSIXct object specifying the \code{mtime} of the most recently
#'   modified file in \code{basepath/fname}
#'
#' @examples
#' library(lubridate)
#'
#' saveRDS(mtcars, 'cars')
#' saveRDS(mtcars, 'cars')
#' 
#' # Some time elapses...
#' 
#' # Decide if the latest version of 'mtcars' is "too old"
#' if (mostRecentTimestamp('mtcars') %--% now() > hours(24)) {
#'   # Store a "new" version
#'   saveRDS(mtcars, 'cars')
#' } else {
#'   cached_mtcars <- retrieveRDS('mtcars')
#' }
#'
#' @export
mostRecentTimestamp <- function(fname, basepath='.') {
  
  # Construct the path to the folder where all copies of 'fname' should be 
  # stored
  fullpath <- file.path(basepath, fname)
  
  # Make sure that this path is a directory I.e., archive/fname.txt needs to 
  # be a folder, not regular file.
  if (!dir.exists(fullpath))
    return(NA)
  
  # Get all the files in this directory
  dirListing <- list.files(fullpath)
  
  # If there were no files in the directory, we can't retrieve them!
  if (length(dirListing) == 0)
    return(NA)
  
  # Get a list of modification times for each file in the directory
  fullPaths <- file.path(fullpath, dirListing)
  mtimes <- purrr::lift_dl(file.mtime)(fullPaths)
  
  max(mtimes)
}