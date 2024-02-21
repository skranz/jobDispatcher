getFreeMemoryMB <- function() {
  osName <- Sys.info()[["sysname"]]
  if (osName == "Windows") {
    x <- system2("wmic", args =  "OS get FreePhysicalMemory /Value", stdout = TRUE)
    x <- x[grepl("FreePhysicalMemory", x)]
    x <- gsub("FreePhysicalMemory=", "", x, fixed = TRUE)
    x <- gsub("\r", "", x, fixed = TRUE)
    return(as.integer(x) / 1e6)
  } else if (osName == 'Linux') {
    x <- system2('free', args='--mega', stdout=TRUE)
    x <- strsplit(x[2], " +")[[1]][4]
    return(as.integer(x))
  } else {
    NA_integer_
  }
}

show_free_memory = function(new_line=FALSE) {
  mb = getFreeMemoryMB()
  if (is.na(mb)) return()
  msg = paste0("Free memory: ", round(mb/1000,2), " GB")
  if (new_line) {
    cat(paste0("\n",msg,"\n"))
  } else {
    cat(paste0("\r",msg))
  }
  flush.console()
}
