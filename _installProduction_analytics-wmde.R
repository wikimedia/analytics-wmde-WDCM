
### --- Install WDCM R packages in production (currently: stat1005)
### --- Hey hey: 
### --- run as sudo -u analytics-wmde Rscript /srv/analytics-wmde/installRlib/_installProduction_analytics-wmde.R

# - set proxy:
Sys.setenv(
  http_proxy = "http://webproxy.eqiad.wmnet:8080",
  https_proxy = "http://webproxy.eqiad.wmnet:8080")

# - fPath: where the scripts is run from?
fPath <- as.character(commandArgs(trailingOnly = FALSE)[4])
fPath <- gsub("--file=", "", fPath, fixed = T)
fPath <- unlist(strsplit(fPath, split = "/", fixed = T))
fPath <- paste(
  paste(fPath[1:length(fPath) - 1], collapse = "/"),
  "/",
  sep = "")

# - find out whether the fPath/r-library directory exists
# - YES: delete it and mkdir, NO: mkdir only
if (dir.exists(paths = paste(fPath, "r-library", sep = ""))) {
  unlink(x = paste(fPath, "r-library", sep = ""), 
         recursive = T)
  dir.create(path = paste(fPath, "r-library", sep = ""))
} else {
  dir.create(path = paste(fPath, "r-library", sep = ""))
}

# - install WDCM related packages:
install.packages(c("dplyr", "httr", "stringr", "XML", "readr", 
                   "data.table", "tidyr", "maptpx", "Rtsne"),
                 lib = paste(fPath, "r-library", sep = ""),
                 repos = c(CRAN = "https://www.stats.bris.ac.uk/R/"))

