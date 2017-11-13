
### --- Install WDCM R packages in production (currently: stat1005)
### --- Hey hey: 
### --- run as sudo -u analytics-wmde Rscript /srv/analytics-wmde/installRlib/_installProduction_analytics-wmde.R

# - set proxy:
Sys.setenv(
  http_proxy = "http://webproxy.eqiad.wmnet:8080",
  https_proxy = "http://webproxy.eqiad.wmnet:8080")

# - install WDCM related packages:
install.packages(c("dplyr", "httr", "stringr", "XML", "readr", 
                   "data.table", "tidyr", "maptpx", "Rtsne"),
                 lib = "/srv/analytics-wmde/r-library",
                 repos = c(CRAN = "https://www.stats.bris.ac.uk/R/"))