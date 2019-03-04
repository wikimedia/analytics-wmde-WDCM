#!/usr/bin/env Rscript

### ---------------------------------------------------------------------------
### --- WDCM Process Module, v2.0 Beta 0.1
### --- Script: WDCM_Update_Labs.R, v. Beta 0.1
### ---------------------------------------------------------------------------
### --- DESCRIPTION:
### --- WDCM_Update_Labs.R runs on cron from wikidataconcepts.eqiad.wmflabs
### --- periodically checking the changes in /srv/published-datasets/wdcm
### --- on stat1007 by accessing 
### --- https://analytics.wikimedia.org/datasets/wmde-analytics-engineering/wdcm/
### --- onto which the /srv/published-datasets/wdcm from stat1007
### --- is mapped.
### --- This script will check the timestamp in toLabsReport.csv
### --- and compare it to a locally stored version.
### --- If the timestamp found in production is newer, the script
### --- will copy all necessary files from 
### --- https://analytics.wikimedia.org/datasets/wmde-analytics-engineering/wdcm/
### --- to a local directory in CloudVPS
### --- and update its local timestamp.
### --- Once the timestamp is updated and the files copied, the script will run 
### --- nohup /home/goransm/WMDE/WDCM/WDCM_RScripts &
### --- to update the WDCM Dashboards.
### ---------------------------------------------------------------------------

### ---------------------------------------------------------------------------
### --- LICENSE:
### ---------------------------------------------------------------------------
### --- GPL v2
### --- This file is part of Wikidata Concepts Monitor (WDCM)
### ---
### --- WDCM is free software: you can redistribute it and/or modify
### --- it under the terms of the GNU General Public License as published by
### --- the Free Software Foundation, either version 2 of the License, or
### --- (at your option) any later version.
### ---
### --- WDCM is distributed in the hope that it will be useful,
### --- but WITHOUT ANY WARRANTY; without even the implied warranty of
### --- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
### --- GNU General Public License for more details.
### ---
### --- You should have received a copy of the GNU General Public License
### --- along with WDCM. If not, see <http://www.gnu.org/licenses/>.
### ---------------------------------------------------------------------------

# - toReport:
print("--------------------------------------------------------------------")
print(paste0("WDCM_Update_Labs run started on: ", as.character(Sys.time())))

### --- Setup
library(httr)
library(curl)
library(stringr)
library(RMySQL)

### --- utilities
wdcm_batchIndexes <- function(x, batchSize) {
  if (class(batchSize) != "integer") {
    message("Error: batchSize not integer.")
    break()
  } else if (!is.vector(x) | length(x) < 100) {
    message("Error: x not vector or length(x) < 100.")
    break()
  } else {
    batchNum <- ceiling(length(x)/batchSize)
    startBatchIx <- c(1:batchNum) * batchSize - batchSize + 1
    stopBatchIx <- c(1:batchNum) * batchSize
    stopBatchIx[batchNum] <- length(x)
    bIx <- vector(mode = "list", length = length(startBatchIx))
    for (i in 1:length(bIx)) {
      bIx[[i]]$start <- startBatchIx[i]
      bIx[[i]]$stop <- stopBatchIx[i]
    }
    return(bIx)
  }
}

### --- Config File
### --- Read WDCM paramereters
# - fPath: where the scripts is run from?
fPath <- as.character(commandArgs(trailingOnly = FALSE)[4])
fPath <- gsub("--file=", "", fPath, fixed = T)
fPath <- unlist(strsplit(fPath, split = "/", fixed = T))
fPath <- paste(
  paste(fPath[1:length(fPath) - 1], collapse = "/"),
  "/",
  sep = "")
params <- xmlParse(paste0(fPath, 'config_WDCM_Update_Labs.xml'))
params <- xmlToList(params)

### --- MySQL credentials
# - credentials on tools.labsdb
cred <- readLines(params$WDCM_main$mySQLCredentialsLocalPath)
mySQLCreds <- data.frame(user = gsub("^[[:alnum:]]+\\s=\\s", "", cred[2]),
                         password = gsub("^[[:alnum:]]+\\s=\\s", "", cred[3]),
                         stringsAsFactors = F)
rm(cred)
### --- list existing tables
con <- dbConnect(MySQL(), 
                 host = params$WDCM_main$mySQLhost, 
                 default.file = params$WDCM_main$mySQLdefault_file,
                 dbname = params$WDCM_main$mySQLdbname,
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)

q <- "SHOW TABLES;"
res <- dbSendQuery(con, q)
st <- fetch(res, -1)
dbClearResult(res)
dbDisconnect(con)
colnames(st) <- "tables"

### ---------------------------------------------------------------------------
### --- 1. WDCM Dashboards: General Update
### --- affected dashboards: Overview, Usage, Semantics
### ---------------------------------------------------------------------------

# - toReport:
print(paste0("Checking for WDCM MAIN update: ", as.character(Sys.time())))

### --- Check update timestamp in production:
### --- https://analytics.wikimedia.org/datasets/wmde-analytics-engineering/wdcm/

# - get update string:
destfile = params$WDCM_main$updateStringLocalPath
URL <- params$WDCM_main$updateStringRemotePath
h <- new_handle()
handle_setopt(h,
              copypostfields = "WDCM Main");
handle_setheaders(h,
                  "Cache-Control" = "no-cache"
)
curl_download(URL, handle = h, destfile = destfile)
timestampProduction <- read.csv(params$WDCM_main$updateStringLocalPath,
                               header = T,
                               check.names = F,
                               stringsAsFactors = F, 
                               row.names = 1)
# - extract timestamp of the latest production run:
tProd <- timestampProduction[max(which(grepl("Orchestra END", timestampProduction$Step))), ]$Time
tProd <- as.POSIXct(tProd, format = "%Y-%m-%d %H:%M:%S")

### --- Check local timestamp on Labs:
timestampLocal <- readChar(params$WDCM_main$localTimestampPath, 
                           file.info(params$WDCM_main$localTimestampPath)$size)
timestampLocal <- gsub("\\n", "", timestampLocal)
tLoc <- as.POSIXct(timestampLocal, format = "%Y-%m-%d %H:%M:%S")

### --- Compare with the current local timestamp
### --- and update if necessary:

# - toReport:
print("Checking update timestamps for the WDCM main update.")

if (tLoc < tProd) {
  
  # - toReport:
  print(paste0("WDCM main update started on: ", as.character(Sys.time())))
  
  # - update local timestamp:
  timestampLocal <- as.character(tProd)
  write(timestampLocal, file = params$WDCM_main$localTimestampPath)

  ### --- download ETL outputs:
    
  # - list files:
  url <- params$WDCM_main$remoteETLdir
  page <- as.character(GET(url))
  links <- str_extract_all(page, "<a href=.+>.+</a>")
  links <- sapply(links, function(x) {str_extract_all(x, ">.+<")})
  links <- sapply(links, function(x) {gsub('^>|"|<$|>|<', "", x)})
  links <- links[3:length(links)]
  
  # - clear output dir:
  setwd(params$WDCM_main$localFilesDir)
  lF <- list.files()
  rmF <- file.remove(lF)
  
  # - Download files to: localFilesDir
  destDir <- params$WDCM_main$localFilesDir
  for (i in 1:length(links)) {
    # - to Report
    print(paste("Downloading file: ", links[i], ": ", i, "/", length(links), sep = ""))
    # - new curl handle:
    h <- new_handle()
    # - set curl options:
    handle_setopt(h,
                  copypostfields = "WDCM Main Update");
    handle_setheaders(h,
                      "Cache-Control" = "no-cache"
    )
    # - download:
    curl_download(url = paste(url, links[i], sep = ""), 
                  handle = h, 
                  destfile = paste(destDir, links[i], sep = "")
                  )
  }
  
  ### --- download ML outputs:
  
  # - list files:
  url <- params$WDCM_main$remoteMLdir
  page <- as.character(GET(url))
  links <- str_extract_all(page, "<a href=.+>.+</a>")
  links <- sapply(links, function(x) {str_extract_all(x, ">.+<")})
  links <- sapply(links, function(x) {gsub('^>|"|<$|>|<', "", x)})
  links <- links[3:length(links)]
  
  # - Download files to: params$WDCM_main$localFilesDir
  destDir <- params$WDCM_main$localFilesDir
  for (i in 1:length(links)) {
    # - to Report
    print(paste("Downloading file: ", links[i], ": ", i, "/", length(links), sep = ""))
    # - new curl handle:
    h <- new_handle()
    # - set curl options:
    handle_setopt(h,
                  copypostfields = "WDCM Main Update");
    handle_setheaders(h,
                      "Cache-Control" = "no-cache"
    )
    # - download:
    curl_download(url = paste(url, links[i], sep = ""), 
                  handle = h, 
                  destfile = paste(destDir, links[i], sep = "")
    )
  }
  
  ### --- fetch WDCM_MainReport.csv and place to:
  ### --- params$WDCM_main$localSystemUpdateFile
  # - new curl handle:
  h <- new_handle()
  # - set curl options:
  handle_setopt(h,
                copypostfields = "WDCM Main Update");
  handle_setheaders(h,
                    "Cache-Control" = "no-cache"
  )
  # - download:
  curl_download(url = params$WDCM_main$updateStringRemotePath, 
                handle = h, 
                destfile = params$WDCM_main$localSystemUpdateFile)
  
  # - set updateDir
  updateDir <- params$WDCM_main$localSystemUpdateDir
  setwd(updateDir)
  
  ### --- start WDCM Dashboards Update:
  # - remove previous runtime report:
  file.remove(params$WDCM_main$localProcessUpdateLogFilename)
  nohupReport <- system(command = params$WDCM_main$localProcessUpdateCommand, intern = T)
  write(nohupReport, params$WDCM_main$localProcessUpdateLogFilename)
  
  ### --- Update Report:
  lF <- list.files()
  if (params$WDCM_main$localUpdateReporFilename %in% lF) {
    updateReport <- read.csv(params$WDCM_main$localUpdateReporFilename,
                             header = T,
                             row.names = 1,
                             check.names = F,
                             stringsAsFactors = F)
    newReport <- data.frame(Step = 'WDCM Update',
                            Time = as.character(Sys.time()),
                            stringsAsFactors = F)
    updateReport <- rbind(updateReport, newReport)
    write.csv(updateReport, params$WDCM_main$localUpdateReporFilename)
  } else {
    newReport <- data.frame(Step = 'WDCM Update',
                            Time = as.character(Sys.time()),
                            stringsAsFactors = F)
    write.csv(newReport, params$WDCM_main$localUpdateReporFilename)
  }

}

### ---------------------------------------------------------------------------
### --- 2. WDCM GeoDashboard Update
### --- affected dashboards: GeoDashboard
### ---------------------------------------------------------------------------

### --- Check update timestamp in production:
### --- https://analytics.wikimedia.org/datasets/wdcm/toLabsReport.csv
# - new curl handle:
h <- new_handle()
# - set curl options:
handle_setopt(h,
              copypostfields = "WDCM Main Update");
handle_setheaders(h,
                  "Cache-Control" = "no-cache"
)
# - download:
curl_download(url = 'https://analytics.wikimedia.org/datasets/wdcm/toLabsGeoReport.csv', 
              handle = h, 
              destfile = '/home/goransm/WMDE/WDCM/WDCM_SystemUpdate/timestampGeoProduction.csv'
)

timestampProduction <- read.csv('/home/goransm/WMDE/WDCM/WDCM_SystemUpdate/timestampGeoProduction.csv',
                                header = T,
                                check.names = F,
                                stringsAsFactors = F, 
                                row.names = 1)
tProd <- as.POSIXct(timestampProduction$timeStamp, format = "%Y-%m-%d %H:%M:%S")

### --- Check local timestamp on Labs:
timestampLocal <- read.csv('/home/goransm/WMDE/WDCM/WDCM_SystemUpdate/timestampGeoLocal.csv',
                           header = T,
                           check.names = F,
                           stringsAsFactors = F,
                           row.names = 1)
tLoc <- as.POSIXct(timestampLocal$timeStamp, format = "%Y-%m-%d %H:%M:%S")

### --- Compare with the current local timestamp
### --- and update if necessary:

# - toReport:
print("Comparing update timestamps for GeoDashboard.")

if (tLoc < tProd) {
  
  # - toReport:
  print(paste0("GeoDashbord update started on: ", as.character(Sys.time())))
  
  # - update local timestamp:
  timestampLocal$timeStamp <- timestampProduction$timeStamp
  write.csv(timestampLocal, file = '/home/goransm/WMDE/WDCM/WDCM_SystemUpdate/timestampGeoLocal.csv')
  
  # - list files:
  url <- 'https://analytics.wikimedia.org/datasets/wdcm/'
  page <- as.character(GET(url))
  links <- str_extract_all(page, "<a href=.+>.+</a>")
  links <- sapply(links, function(x) {str_extract_all(x, ">.+<")})
  links <- sapply(links, function(x) {gsub('^>|"|<$|>|<', "", x)})
  links <- links[3:length(links)]
  links <- links[which(grepl("^wdcm_geoitem_", links))]
  
  # - Download files to: /home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_DataIN_ClientUsage_v2
  destDir <- '/home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_DataIN_ClientUsage_v2/'
  for (i in 1:length(links)) {
    # - to nohup:
    print(paste("GeoDashboard - Downloading file: ", links[i], ": ", i, "/", length(links), sep = ""))
    # - new curl handle:
    h <- new_handle()
    # - set curl options:
    handle_setopt(h,
                  copypostfields = "WDCM Main Update");
    handle_setheaders(h,
                      "Cache-Control" = "no-cache"
    )
    # - download:
    curl_download(url = paste(url, links[i], sep = ""), 
                  handle = h, 
                  destfile = paste(destDir, links[i], sep = "")
    )
  }
  
  ### --- fetch toLabsReport.csv and place to:
  ### --- /home/goransm/WMDE/WDCM/WDCM_SystemUpdate/
  # - new curl handle:
  h <- new_handle()
  # - set curl options:
  handle_setopt(h,
                copypostfields = "WDCM Main Update");
  handle_setheaders(h,
                    "Cache-Control" = "no-cache"
  )
  # - download:
  curl_download(url = 'https://analytics.wikimedia.org/datasets/wdcm/toLabsGeoReport.csv',
                handle = h, 
                destfile = '/home/goransm/WMDE/WDCM/WDCM_SystemUpdate/toLabsGeoReport.csv'
                )
  
  
  # - Copy GeoDashboard Data Sets to Dashboard /srv
  setwd(destDir)
  lF <- list.files()
  lF <- lF[grepl("^wdcm_geoitem_", lF)]
  # - escape white space in file names
  lFiles <- sapply(lF, function(x) {
    gsub(" ", "\\ ", x, fixed = T)
  })
  for (i in 1:length(lF)) {
    system(paste('sudo cp ', destDir, 
                 lFiles[i], 
                 ' /srv/shiny-server/WDCM_GeoDashboard/data/',
                 sep = ''),
           wait = T)
  }
  
  ### --- Copy toLabsGeoReport to Dashboard /srv
  system('sudo cp /home/goransm/WMDE/WDCM/WDCM_SystemUpdate/toLabsGeoReport.csv /srv/shiny-server/WDCM_GeoDashboard/update/', 
         wait = T)

  ### --- Update Report:
  # - set updateDir
  updateDir <- params$WDCM_main$localSystemUpdateDir
  setwd(updateDir)
  lF <- list.files()
  if (params$WDCM_main$localUpdateReporFilename %in% lF) {
    updateReport <- read.csv(params$WDCM_main$localUpdateReporFilename,
                             header = T,
                             row.names = 1,
                             check.names = F,
                             stringsAsFactors = F)
    newReport <- data.frame(Step = 'WDCM Geo Update',
                            Time = as.character(Sys.time()),
                            stringsAsFactors = F)
    updateReport <- rbind(updateReport, newReport)
    write.csv(updateReport, params$WDCM_main$localUpdateReporFilename)
  } else {
    newReport <- data.frame(Step = 'WDCM Geo Update',
                            Time = as.character(Sys.time()),
                            stringsAsFactors = F)
    write.csv(newReport, params$WDCM_main$localSystemUpdateDir)
  }
  
}


### ---------------------------------------------------------------------------
### --- 3. WDCM Dashboards: Biases Dashboard
### --- affected dashboards: WDCM Biases
### ---------------------------------------------------------------------------

### --- fetch WDCM Biases Dashboard Data and place to:
### --- /home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/

# - toReport:
print(paste0("Updating the WDCM Biases Dashboard now:", Sys.time()))

### --- update File
# - new curl handle:
h <- new_handle()
# - set curl options:
handle_setopt(h,
              copypostfields = "WDCM Main Update");
handle_setheaders(h,
                  "Cache-Control" = "no-cache"
                  )
# - download:
curl_download(url = 'https://analytics.wikimedia.org/datasets/wdcm/update_WDCMBiases.csv',
              handle = h, 
              destfile = '/home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/update_WDCMBiases.csv'
              )

### --- data

# - genderProjectDataSet.csv
# - new curl handle:
h <- new_handle()
# - set curl options:
handle_setopt(h,
              copypostfields = "WDCM Main Update");
handle_setheaders(h,
                  "Cache-Control" = "no-cache"
)
# - download:
curl_download(url = 'https://analytics.wikimedia.org/datasets/wdcm/genderProjectDataSet.csv',
              handle = h, 
              destfile = '/home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/genderProjectDataSet.csv'
)

# - globalIndicators.csv
# - new curl handle:
h <- new_handle()
# - set curl options:
handle_setopt(h,
              copypostfields = "WDCM Main Update");
handle_setheaders(h,
                  "Cache-Control" = "no-cache"
)
# - download:
curl_download(url = 'https://analytics.wikimedia.org/datasets/wdcm/globalIndicators.csv',
              handle = h, 
              destfile = '/home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/globalIndicators.csv'
)

# - mfPropProject.csv
h <- new_handle()
# - set curl options:
handle_setopt(h,
              copypostfields = "WDCM Main Update");
handle_setheaders(h,
                  "Cache-Control" = "no-cache"
)
# - download:
curl_download(url = 'https://analytics.wikimedia.org/datasets/wdcm/mfPropProject.csv',
              handle = h, 
              destfile = '/home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/mfPropProject.csv'
)

# - occUsage.csv
h <- new_handle()
# - set curl options:
handle_setopt(h,
              copypostfields = "WDCM Main Update");
handle_setheaders(h,
                  "Cache-Control" = "no-cache"
)
# - download:
curl_download(url = 'https://analytics.wikimedia.org/datasets/wdcm/occUsage.csv',
              handle = h, 
              destfile = '/home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/occUsage.csv'
)

### --- visuals

# - genderUsage_Distribution.png
h <- new_handle()
# - set curl options:
handle_setopt(h,
              copypostfields = "WDCM Main Update");
handle_setheaders(h,
                  "Cache-Control" = "no-cache"
)
# - download:
curl_download(url = 'https://analytics.wikimedia.org/datasets/wdcm/genderUsage_Distribution.png',
              handle = h, 
              destfile = '/home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/genderUsage_Distribution.png'
)

# - genderUsage_Distribution_jitterp.png
h <- new_handle()
# - set curl options:
handle_setopt(h,
              copypostfields = "WDCM Main Update");
handle_setheaders(h,
                  "Cache-Control" = "no-cache"
)
# - download:
curl_download(url = 'https://analytics.wikimedia.org/datasets/wdcm/genderUsage_Distribution_jitterp.png',
              handle = h, 
              destfile = '/home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/genderUsage_Distribution_jitterp.png'
)

# - M_Items_Distribution.png
h <- new_handle()
# - set curl options:
handle_setopt(h,
              copypostfields = "WDCM Main Update");
handle_setheaders(h,
                  "Cache-Control" = "no-cache"
)
# - download:
curl_download(url = 'https://analytics.wikimedia.org/datasets/wdcm/M_Items_Distribution.png',
              handle = h, 
              destfile = '/home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/M_Items_Distribution.png'
)

# - F_Items_Distribution.png
h <- new_handle()
# - set curl options:
handle_setopt(h,
              copypostfields = "WDCM Main Update");
handle_setheaders(h,
                  "Cache-Control" = "no-cache"
)
# - download:
curl_download(url = 'https://analytics.wikimedia.org/datasets/wdcm/F_Items_Distribution.png',
              handle = h, 
              destfile = '/home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/F_Items_Distribution.png'
)

# - Gender_LorenzCurves.png
h <- new_handle()
# - set curl options:
handle_setopt(h,
              copypostfields = "WDCM Main Update");
handle_setheaders(h,
                  "Cache-Control" = "no-cache"
)
# - download:
curl_download(url = 'https://analytics.wikimedia.org/datasets/wdcm/Gender_LorenzCurves.png',
              handle = h, 
              destfile = '/home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/Gender_LorenzCurves.png'
)

### --- copy all WDCM Biases DATA files and the UPDATE file to
### --- /srv/shiny-server/WDCM_BiasesDashboard/data/

# - toReport:
print(paste0("Copying the WDCM Biases Dashboard files now:", Sys.time()))

# - update
system('sudo cp /home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/update_WDCMBiases.csv /srv/shiny-server/WDCM_BiasesDashboard/data/', 
       wait = T)

# - genderProjectDataSet.csv
system('sudo cp /home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/genderProjectDataSet.csv /srv/shiny-server/WDCM_BiasesDashboard/data/', 
       wait = T)

# - globalIndicators.csv
system('sudo cp /home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/globalIndicators.csv /srv/shiny-server/WDCM_BiasesDashboard/data/', 
       wait = T)

# - mfPropProject.csv
system('sudo cp /home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/mfPropProject.csv /srv/shiny-server/WDCM_BiasesDashboard/data/', 
       wait = T)

# - occUsage.csv
system('sudo cp /home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/occUsage.csv /srv/shiny-server/WDCM_BiasesDashboard/data/', 
       wait = T)

### --- copy all WDCM Biases VISUAL files to
### --- /srv/shiny-server/WDCM_BiasesDashboard/www/

# - genderUsage_Distribution.png
system('sudo cp /home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/genderUsage_Distribution.png /srv/shiny-server/WDCM_BiasesDashboard/www/', 
       wait = T)

# - genderUsage_Distribution_jitterp.png
system('sudo cp /home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/genderUsage_Distribution_jitterp.png /srv/shiny-server/WDCM_BiasesDashboard/www/', 
       wait = T)

# - M_Items_Distribution.png
system('sudo cp /home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/M_Items_Distribution.png /srv/shiny-server/WDCM_BiasesDashboard/www/', 
       wait = T)

# - F_Items_Distribution.png
system('sudo cp /home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/F_Items_Distribution.png /srv/shiny-server/WDCM_BiasesDashboard/www/', 
       wait = T)

# - Gender_LorenzCurves.png
system('sudo cp /home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/Gender_LorenzCurves.png /srv/shiny-server/WDCM_BiasesDashboard/www/', 
       wait = T)

### ---------------------------------------------------------------------------
### --- 5. WDCM: WDCM (S)itelinks Dashboard
### --- affected dashboards: WDCM (S)itelinks Dashboard
### ---------------------------------------------------------------------------

# - toReport:
print(paste0("WDCM: Wikipedia (S)itelinks update started on: ", as.character(Sys.time())))

# - get update string:
destfile = '/home/goransm/WMDE/WDCM/WDCM_SitelinksDashboard/data/wikipediaSitelinksUpdateString.txt'
URL <-
  "https://analytics.wikimedia.org/datasets/wdcm/WDCM_Sitelinks/wikipediaSitelinksUpdateString.txt"
h <- new_handle()
handle_setopt(h,
              copypostfields = "WDCM Wikipedia Sitelinks");
handle_setheaders(h,
                  "Cache-Control" = "no-cache"
)
req <- curl_fetch_memory(URL, handle = h)
req <- rawToChar(req$content)
req <- gsub("\\n", "", req)
write(req, destfile)

# - compare with stored update string:
newUpdateString <- readLines('/home/goransm/WMDE/WDCM/WDCM_SitelinksDashboard/data/wikipediaSitelinksUpdateString.txt')
existingUpdateString <- readLines('/srv/shiny-server/WDCM_SitelinksDashboard/data/wikipediaSitelinksUpdateString.txt')

# - update if necessary:

# - toReport:
print("Comparing update timestamps for the WDCM Wikipedia Sitelinks update now.")

if (newUpdateString != existingUpdateString) {

  # - toReport:
  print(paste0("WDCM Wikipedia Sitelinks download started on: ", as.character(Sys.time())))

  # - copy update string to: /srv/shiny-server/WDCM_SitelinksDashboard/data
  system(command = 'sudo cp /home/goransm/WMDE/WDCM/WDCM_SitelinksDashboard/data/wikipediaSitelinksUpdateString.txt /srv/shiny-server/WDCM_SitelinksDashboard/data/wikipediaSitelinksUpdateString.txt',
         wait = T)


  # - touch https://analytics.wikimedia.org/datasets/wdcm/WDCM_Sitelinks/
  # - list files:
  url <- 'https://analytics.wikimedia.org/datasets/wdcm/WDCM_Sitelinks/'
  page <- as.character(GET(url))
  links <- str_extract_all(page, "<a href=.+>.+</a>")
  links <- sapply(links, function(x) {str_extract_all(x, ">.+<")})
  links <- sapply(links, function(x) {gsub('^>|"|<$|>|<', "", x)})
  links <- links[3:length(links)]
  links <- links[!grepl("/", links, fixed = T)]
  # - clean up README.txt
  wREADME <- which(grepl("README", links))
  if (length(wREADME) > 0) {links <- links[-wREADME]}
  # - clean up sitelinks data BIG files:
  wSitelinks <- which(grepl("^siteLinks", links))
  if (length(wSitelinks) > 0) {links <- links[-wSitelinks]}
  # - download files:
  for (i in 1:length(links)) {
    download.file(paste0(url, links[i]),
                  destfile = paste0('/home/goransm/WMDE/WDCM/WDCM_SitelinksDashboard/data/', links[i]),
                  quiet = T)
  }
  
  # - toReport:
  print(paste0("WDCM Wikimedia Sitelinks copy files started on: ", as.character(Sys.time())))

  # - migrate to /srv/shiny-server/
  system(command =
           'sudo cp /home/goransm/WMDE/WDCM/WDCM_SitelinksDashboard/data/* /srv/shiny-server/WDCM_SitelinksDashboard/data',
         wait = T)

  # - toReport:
  print(paste0("WDCM (S)itelinks update completed on: ", as.character(Sys.time())))


} else {

  # - toReport:
  print(paste0("WDCM (S)itelinks update not run: update strings are matched. At: ", as.character(Sys.time())))

}

### ---------------------------------------------------------------------------
### --- 7. WDCM: WDCM (T)itles Dashboard
### --- affected dashboards: WDCM (T)itles Dashboard
### ---------------------------------------------------------------------------

# - toReport:
print(paste0("WDCM: Wikipedia (T)itles update started on: ", as.character(Sys.time())))

# - get update string:
destfile = '/home/goransm/WMDE/WDCM/WDCM_TitlesDashboard/data/wikipediaTitlesUpdateString.txt'
URL <-
  "https://analytics.wikimedia.org/datasets/wdcm/WDCM_Titles/wikipediaTitlesUpdateString.txt"
h <- new_handle()
handle_setopt(h,
              copypostfields = "WDCM Wikipedia Titles");
handle_setheaders(h,
                  "Cache-Control" = "no-cache"
)
req <- curl_fetch_memory(URL, handle = h)
req <- rawToChar(req$content)
req <- gsub("\\n", "", req)
write(req, destfile)

# - compare with stored update string:
newUpdateString <- readLines('/home/goransm/WMDE/WDCM/WDCM_TitlesDashboard/data/wikipediaTitlesUpdateString.txt')
existingUpdateString <- readLines('/srv/shiny-server/WDCM_TitlesDashboard/data/wikipediaTitlesUpdateString.txt')

# - update if necessary:

# - toReport:
print("Comparing update timestamps for the WDCM Wikipedia Titles update now.")

if (newUpdateString != existingUpdateString) {
  
  # - toReport:
  print(paste0("WDCM Wikipedia Titles download started on: ", as.character(Sys.time())))
  
  # - copy update string to: /srv/shiny-server/WDCM_TitlesDashboard/data/
  system(command = 'sudo cp /home/goransm/WMDE/WDCM/WDCM_TitlesDashboard/data/wikipediaTitlesUpdateString.txt /srv/shiny-server/WDCM_TitlesDashboard/data/wikipediaTitlesUpdateString.txt',
         wait = T)
  
  
  # - touch https://analytics.wikimedia.org/datasets/wdcm/WDCM_Titles/
  # - list files:
  url <- 'https://analytics.wikimedia.org/datasets/wdcm/WDCM_Titles/'
  page <- as.character(GET(url))
  links <- str_extract_all(page, "<a href=.+>.+</a>")
  links <- sapply(links, function(x) {str_extract_all(x, ">.+<")})
  links <- sapply(links, function(x) {gsub('^>|"|<$|>|<', "", x)})
  links <- links[3:length(links)]
  links <- links[!grepl("/", links, fixed = T)]
  # - clean up README.txt
  wREADME <- which(grepl("README", links))
  if (length(wREADME) > 0) {links <- links[-wREADME]}
  # - clean up sitelinks data BIG files:
  wSitelinks <- which(grepl("^titles", links))
  if (length(wSitelinks) > 0) {links <- links[-wSitelinks]}
  # - download files:
  for (i in 1:length(links)) {
    download.file(paste0(url, links[i]),
                  destfile = paste0('/home/goransm/WMDE/WDCM/WDCM_TitlesDashboard/data/', links[i]),
                  quiet = T)
  }
  
  # - toReport:
  print(paste0("WDCM Wikimedia Titles copy files started on: ", as.character(Sys.time())))
  
  # - migrate to /srv/shiny-server/
  system(command =
           'sudo cp /home/goransm/WMDE/WDCM/WDCM_TitlesDashboard/data/* /srv/shiny-server/WDCM_TitlesDashboard/data',
         wait = T)
  
  # - toReport:
  print(paste0("WDCM (T)titles update completed on: ", as.character(Sys.time())))
  
  
} else {
  
  # - toReport:
  print(paste0("WDCM (T)itles update not run: update strings are matched. At: ", as.character(Sys.time())))
  
}

# - toReport:
print("Restarting Shiny Server now.")

###### ------- Restart Shiny Server
system('sudo systemctl restart shiny-server',
       wait = T)
###### ------- Restart Shiny Server END

# - toReport:
print(paste0("WDCM_Update_Labs run completed on: ", as.character(Sys.time())))
print("--------------------------------------------------------------------")


