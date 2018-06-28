### --- WDCM Process Module, v. Beta 0.1
### --- Script: WDCM_Update_Labs.R, v. Beta 0.1
### ---------------------------------------------------------------------------
### --- DESCRIPTION:
### --- WDCM_Update_Labs.R runs on cron from wikidataconcepts.eqiad.wmflabs
### --- periodically checking the changes in /srv/published-datasets/wdcm
### --- on stat1005 by accessing https://analytics.wikimedia.org/datasets/wdcm/
### --- onto which the /srv/published-datasets/wdcm from stat1005
### --- is mapped.
### --- This script will check the timestamp in toLabsReport.csv
### --- and compare it to a locally stored version.
### --- If the timestamp found in production is newer, the script
### --- will copy all files from https://analytics.wikimedia.org/datasets/wdcm/
### --- to: /home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_DataIN_ClientUsage_v2
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

### --- Setup
library(httr)
library(stringr)

### ---------------------------------------------------------------------------
### --- 1. WDCM Dashboards: General Update
### --- affected dashboards: Overview, Usage, Semantics
### ---------------------------------------------------------------------------

### --- Check update timestamp in production:
### --- https://analytics.wikimedia.org/datasets/wdcm/toLabsReport.csv
download.file('https://analytics.wikimedia.org/datasets/wdcm/toLabsReport.csv',
              destfile = '/home/goransm/WMDE/WDCM/WDCM_SystemUpdate/timestampProducton.csv',
              quiet = T)
timestampProduction <- read.csv('/home/goransm/WMDE/WDCM/WDCM_SystemUpdate/timestampProducton.csv',
                               header = T,
                               check.names = F,
                               stringsAsFactors = F, 
                               row.names = 1)
tProd <- as.POSIXct(timestampProduction$timeStamp, format = "%Y-%m-%d %H:%M:%S")

### --- Check local timestamp on Labs:
timestampLocal <- read.csv('/home/goransm/WMDE/WDCM/WDCM_SystemUpdate/timestampLocal.csv',
                           header = T,
                           check.names = F,
                           stringsAsFactors = F,
                           row.names = 1)
tLoc <- as.POSIXct(timestampLocal$timeStamp, format = "%Y-%m-%d %H:%M:%S")


### --- Compare with the current local timestamp
### --- and update if necessary:
if (tLoc < tProd) {
  
  # - update local timestamp:
  timestampLocal$timeStamp <- timestampProduction$timeStamp
  write.csv(timestampLocal, file = '/home/goransm/WMDE/WDCM/WDCM_SystemUpdate/timestampLocal.csv')
  
  # - list files:
  url <- 'https://analytics.wikimedia.org/datasets/wdcm/'
  page <- as.character(GET(url))
  links <- str_extract_all(page, "<a href=.+>.+</a>")
  links <- sapply(links, function(x) {str_extract_all(x, ">.+<")})
  links <- sapply(links, function(x) {gsub('^>|"|<$|>|<', "", x)})
  links <- links[3:length(links)]
  
  # - clean up:
  wREADME <- which(grepl("README", links))
  links <- links[-wREADME]
  wtoLabsReport <- which(grepl("toLabs", links))
  links <- links[-wtoLabsReport]
  wWDCM_MainReport <- which(grepl("WDCM_MainReport", links))
  links <- links[-wWDCM_MainReport]
  
  # - clear output dir:
  setwd('/home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_DataIN_ClientUsage_v2')
  lF <- list.files()
  rmF <- file.remove(lF)
  
  # - Download files to: /home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_DataIN_ClientUsage_v2
  destDir <- '/home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_DataIN_ClientUsage_v2/'
  for (i in 1:length(links)) {
    # - to nohup:
    print(paste("Downloading file: ", links[i], ": ", i, "/", length(links), sep = ""))
    download.file(url = paste(url, links[i], sep = ""), 
                  destfile = paste(destDir, links[i], sep = ""), 
                  quiet = T)
  }
  
  ### --- fetch toLabsReport.csv and place to:
  ### --- /home/goransm/WMDE/WDCM/WDCM_SystemUpdate/
  download.file('https://analytics.wikimedia.org/datasets/wdcm/toLabsReport.csv',
                destfile = '/home/goransm/WMDE/WDCM/WDCM_SystemUpdate/toLabsReport.csv',
                quiet = T)
  
  # - set updateDir
  updateDir <- '/home/goransm/WMDE/WDCM/WDCM_SystemUpdate/'
  setwd(updateDir)
  
  ### --- start WDCM Dashboards Update:
  # - remove previous runtime report:
  file.remove('WDCM_Update_Process_nohupReport.txt')
  nohupReport <- system(command = 'nohup Rscript /home/goransm/WMDE/WDCM/WDCM_RScripts/WDCM_Process.R', intern = T)
  write(nohupReport, 'WDCM_Update_Process_nohupReport.txt')
  
  ### --- Update Report:
  lF <- list.files()
  if ('WDCM_SystemUpdateReport.csv' %in% lF) {
    updateReport <- read.csv('WDCM_SystemUpdateReport.csv',
                             header = T,
                             row.names = 1,
                             check.names = F,
                             stringsAsFactors = F)
    newReport <- data.frame(Step = 'WDCM Update',
                            Time = as.character(Sys.time()),
                            stringsAsFactors = F)
    updateReport <- rbind(updateReport, newReport)
    write.csv(updateReport, 'WDCM_SystemUpdateReport.csv')
  } else {
    newReport <- data.frame(Step = 'WDCM Update',
                            Time = as.character(Sys.time()),
                            stringsAsFactors = F)
    write.csv(newReport, 'WDCM_SystemUpdateReport.csv')
  }

}

### ---------------------------------------------------------------------------
### --- 2. WDCM GeoDashboard Update
### --- affected dashboards: GeoDashboard
### ---------------------------------------------------------------------------

### --- Check update timestamp in production:
### --- https://analytics.wikimedia.org/datasets/wdcm/toLabsReport.csv
download.file('https://analytics.wikimedia.org/datasets/wdcm/toLabsGeoReport.csv',
              destfile = '/home/goransm/WMDE/WDCM/WDCM_SystemUpdate/timestampGeoProduction.csv',
              quiet = T)
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
if (tLoc < tProd) {
  
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
    download.file(url = paste(url, links[i], sep = ""), 
                  destfile = paste(destDir, links[i], sep = ""), 
                  quiet = T)
  }
  
  ### --- fetch toLabsReport.csv and place to:
  ### --- /home/goransm/WMDE/WDCM/WDCM_SystemUpdate/
  download.file('https://analytics.wikimedia.org/datasets/wdcm/toLabsGeoReport.csv',
                destfile = '/home/goransm/WMDE/WDCM/WDCM_SystemUpdate/toLabsGeoReport.csv',
                quiet = T)
  
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
           wait = F)
  }
  
  ### --- Copy toLabsGeoReport to Dashboard /srv
  system('sudo cp /home/goransm/WMDE/WDCM/WDCM_SystemUpdate/toLabsGeoReport.csv /srv/shiny-server/WDCM_GeoDashboard/update/', 
         wait = F)

  ### --- Update Report:
  # - set updateDir
  updateDir <- '/home/goransm/WMDE/WDCM/WDCM_SystemUpdate/'
  setwd(updateDir)
  lF <- list.files()
  if ('WDCM_SystemUpdateReport.csv' %in% lF) {
    updateReport <- read.csv('WDCM_SystemUpdateReport.csv',
                             header = T,
                             row.names = 1,
                             check.names = F,
                             stringsAsFactors = F)
    newReport <- data.frame(Step = 'WDCM Geo Update',
                            Time = as.character(Sys.time()),
                            stringsAsFactors = F)
    updateReport <- rbind(updateReport, newReport)
    write.csv(updateReport, 'WDCM_SystemUpdateReport.csv')
  } else {
    newReport <- data.frame(Step = 'WDCM Geo Update',
                            Time = as.character(Sys.time()),
                            stringsAsFactors = F)
    write.csv(newReport, 'WDCM_SystemUpdateReport.csv')
  }
  
}


### ---------------------------------------------------------------------------
### --- 3. WDCM Dashboards: Biases Dashboard
### --- affected dashboards: WDCM Biases
### ---------------------------------------------------------------------------

### --- fetch WDCM Biases Dashboard Data and place to:
### --- /home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/

### --- update File
download.file('https://analytics.wikimedia.org/datasets/wdcm/update_WDCMBiases.csv',
              destfile = '/home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/update_WDCMBiases.csv',
              quiet = T)

### --- data

# - genderProjectDataSet.csv
download.file('https://analytics.wikimedia.org/datasets/wdcm/genderProjectDataSet.csv',
              destfile = '/home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/genderProjectDataSet.csv',
              quiet = T)
# - globalIndicators.csv
download.file('https://analytics.wikimedia.org/datasets/wdcm/globalIndicators.csv',
              destfile = '/home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/globalIndicators.csv',
              quiet = T)
# - mfPropProject.csv
download.file('https://analytics.wikimedia.org/datasets/wdcm/mfPropProject.csv',
              destfile = '/home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/mfPropProject.csv',
              quiet = T)
# - occUsage.csv
download.file('https://analytics.wikimedia.org/datasets/wdcm/occUsage.csv',
              destfile = '/home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/occUsage.csv',
              quiet = T)

### --- visuals

# - genderUsage_Distribution.png
download.file('https://analytics.wikimedia.org/datasets/wdcm/genderUsage_Distribution.png',
              destfile = '/home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/genderUsage_Distribution.png',
              quiet = T)
# - genderUsage_Distribution_jitterp.png
download.file('https://analytics.wikimedia.org/datasets/wdcm/genderUsage_Distribution_jitterp.png',
              destfile = '/home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/genderUsage_Distribution_jitterp.png',
              quiet = T)
# - M_Items_Distribution.png
download.file('https://analytics.wikimedia.org/datasets/wdcm/M_Items_Distribution.png',
              destfile = '/home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/M_Items_Distribution.png',
              quiet = T)
# - F_Items_Distribution.png
download.file('https://analytics.wikimedia.org/datasets/wdcm/F_Items_Distribution.png',
              destfile = '/home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/F_Items_Distribution.png',
              quiet = T)
# - Gender_LorenzCurves.png
download.file('https://analytics.wikimedia.org/datasets/wdcm/Gender_LorenzCurves.png',
              destfile = '/home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/Gender_LorenzCurves.png',
              quiet = T)

### --- copy all WDCM Biases DATA files and the UPDATE file to
### --- /srv/shiny-server/WDCM_BiasesDashboard/_data/

# - update
system('sudo cp /home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/update_WDCMBiases.csv /srv/shiny-server/WDCM_BiasesDashboard/_data/', 
       wait = F)

# - genderProjectDataSet.csv
system('sudo cp /home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/genderProjectDataSet.csv /srv/shiny-server/WDCM_BiasesDashboard/_data/', 
       wait = F)

# - globalIndicators.csv
system('sudo cp /home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/globalIndicators.csv /srv/shiny-server/WDCM_BiasesDashboard/_data/', 
       wait = F)

# - mfPropProject.csv
system('sudo cp /home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/mfPropProject.csv /srv/shiny-server/WDCM_BiasesDashboard/_data/', 
       wait = F)

# - occUsage.csv
system('sudo cp /home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/occUsage.csv /srv/shiny-server/WDCM_BiasesDashboard/_data/', 
       wait = F)

### --- copy all WDCM Biases VISUAL files to
### --- /srv/shiny-server/WDCM_BiasesDashboard/www/

# - genderUsage_Distribution.png
system('sudo cp /home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/genderUsage_Distribution.png /srv/shiny-server/WDCM_BiasesDashboard/www/', 
       wait = F)

# - genderUsage_Distribution_jitterp.png
system('sudo cp /home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/genderUsage_Distribution_jitterp.png /srv/shiny-server/WDCM_BiasesDashboard/www/', 
       wait = F)

# - M_Items_Distribution.png
system('sudo cp /home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/M_Items_Distribution.png /srv/shiny-server/WDCM_BiasesDashboard/www/', 
       wait = F)

# - F_Items_Distribution.png
system('sudo cp /home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/F_Items_Distribution.png /srv/shiny-server/WDCM_BiasesDashboard/www/', 
       wait = F)

# - Gender_LorenzCurves.png
system('sudo cp /home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_BiasesDashboardUpdate/Gender_LorenzCurves.png /srv/shiny-server/WDCM_BiasesDashboard/www/', 
       wait = F)


### ---------------------------------------------------------------------------
### --- 4. Technical Wishlist: Advanced Search Extension Dashboard
### --- affected dashboards: Advanced Search Extension Dashboard
### ---------------------------------------------------------------------------

### --- data

# - asExtensionUpdate.csv
download.file('https://analytics.wikimedia.org/datasets/wmde-analytics-engineering/TechnicalWishes/AdvancedSearchExtension/asExtensionUpdate.csv',
              destfile = '/home/goransm/WMDE/TechnicalWishes/AdvancedSearchExtension/data/asExtensionUpdate.csv',
              quiet = T)

# - cirrusSearchUpdate.csv
download.file('https://analytics.wikimedia.org/datasets/wmde-analytics-engineering/TechnicalWishes/AdvancedSearchExtension/cirrusSearchUpdate.csv',
              destfile = '/home/goransm/WMDE/TechnicalWishes/AdvancedSearchExtension/data/cirrusSearchUpdate.csv',
              quiet = T)

# - cirrusSearchUpdateKeywords.csv
download.file('https://analytics.wikimedia.org/datasets/wmde-analytics-engineering/TechnicalWishes/AdvancedSearchExtension/cirrusSearchUpdateKeywords.csv',
              destfile = '/home/goransm/WMDE/TechnicalWishes/AdvancedSearchExtension/data/cirrusSearchUpdateKeywords.csv',
              quiet = T)

### --- copy data:

# - asExtensionUpdate.csv
system('sudo cp /home/goransm/WMDE/TechnicalWishes/AdvancedSearchExtension/data/asExtensionUpdate.csv /srv/shiny-server/TW_AdvancedSearchExtension/data/', 
       wait = F)

# - cirrusSearchUpdate.csv
system('sudo cp /home/goransm/WMDE/TechnicalWishes/AdvancedSearchExtension/data/cirrusSearchUpdate.csv /srv/shiny-server/TW_AdvancedSearchExtension/data/', 
       wait = F)

# - cirrusSearchUpdateKeywords.csv
system('sudo cp /home/goransm/WMDE/TechnicalWishes/AdvancedSearchExtension/data/cirrusSearchUpdateKeywords.csv /srv/shiny-server/TW_AdvancedSearchExtension/data/', 
       wait = F)


###### ------- Restart Shiny Server
# system('sudo restart shiny-server', 
#        wait = F)
###### ------- Restart Shiny Server END

