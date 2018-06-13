#!/usr/bin/env Rscript

### ---------------------------------------------------------------------------
### --- WDCM EngineBiases, v. Beta 0.1
### --- Script: WDCM_EngineBiases.R, v. Beta 0.1
### --- Author: Goran S. Milovanovic, Data Analyst, WMDE
### --- Developed under the contract between Goran Milovanovic PR Data Kolektiv
### --- and WMDE.
### --- Contact: goran.milovanovic_ext@wikimedia.de
### ---------------------------------------------------------------------------
### --- DESCRIPTION:
### --- WDCM_EngineBiases contacts the WDQS SPARQL end-point
### --- and fetches all Q5 (Human) item IDs
### --- that have Gender (P21) defined. 
### --- WDCM_EngineBiases connects to https://tools.wmflabs.org/wikidata-analysis/
### --- finds the latest WDTK generated update of geo-localized Wikidata items
### --- and downloads the respective wdlabel.json for further processing.
### --- The goal of the analysis is to present the Gender Bias as well as the 
### --- North-South Divide in Wikidata usage.
### --- The remainder of the script searches the Hive goransm.wdcm_maintable
### --- for usage data and prepares the export .tsv files.
### --- NOTE: the execution of this WDCM script is always dependent upon the
### --- previous WDCM_Sqoop_Clients.R run from stat1004 (currently).
### ---------------------------------------------------------------------------
### --- RUN FROM: /home/goransm/RScripts/WDCM_R
### --- nohup Rscript WDCM_EngineBiases.R &
### --- currently running on crontab
### --- from stat1005, user: goransm
### --- 45 23 8 * * export USER=goransm && nice -10 Rscript /home/goransm/RScripts/WDCM_R/WDCM_EngineBiases.R >> 
### ---- /home/goransm/RScripts/WDCM_R/WDCM_Logs/WDCM_EngineBiasesRuntimeLog.log 2>&1
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
# - Contact
library(httr)
library(jsonlite)
library(XML)
# - Wrangle
library(stringr)
library(data.table)
library(dplyr)
library(tidyr)
# - Analytics
library(ggplot2)
library(scales)
library(bayesAB)
library(ineq)

### --- Directories
# - fPath: where the scripts is run from?
fPath <- '/home/goransm/RScripts/WDCM_R'
# - log paths
logDir <- paste(fPath, '/WDCM_Logs', sep = "")
# - stat1005 published-datasets dir, maps onto
# - https://analytics.wikimedia.org/datasets/wdcm/
dataDir <- '/srv/published-datasets/wdcm'
# - temporary .tsv files dir
tempDataDir <- '/home/goransm/RScripts/WDCM_R/WDCM_Bias_tempData/'

### --- Functions
# - projectType() to determine project type
projectType <- function(projectName) {
  unname(sapply(projectName, function(x) {
    if (grepl("commons", x, fixed = T)) {"Commons"
    } else if (grepl("mediawiki|meta|species|wikidata", x)) {"Other"
    } else if (grepl("wiki$", x)) {"Wikipedia"
    } else if (grepl("quote$", x)) {"Wikiquote"
    } else if (grepl("voyage$", x)) {"Wikivoyage"
    } else if (grepl("news$", x)) {"Wikinews"
    } else if (grepl("source$", x)) {"Wikisource"
    } else if (grepl("wiktionary$", x)) {"Wiktionary"
    } else if (grepl("versity$", x)) {"Wikiversity"
    } else if (grepl("books$", x)) {"Wikibooks"
    } else {"Other"}
  }))
}

### ---  Remove everything from the temporary directory:
do.call(file.remove, list(list.files(tempDataDir, full.names = TRUE)))

### ---------------------------------------------------------------------------
### --- Step 1: Collect from WQDS
### ---------------------------------------------------------------------------

# - initate logging:
print(paste0("WDCM_Biases Regular Update: ", as.character(Sys.time())))

# - toLog:
print("Step 1: Collect from WQDS...")

# - set dataDir:
setwd(dataDir)

### --- Set proxy
Sys.setenv(
  http_proxy = "http://webproxy.eqiad.wmnet:8080",
  https_proxy = "http://webproxy.eqiad.wmnet:8080")

### --- 1A: Fetch all M/F items + Birthplace from WDQS

# - endPoint:
endPointURL <- "https://query.wikidata.org/bigdata/namespace/wdq/sparql?format=json&query="

# - fetch all P21: male (Q6581097) + placeOfBirth
query <- 'SELECT ?item ?placeOfBirth WHERE { ?item wdt:P21 wd:Q6581097; wdt:P31 wd:Q5. OPTIONAL { ?item wdt:P19 ?placeOfBirth } }'

# Run Query:
print("Contacting WDQS for M items...")
res <- GET(url = paste0(endPointURL, URLencode(query)))
print("Done.")
if (res$status_code == 200) {

  # JSON:
  rc <- fromJSON(rawToChar(res$content), simplifyDataFrame = T)

  # clear:
  rm(res); gc()

  # extract:
  # - to runtime Log:
  print(paste("Parsing now M items..."))
  # - as.data.frame:
  items <- data.frame(item = rc$results$bindings$item$value,
                      gender = 'M',
                      placeOfBirth = rc$results$bindings$placeOfBirth$value,
                      stringsAsFactors = F)
  items$item <- str_extract(items$item, "Q[[:digit:]]+")
  items$placeOfBirth <- str_extract(items$placeOfBirth, "Q[[:digit:]]+")
  rm(rc); gc()

  # - store
  write.csv(items, 'maleItems.csv')
  
  ### -----------------------------------
  # - indicators
  ### -----------------------------------
  nMaleItems <- length(unique(items$item))

  # - clear
  rm(items); gc()

} else {

  # - clear
  rm(rc); gc()

  # - report
  print(paste("Status code not 200 for M items..."))

}

# - fetch all P21: female (Q6581072) + placeOfBirth
query <- 'SELECT ?item ?placeOfBirth WHERE { ?item wdt:P21 wd:Q6581072; wdt:P31 wd:Q5. OPTIONAL { ?item wdt:P19 ?placeOfBirth } }'

# Run Query:
print("Contacting WDQS for F items...")
res <- GET(url = paste0(endPointURL, URLencode(query)))
print("Done.")
if (res$status_code == 200) {

  # JSON:
  rc <- fromJSON(rawToChar(res$content), simplifyDataFrame = T)

  # clear:
  rm(res); gc()

  # extract:
  # - to runtime Log:
  print(paste("Parsing now F items..."))
  # - as.data.frame:
  items <- data.frame(item = rc$results$bindings$item$value,
                      gender = 'F',
                      placeOfBirth = rc$results$bindings$placeOfBirth$value,
                      stringsAsFactors = F)
  items$item <- str_extract(items$item, "Q[[:digit:]]+")
  items$placeOfBirth <- str_extract(items$placeOfBirth, "Q[[:digit:]]+")
  rm(rc); gc()

  # - store
  write.csv(items, 'femaleItems.csv')
  
  ### -----------------------------------
  # - indicators
  ### -----------------------------------
  nFemaleItems <- length(unique(items$item))

  # - clear
  rm(items); gc()

} else {

  # - clear
  rm(rc); gc()

  # - report
  print(paste("Status code not 200 for F items..."))

}

# - fetch all P21: intersex (Q1097630) + placeOfBirth
query <- 'SELECT ?item ?placeOfBirth WHERE { ?item wdt:P21 wd:Q1097630; wdt:P31 wd:Q5. OPTIONAL { ?item wdt:P19 ?placeOfBirth } }'

# Run Query:
print("Contacting WDQS for Intersex items...")
res <- GET(url = paste0(endPointURL, URLencode(query)))
print("Done.")
if (res$status_code == 200) {
  
  # JSON:
  rc <- fromJSON(rawToChar(res$content), simplifyDataFrame = T)
  
  # clear:
  rm(res); gc()
  
  # extract:
  # - to runtime Log:
  print(paste("Parsing now Intersex items..."))
  # - as.data.frame:
  items <- data.frame(item = rc$results$bindings$item$value,
                      gender = 'F',
                      placeOfBirth = rc$results$bindings$placeOfBirth$value,
                      stringsAsFactors = F)
  items$item <- str_extract(items$item, "Q[[:digit:]]+")
  items$placeOfBirth <- str_extract(items$placeOfBirth, "Q[[:digit:]]+")
  rm(rc); gc()
  
  # - store
  write.csv(items, 'intersexItems.csv')
  
  ### -----------------------------------
  # - indicators
  ### -----------------------------------
  nIntersexItems <- length(unique(items$item))
  
  # - clear
  rm(items); gc()
  
} else {
  
  # - clear
  rm(rc); gc()
  
  # - report
  print(paste("Status code not 200 for Intersex items..."))
  
}

# - fetch all P21: transgender female (Q1052281) + placeOfBirth
query <- 'SELECT ?item ?placeOfBirth WHERE { ?item wdt:P21 wd:Q1052281; wdt:P31 wd:Q5. OPTIONAL { ?item wdt:P19 ?placeOfBirth } }'

# Run Query:
print("Contacting WDQS for transgender female items...")
res <- GET(url = paste0(endPointURL, URLencode(query)))
print("Done.")
if (res$status_code == 200) {
  
  # JSON:
  rc <- fromJSON(rawToChar(res$content), simplifyDataFrame = T)
  
  # clear:
  rm(res); gc()
  
  # extract:
  # - to runtime Log:
  print(paste("Parsing now transgender female items..."))
  # - as.data.frame:
  items <- data.frame(item = rc$results$bindings$item$value,
                      gender = 'F',
                      placeOfBirth = rc$results$bindings$placeOfBirth$value,
                      stringsAsFactors = F)
  items$item <- str_extract(items$item, "Q[[:digit:]]+")
  items$placeOfBirth <- str_extract(items$placeOfBirth, "Q[[:digit:]]+")
  rm(rc); gc()
  
  # - store
  write.csv(items, 'transFItems.csv')
  
  ### -----------------------------------
  # - indicators
  ### -----------------------------------
  nTransFItems <- length(unique(items$item))
  
  # - clear
  rm(items); gc()
  
} else {
  
  # - clear
  rm(rc); gc()
  
  # - report
  print(paste("Status code not 200 for transgender female items..."))
  
}


# - fetch all P21: transgender male (Q2449503) + placeOfBirth
query <- 'SELECT ?item ?placeOfBirth WHERE { ?item wdt:P21 wd:Q2449503; wdt:P31 wd:Q5. OPTIONAL { ?item wdt:P19 ?placeOfBirth } }'

# Run Query:
print("Contacting WDQS for transgender male items...")
res <- GET(url = paste0(endPointURL, URLencode(query)))
print("Done.")
if (res$status_code == 200) {
  
  # JSON:
  rc <- fromJSON(rawToChar(res$content), simplifyDataFrame = T)
  
  # clear:
  rm(res); gc()
  
  # extract:
  # - to runtime Log:
  print(paste("Parsing now transgender male items..."))
  # - as.data.frame:
  items <- data.frame(item = rc$results$bindings$item$value,
                      gender = 'F',
                      placeOfBirth = rc$results$bindings$placeOfBirth$value,
                      stringsAsFactors = F)
  items$item <- str_extract(items$item, "Q[[:digit:]]+")
  items$placeOfBirth <- str_extract(items$placeOfBirth, "Q[[:digit:]]+")
  rm(rc); gc()
  
  # - store
  write.csv(items, 'transMItems.csv')
  
  ### -----------------------------------
  # - indicators
  ### -----------------------------------
  nTransMItems <- length(unique(items$item))
  
  # - clear
  rm(items); gc()
  
} else {
  
  # - clear
  rm(rc); gc()
  
  # - report
  print(paste("Status code not 200 for transgender male items..."))
  
}

# - toLog:
print(paste0("Step 1: Collect from WQDS... DONE.", as.character(Sys.time())))

### ---------------------------------------------------------------------------
### --- Step 2A: Collect M/F WDCM usage statistics
### ---------------------------------------------------------------------------

### --- 2A: M items

# - toLog:
print(paste0("Step 2: Collect M/F WDCM usage statistics...", as.character(Sys.time())))

# - load
items <- fread('maleItems.csv',
               header = T,
               check.names = F,
               stringsAsFactors = F)
items <- unique(items$item)

# - HiveQL

### --- cut into batches
# - cut into batches
batchNum <- round(length(items)/500000)
batchSize <- 500000
startBatchIx <- c(1:batchNum) * batchSize - batchSize + 1
stopBatchIx <- c(1:batchNum) * batchSize
stopBatchIx[batchNum] <- length(items)

for (batch in 1:batchNum) {
  
  # - to runtime Log:
  print(paste("------------- Processing batch: ", batch, " out of ", batchNum, sep = ""))
  
  # - output file name:
  fileName <- paste0('M_Items_TotalUsage_Batch', batch, ".tsv")
  
  # - WDCM usage statistics for the current M batch, HiveQL query:
  hiveQLquery <- paste0('USE goransm; 
                        SET hive.mapred.mode=unstrict; 
                        SELECT eu_entity_id, SUM(eu_count) AS eu_count FROM wdcm_maintable WHERE eu_entity_id IN (',
                        paste("\"", items[startBatchIx[batch]:stopBatchIx[batch]], "\"", collapse = ", ", sep = ""),
                        ') GROUP BY eu_entity_id;')
  # - save HiveQLQuery
  setwd(tempDataDir)
  write(hiveQLquery, 'hiveQLquery.hql')
  
  # - execute HiveQLQuery:
  hiveQLQueryCommand <- paste("/usr/local/bin/beeline --silent -f /home/goransm/RScripts/WDCM_R/WDCM_Bias_tempData/hiveQLquery.hql ",
                              paste0(
                                " > /home/goransm/RScripts/WDCM_R/WDCM_Bias_tempData/",
                                fileName),
                              sep = "")
  queryCom <- system(command = hiveQLQueryCommand, wait = TRUE)
  # - check for Success or Failure:
  if (queryCom != 0) {
    # - to runtime Log:
    print("--- (!!!) query failed: waiting for 1h before next attempt...")
    # - sleep for one hour
    Sys.sleep(time = 60*60)
    # - re-run query
    queryCom <- system(command = hiveQLQueryCommand, wait = TRUE)
    # - check errors:
    if (queryCom != 0) {
      # - to runtime Log:
      print("--- (!!!) query failed AGAIN: quiting.")
      quit()
    }
  }
  
}

print("Done.")
setwd(dataDir)

### --- 2B: F items

print("HiveQL queries for F items: ...")

# - load
items <- fread('femaleItems.csv',
               header = T,
               check.names = F,
               stringsAsFactors = F)
items <- unique(items$item)

# - HiveQL

### --- cut into batches
# - cut into batches
batchNum <- round(length(items)/100000)
batchSize <- 100000
startBatchIx <- c(1:batchNum) * batchSize - batchSize + 1
stopBatchIx <- c(1:batchNum) * batchSize
stopBatchIx[batchNum] <- length(items)

for (batch in 1:batchNum) {
  
  # - to runtime Log:
  print(paste("------------- Processing batch: ", batch, " out of ", batchNum, sep = ""))
  
  # - output file name:
  fileName <- paste0('F_Items_TotalUsage_Batch', batch, ".tsv")
  
  # - WDCM usage statistics for the current M batch, HiveQL query:
  hiveQLquery <- paste0('USE goransm; 
                        SET hive.mapred.mode=unstrict; 
                        SELECT eu_entity_id, SUM(eu_count) AS eu_count FROM wdcm_maintable WHERE eu_entity_id IN (',
                        paste("\"", items[startBatchIx[batch]:stopBatchIx[batch]], "\"", collapse = ", ", sep = ""),
                        ') GROUP BY eu_entity_id;')
  # - save HiveQLQuery
  setwd(tempDataDir)
  write(hiveQLquery, 'hiveQLquery.hql')
  
  # - execute HiveQLQuery:
  hiveQLQueryCommand <- paste("/usr/local/bin/beeline --silent -f /home/goransm/RScripts/WDCM_R/WDCM_Bias_tempData/hiveQLquery.hql ",
                              paste0(
                                " > /home/goransm/RScripts/WDCM_R/WDCM_Bias_tempData/",
                                fileName),
                              sep = "")
  queryCom <- system(command = hiveQLQueryCommand, wait = TRUE)
  # - check for Success or Failure:
  if (queryCom != 0) {
    # - to runtime Log:
    print("--- (!!!) query failed: waiting for 1h before next attempt...")
    # - sleep for one hour
    Sys.sleep(time = 60*60)
    # - re-run query
    queryCom <- system(command = hiveQLQueryCommand, wait = TRUE)
    # - check errors:
    if (queryCom != 0) {
      # - to runtime Log:
      print("--- (!!!) query failed AGAIN: quiting.")
      quit()
    }
  }
}
setwd(dataDir)

# - toLog:
print(paste0("Step 2: Collect M/F WDCM usage statistics... DONE.", as.character(Sys.time())))

### ---------------------------------------------------------------------------
### --- Step 2B: Collect from geo-coordinates of birthPlace for M/F
### ---------------------------------------------------------------------------

# - toLog:
print(paste0("Step 2B: Collect from geo-coordinates of birthPlace for M/F...", as.character(Sys.time())))

# - download pre-process geo-localizations
print('Downloading wdlabel.json - pre-process geo-localizations from https://tools.wmflabs.org/wikidata-analysis/... ')
# - determine the latest update on Wikidata geo-localized items from
# - https://tools.wmflabs.org/wikidata-analysis/
geoPage <- GET('https://tools.wmflabs.org/wikidata-analysis/')
geoPage <- rawToChar(geoPage$content)
geoPage <- htmlParse(geoPage)
tNode <- getNodeSet(geoPage,'//table')
geoPage <- readHTMLTable(tNode[[1]], stringsAsFactors = FALSE)
geoPage <- geoPage[-c(1, dim(geoPage)[1]), ]
geoPage$`Last Modified` <- sapply(geoPage$`Last Modified`, 
                                  function(x) {
                                    d <- strsplit(x, split = " ", fixed = T)
                                    d[[1]][1] <- strsplit(d[[1]][1], split = "-", fixed = T)
                                    d[[1]][[1]][2] <- which(month.abb %in% d[[1]][[1]][2])
                                    if (nchar(d[[1]][[1]][2]) == 1) {
                                      d[[1]][[1]][2] <- paste0("0", d[[1]][[1]][2])
                                    }
                                    d[[1]][1] <- paste(unlist(d[[1]][1]), collapse = "-", sep = "")
                                    d <- paste(unlist(d), collapse = " ")
                                  })
geoPage$`Last Modified` <- as.POSIXct(geoPage$`Last Modified`, tz = "UTC")
geoDir <- geoPage$Name[which.max(geoPage$`Last Modified`)]
geoDownPath <- paste0('https://tools.wmflabs.org/wikidata-analysis/', geoDir, "wdlabel.json")
# - download WDTK geo-localized items:
download.file(geoDownPath,
              destfile = paste0(dataDir, '/wdlabel.json'),
              quiet = T)
print('Done.')

### --- 2A: M items
# - open json
print('Load wdlabel.json...')
geoCoordinates <- fromJSON('wdlabel.json')
print('Done.')
gcItems <- names(geoCoordinates)

geoitems <- fread('maleItems.csv',
                  header = T,
                  check.names = F,
                  stringsAsFactors = F)
geoitemsSearch <- geoitems$placeOfBirth[which(grepl("^Q", geoitems$placeOfBirth))]
w <- which(gcItems %in% geoitemsSearch)
geoCoordinates <- geoCoordinates[w]
gcItems <- names(geoCoordinates)
rm(w); gc()
geoCoordinates <- lapply(geoCoordinates, function(x) {
  data.frame(x = x$x,
             y = x$y,
             stringsAsFactors = F)
})
geoCoordinates <- rbindlist(geoCoordinates)
geoCoordinates$item <- gcItems;
rm(gcItems); gc()
geoitems <- left_join(geoitems, geoCoordinates,
                      by = c('placeOfBirth' = 'item'))
geoitems$V1 <- NULL
write.csv(geoitems, 'maleItems.csv')
# - clear
rm(geoCoordinates); rm(geoitems); rm(geoitemsSearch); gc()

### --- 2B: F items
# - open json
print('Load wdlabel.json...')
geoCoordinates <- fromJSON('wdlabel.json')
print('Done.')
gcItems <- names(geoCoordinates)

geoitems <- fread('femaleItems.csv',
                  header = T,
                  check.names = F,
                  stringsAsFactors = F)
geoitemsSearch <- geoitems$placeOfBirth[which(grepl("^Q", geoitems$placeOfBirth))]
w <- which(gcItems %in% geoitemsSearch)
geoCoordinates <- geoCoordinates[w]
gcItems <- names(geoCoordinates)
rm(w); gc()
geoCoordinates <- lapply(geoCoordinates, function(x) {
  data.frame(x = x$x,
             y = x$y,
             stringsAsFactors = F)
})
geoCoordinates <- rbindlist(geoCoordinates)
geoCoordinates$item <- gcItems;
rm(gcItems); gc()
geoitems <- left_join(geoitems, geoCoordinates,
                      by = c('placeOfBirth' = 'item'))
geoitems$V1 <- NULL
write.csv(geoitems, 'femaleItems.csv')
# - clear
rm(geoCoordinates); rm(geoitems); rm(geoitemsSearch); gc()
# - remove wdlabel.json
file.remove('wdlabel.json')

# - toLog:
print(paste0("Step 2B: Collect from geo-coordinates of birthPlace for M/F... DONE ", as.character(Sys.time())))

### ---------------------------------------------------------------------------
### --- Step 3: Fetch WDCM usage statistics for M/F items w. placeOfBirth
### --- from goransm.wdcm_maintable Hive table
### ---------------------------------------------------------------------------

# - toLog:
print(paste0("Step 3: Fetch WDCM usage statistics for M/F items w. placeOfBirth from goransm.wdcm_maintable Hive table...", 
             as.character(Sys.time())))

### --- 3A: M items

print("HiveQL queries for M items: non-aggregated...")

# - load
items <- fread('maleItems.csv',
               header = T,
               check.names = F,
               stringsAsFactors = F)
items <- unique(items$item[which(!is.na(items$placeOfBirth))])

# - HiveQL

### --- cut into batches
# - cut into batches
batchNum <- round(length(items)/500000)
batchSize <- 500000
startBatchIx <- c(1:batchNum) * batchSize - batchSize + 1
stopBatchIx <- c(1:batchNum) * batchSize
stopBatchIx[batchNum] <- length(items)

for (batch in 1:batchNum) {
  
  # - to runtime Log:
  print(paste("------------- Processing batch: ", batch, " out of ", batchNum, sep = ""))
  
  # - output file name:
  fileName <- paste0('M_Items_Usage_Batch', batch, ".tsv")

  # - WDCM usage statistics for the current M batch, HiveQL query:
  hiveQLquery <- paste0('USE goransm; 
                        SET hive.mapred.mode=unstrict; 
                        SELECT eu_entity_id, SUM(eu_count) AS eu_count FROM wdcm_maintable WHERE eu_entity_id IN (',
                        paste("\"", items[startBatchIx[batch]:stopBatchIx[batch]], "\"", collapse = ", ", sep = ""),
                        ') GROUP BY eu_entity_id;')
  # - save HiveQLQuery
  setwd(tempDataDir)
  write(hiveQLquery, 'hiveQLquery.hql')
  
  # - execute HiveQLQuery:
  hiveQLQueryCommand <- paste("/usr/local/bin/beeline --silent -f /home/goransm/RScripts/WDCM_R/WDCM_Bias_tempData/hiveQLquery.hql ",
                              paste0(
                                " > /home/goransm/RScripts/WDCM_R/WDCM_Bias_tempData/",
                                fileName),
                              sep = "")
  queryCom <- system(command = hiveQLQueryCommand, wait = TRUE)
  # - check for Success or Failure:
  if (queryCom != 0) {
    # - to runtime Log:
    print("--- (!!!) query failed: waiting for 1h before next attempt...")
    # - sleep for one hour
    Sys.sleep(time = 60*60)
    # - re-run query
    queryCom <- system(command = hiveQLQueryCommand, wait = TRUE)
    # - check errors:
    if (queryCom != 0) {
      # - to runtime Log:
      print("--- (!!!) query failed AGAIN: quiting.")
      quit()
    }
  }
}

print("Done.")
setwd(dataDir)

### --- 3B: F items

print("HiveQL queries for F items: non-aggregated...")

# - load
items <- fread('femaleItems.csv',
               header = T,
               check.names = F,
               stringsAsFactors = F)
items <- unique(items$item[which(!is.na(items$placeOfBirth))])

# - HiveQL

### --- cut into batches
# - cut into batches
batchNum <- round(length(items)/100000)
batchSize <- 100000
startBatchIx <- c(1:batchNum) * batchSize - batchSize + 1
stopBatchIx <- c(1:batchNum) * batchSize
stopBatchIx[batchNum] <- length(items)

for (batch in 1:batchNum) {
  
  # - to runtime Log:
  print(paste("------------- Processing batch: ", batch, " out of ", batchNum, sep = ""))
  
  # - output file name:
  fileName <- paste0('F_Items_Usage_Batch', batch, ".tsv")
  
  # - WDCM usage statistics for the current M batch, HiveQL query:
  hiveQLquery <- paste0('USE goransm; 
                        SET hive.mapred.mode=unstrict; 
                        SELECT eu_entity_id, SUM(eu_count) AS eu_count FROM wdcm_maintable WHERE eu_entity_id IN (',
                        paste("\"", items[startBatchIx[batch]:stopBatchIx[batch]], "\"", collapse = ", ", sep = ""),
                        ') GROUP BY eu_entity_id;')
  # - save HiveQLQuery
  setwd(tempDataDir)
  write(hiveQLquery, 'hiveQLquery.hql')
  
  # - execute HiveQLQuery:
  hiveQLQueryCommand <- paste("/usr/local/bin/beeline --silent -f /home/goransm/RScripts/WDCM_R/WDCM_Bias_tempData/hiveQLquery.hql ",
                              paste0(
                                " > /home/goransm/RScripts/WDCM_R/WDCM_Bias_tempData/",
                                fileName),
                              sep = "")
  queryCom <- system(command = hiveQLQueryCommand, wait = TRUE)
  # - check for Success or Failure:
  if (queryCom != 0) {
    # - to runtime Log:
    print("--- (!!!) query failed: waiting for 1h before next attempt...")
    # - sleep for one hour
    Sys.sleep(time = 60*60)
    # - re-run query
    queryCom <- system(command = hiveQLQueryCommand, wait = TRUE)
    # - check errors:
    if (queryCom != 0) {
      # - to runtime Log:
      print("--- (!!!) query failed AGAIN: quiting.")
      quit()
    }
  }
}
print("Done.")
setwd(dataDir)

### --- 3C: join WDCM usage statistics w. geo-coordinates data sets

# - 3C.1 Male items

print("Join: M items w. WDCM usage statistics...")

# - load M items: geo-coordinates
items <- fread('maleItems.csv',
               header = T,
               check.names = F,
               stringsAsFactors = F)
items$V1 <- NULL

# - load M items: WDCM usage statistics
setwd(tempDataDir)
lF <- list.files()
lF <- lF[grepl("^M_Items_Usage_Batch", lF)]
mItemsUsage <- vector(mode = "list", length = length(lF))
for (i in 1:length(mItemsUsage)) {
  mItemsUsage[[i]] <- fread(lF[i],
                            header = T,
                            quote = "", 
                            sep = "\t")
  
}
mItemsUsage <- rbindlist(mItemsUsage)
items <- left_join(items, mItemsUsage, 
                   by = c('item' = 'eu_entity_id'))
rm(mItemsUsage); gc()
items <- items[complete.cases(items), ]
iUsage <- items %>% 
  select(placeOfBirth, eu_count) %>% 
  group_by(placeOfBirth) %>% 
  summarise(usage = sum(eu_count)) %>% 
  arrange(desc(usage))
iCoord <- items %>% 
  select(placeOfBirth, x, y)
iCoord <- iCoord[!duplicated(iCoord), ]
iUsage <- left_join(iUsage, iCoord, by = 'placeOfBirth')
iUsage$gender <- 'M'
setwd(dataDir)
write.csv(iUsage, "iUsageGeo_M.csv")
rm(items); rm(iCoord); rm(iUsage); gc()

# - 3C.1 Female items

print("Join: F items w. WDCM usage statistics...")
# - load M items: geo-coordinates
items <- fread('femaleItems.csv',
               header = T,
               check.names = F,
               stringsAsFactors = F)
items$V1 <- NULL
# - load M items: WDCM usage statistics
setwd(tempDataDir)
lF <- list.files()
lF <- lF[grepl("^F_Items_Usage_Batch", lF)]
mItemsUsage <- vector(mode = "list", length = length(lF))
for (i in 1:length(mItemsUsage)) {
  mItemsUsage[[i]] <- fread(lF[i],
                            header = T,
                            quote = "", 
                            sep = "\t")
  
}
mItemsUsage <- rbindlist(mItemsUsage)
items <- left_join(items, mItemsUsage, 
                   by = c('item' = 'eu_entity_id'))
rm(mItemsUsage); gc()
items <- items[complete.cases(items), ]
iUsage <- items %>% 
  select(placeOfBirth, eu_count) %>% 
  group_by(placeOfBirth) %>% 
  summarise(usage = sum(eu_count)) %>% 
  arrange(desc(usage))
iCoord <- items %>% 
  select(placeOfBirth, x, y)
iCoord <- iCoord[!duplicated(iCoord), ]
iUsage <- left_join(iUsage, iCoord, by = 'placeOfBirth')
iUsage$gender <- 'F'
setwd(dataDir)
write.csv(iUsage, "iUsageGeo_F.csv")
rm(items); rm(iCoord); rm(iUsage); gc()

# - join M and F geo-coordinates + usage data sets
fIts <- fread('iUsageGeo_F.csv',
              header = T)
mIts <- fread('iUsageGeo_M.csv',
              header = T)
geoItemsUsage <- rbind(fIts, mIts)
geoItemsUsage$V1 <- NULL
rm(fIts); rm(mIts); gc();
write.csv(geoItemsUsage, 'geoItemsUsage.csv')

# - toLog:
print(paste0("Step 3: Fetch WDCM usage statistics for M/F items w. placeOfBirth from goransm.wdcm_maintable Hive table... DONE", 
             as.character(Sys.time())))

### ---------------------------------------------------------------------------
### --- Step 4: Fetch WDCM usage statistics for M/F items per project
### --- from goransm.wdcm_maintable Hive table
### ---------------------------------------------------------------------------

# - toLog:
print(paste0("Step 4: Fetch WDCM usage statistics for M/F items per project from goransm.wdcm_maintable Hive table...", 
             as.character(Sys.time())))

# - 4A. Male items

print("Per-project: M items WDCM usage statistics...")

mItems <- fread('maleItems.csv', 
                header = T)
items <- unique(mItems$item)
rm(mItems); gc()

# - HiveQL
### --- cut into batches
# - cut into batches
batchNum <- round(length(items)/500000)
batchSize <- 500000
startBatchIx <- c(1:batchNum) * batchSize - batchSize + 1
stopBatchIx <- c(1:batchNum) * batchSize
stopBatchIx[batchNum] <- length(items)

for (batch in 1:batchNum) {
  
  # - to runtime Log:
  print(paste("------------- Processing batch: ", batch, " out of ", batchNum, sep = ""))
  
  # - output file name:
  fileName <- paste0('Per-project_M_Items_Usage_Batch', batch, ".tsv")
  
  # - WDCM usage statistics for the current M batch per project, HiveQL query:
  hiveQLquery <- paste0('USE goransm; 
                        SELECT SUM(eu_count) AS eu_count, eu_project FROM wdcm_maintable WHERE ((eu_entity_id IN (',
                        paste("\"", items[startBatchIx[batch]:stopBatchIx[batch]], "\"", collapse = ", ", sep = ""),
                        ')) AND category="Human") GROUP BY eu_project;')
  # - save HiveQLQuery
  setwd(tempDataDir)
  write(hiveQLquery, 'hiveQLquery.hql')
  
  # - execute HiveQLQuery:
  hiveQLQueryCommand <- paste("/usr/local/bin/beeline --silent -f /home/goransm/RScripts/WDCM_R/WDCM_Bias_tempData/hiveQLquery.hql ",
                              paste0(
                                " > /home/goransm/RScripts/WDCM_R/WDCM_Bias_tempData/",
                                fileName),
                              sep = "")
  queryCom <- system(command = hiveQLQueryCommand, wait = TRUE)
  # - check for Success or Failure:
  if (queryCom != 0) {
    # - to runtime Log:
    print("--- (!!!) query failed: waiting for 1h before next attempt...")
    # - sleep for one hour
    Sys.sleep(time = 60*60)
    # - re-run query
    queryCom <- system(command = hiveQLQueryCommand, wait = TRUE)
    # - check errors:
    if (queryCom != 0) {
      # - to runtime Log:
      print("--- (!!!) query failed AGAIN: quiting.")
      quit()
    }
  }
}
rm(items); gc()
print("Done.")
setwd(dataDir)

# - 4B. Female items

print("Per-project: F items WDCM usage statistics...")

mItems <- fread('femaleItems.csv', 
                header = T)
items <- unique(mItems$item)
rm(mItems); gc()

# - HiveQL

### --- cut into batches
# - cut into batches
batchNum <- round(length(items)/100000)
batchSize <- 100000
startBatchIx <- c(1:batchNum) * batchSize - batchSize + 1
stopBatchIx <- c(1:batchNum) * batchSize
stopBatchIx[batchNum] <- length(items)

for (batch in 1:batchNum) {
  
  # - to runtime Log:
  print(paste("------------- Processing batch: ", batch, " out of ", batchNum, sep = ""))
  
  # - output file name:
  fileName <- paste0('Per-project_F_Items_Usage_Batch', batch, ".tsv")
  
  # - WDCM usage statistics for the current M batch per project, HiveQL query:
  hiveQLquery <- paste0('USE goransm; 
                        SELECT SUM(eu_count) AS eu_count, eu_project FROM wdcm_maintable WHERE ((eu_entity_id IN (',
                        paste("\"", items[startBatchIx[batch]:stopBatchIx[batch]], "\"", collapse = ", ", sep = ""),
                        ')) AND category="Human") GROUP BY eu_project;')
  # - save HiveQLQuery
  setwd(tempDataDir)
  write(hiveQLquery, 'hiveQLquery.hql')
  
  # - execute HiveQLQuery:
  hiveQLQueryCommand <- paste("/usr/local/bin/beeline --silent -f /home/goransm/RScripts/WDCM_R/WDCM_Bias_tempData/hiveQLquery.hql ",
                              paste0(
                                " > /home/goransm/RScripts/WDCM_R/WDCM_Bias_tempData/",
                                fileName),
                              sep = "")
  queryCom <- system(command = hiveQLQueryCommand, wait = TRUE)
  # - check for Success or Failure:
  if (queryCom != 0) {
    # - to runtime Log:
    print("--- (!!!) query failed: waiting for 1h before next attempt...")
    # - sleep for one hour
    Sys.sleep(time = 60*60)
    # - re-run query
    queryCom <- system(command = hiveQLQueryCommand, wait = TRUE)
    # - check errors:
    if (queryCom != 0) {
      # - to runtime Log:
      print("--- (!!!) query failed AGAIN: quiting.")
      quit()
    }
  }
}
rm(items); gc()
print("Done.")
setwd(dataDir)

# - 4C. Combine per project usage datasets

# - 4C.1 load M items: WDCM usage statistics per project
setwd(tempDataDir)
lF <- list.files()
lF <- lF[grepl("Per-project", lF, fixed = T)]
lF <- lF[grepl("M_", lF)]
mItemsUsage <- vector(mode = "list", length = length(lF))
for (i in 1:length(mItemsUsage)) {
  mItemsUsage[[i]] <- fread(lF[i],
                            header = T,
                            quote = "", 
                            sep = "\t")
  
}
projects <- unique(unlist(lapply(mItemsUsage, function(x) {
  x$eu_project
})))
mItemsDataSet <- data.frame(project = projects, 
                            stringsAsFactors = F)
for (i in 1:length(mItemsUsage)) {
  dS <- data.frame(project = mItemsUsage[[i]]$eu_project,
                   usage = mItemsUsage[[i]]$eu_count,
                   stringsAsFactors = F)
  mItemsDataSet <- left_join(mItemsDataSet, dS,
                             by = 'project')
}
mItemsDataSet[is.na(mItemsDataSet)] <- 0
mItemsDataSet$usageM <- rowSums(mItemsDataSet[, 2:dim(mItemsDataSet)[2]], na.rm = T)
mItemsDataSet <- select(mItemsDataSet, project, usageM)

# - 4C.2 load F items: WDCM usage statistics per project
lF <- list.files()
lF <- lF[grepl("Per-project", lF, fixed = T)]
lF <- lF[grepl("F_", lF)]
fItemsUsage <- vector(mode = "list", length = length(lF))
for (i in 1:length(fItemsUsage)) {
  fItemsUsage[[i]] <- fread(lF[i],
                            header = T,
                            quote = "", 
                            sep = "\t")
  
}
projects <- unique(unlist(lapply(fItemsUsage, function(x) {
  x$eu_project
})))
fItemsDataSet <- data.frame(project = projects, 
                            stringsAsFactors = F)
for (i in 1:length(fItemsUsage)) {
  dS <- data.frame(project = fItemsUsage[[i]]$eu_project,
                   usage = fItemsUsage[[i]]$eu_count,
                   stringsAsFactors = F)
  fItemsDataSet <- left_join(fItemsDataSet, dS,
                             by = 'project')
}
fItemsDataSet[is.na(fItemsDataSet)] <- 0
fItemsDataSet$usageF <- rowSums(fItemsDataSet[, 2:dim(fItemsDataSet)[2]], na.rm = T)
fItemsDataSet <- select(fItemsDataSet, project, usageF)

# - combine M and F data sets
genderProjectDataSet <- left_join(mItemsDataSet, fItemsDataSet, 
                                   by = 'project')
genderProjectDataSet$usageM[is.na(genderProjectDataSet$usageM)] <- 0
genderProjectDataSet$usageF[is.na(genderProjectDataSet$usageF)] <- 0

# - add proportions and percents
genderProjectDataSet$propM <- genderProjectDataSet$usageM/(genderProjectDataSet$usageM + genderProjectDataSet$usageF)
genderProjectDataSet$propF <- genderProjectDataSet$usageF/(genderProjectDataSet$usageM + genderProjectDataSet$usageF)
genderProjectDataSet$percentM <- round(genderProjectDataSet$usageM/(genderProjectDataSet$usageM + genderProjectDataSet$usageF)*100, 2)
genderProjectDataSet$percentF <- round(genderProjectDataSet$usageF/(genderProjectDataSet$usageM + genderProjectDataSet$usageF)*100, 2)
genderProjectDataSet$projectType <- projectType(genderProjectDataSet$project)

# - store gender x projects data set
setwd(dataDir)
write.csv(genderProjectDataSet, 'genderProjectDataSet.csv')

# - toLog:
print(paste0("Step 4: Fetch WDCM usage statistics for M/F items per project from goransm.wdcm_maintable Hive table... DONE", 
             as.character(Sys.time())))

### ---------------------------------------------------------------------------
### --- Step 5: M and F Occupations
### ---------------------------------------------------------------------------

# - toLog:
print(paste0("Step 5: M and F Occupations...", 
             as.character(Sys.time())))

### --- 5.A Male occupations

print("SPARQL for Male occupations:")
query <- 'SELECT ?occupation ?count
WHERE {
SELECT ?occupation (COUNT(?item) AS ?count)
WHERE {
?item wdt:P31 wd:Q5 .
?item wdt:P21 wd:Q6581097 .
?item wdt:P106 ?occupation .
} GROUP BY ?occupation 
} ORDER BY DESC(?count)'

res <- GET(url = paste0(endPointURL, URLencode(query)))
rc <- fromJSON(rawToChar(res$content), simplifyDataFrame = T)
rm(res); gc()
# - as.data.frame:
items <- data.frame(occupation = rc$results$bindings$occupation$value,
                    gender = 'M',
                    count = rc$results$bindings$count$value,
                    stringsAsFactors = F)
items$occupation <- str_extract(items$occupation, "Q[[:digit:]]+")
w <- which(is.na(items$occupation))
items <- items[-w, ]
print("DONE.")

print("SQL:")
# - fetch labels from wb_terms
itemList <- items$occupation
itemList <- paste0("'", itemList, "'", collapse = ", ", sep = "")
q <- paste("USE wikidatawiki; SELECT term_full_entity_id, term_text FROM wb_terms ",
           "WHERE ((term_full_entity_id IN (", itemList, ")) AND ",
           "(term_language = 'en') AND (term_type = 'label'));", 
           sep = "");
sqlCommand <- paste0(
  'mysql --defaults-file=/etc/mysql/conf.d/analytics-research-client.cnf -h analytics-store.eqiad.wmnet -A -e ',
  '"', q, '" > /home/goransm/RScripts/WDCM_R/WDCM_Bias_tempData/maleOccupationWD.tsv')
sqlCom <- system(command = sqlCommand, wait = TRUE)
setwd(tempDataDir)
labelSet <- read.delim('maleOccupationWD.tsv', 
                       sep = "\t", 
                       quote = "", 
                       header = T, 
                       stringsAsFactors = F)
# - join labels
items$occupationLabel <- unlist(lapply(items$occupation, function(x) {
  w <- which(labelSet$term_full_entity_id %in% x)
  if (length(w) == 1) {
    labelSet$term_text[which(labelSet$term_full_entity_id %in% x)]
  } else {NA}
}))
setwd(dataDir)
write.csv(items, "maleOccupationWD.csv")
rm(items); gc(); rm(labelSet); gc()
print("DONE.")

### --- 5.B Female occupations
print("SPARQL for Female occupations:")
query <- 'SELECT ?occupation ?count
WHERE {
SELECT ?occupation (COUNT(?item) AS ?count)
WHERE {
?item wdt:P31 wd:Q5 .
?item wdt:P21 wd:Q6581072 .
?item wdt:P106 ?occupation .
} GROUP BY ?occupation 
} ORDER BY DESC(?count)'

res <- GET(url = paste0(endPointURL, URLencode(query)))
rc <- fromJSON(rawToChar(res$content), simplifyDataFrame = T)
rm(res); gc()
# - as.data.frame:
items <- data.frame(occupation = rc$results$bindings$occupation$value,
                    gender = 'F',
                    count = rc$results$bindings$count$value,
                    stringsAsFactors = F)
items$occupation <- str_extract(items$occupation, "Q[[:digit:]]+")
w <- which(is.na(items$occupation))
items <- items[-w, ]
print("DONE.")

# - fetch labels from wb_terms
print("SQL:")
itemList <- items$occupation
itemList <- paste0("'", itemList, "'", collapse = ", ", sep = "")
q <- paste("USE wikidatawiki; SELECT term_full_entity_id, term_text FROM wb_terms ",
           "WHERE ((term_full_entity_id IN (", itemList, ")) AND ",
           "(term_language = 'en') AND (term_type = 'label'));", 
           sep = "");
sqlCommand <- paste0(
  'mysql --defaults-file=/etc/mysql/conf.d/analytics-research-client.cnf -h analytics-store.eqiad.wmnet -A -e ',
  '"', q, '" > /home/goransm/RScripts/WDCM_R/WDCM_Bias_tempData/femaleOccupationWD.tsv')
sqlCom <- system(command = sqlCommand, wait = TRUE)
setwd(tempDataDir)
labelSet <- read.delim('femaleOccupationWD.tsv', 
                       sep = "\t", 
                       quote = "", 
                       header = T, 
                       stringsAsFactors = F)
# - join labels
items$occupationLabel <- unlist(lapply(items$occupation, function(x) {
  w <- which(labelSet$term_full_entity_id %in% x)
  if (length(w) == 1) {
    labelSet$term_text[which(labelSet$term_full_entity_id %in% x)]
  } else {NA}
}))
setwd(dataDir)
write.csv(items, "femaleOccupationWD.csv")
rm(items); gc(); rm(labelSet); gc()
print("DONE.")

### --- 5.C fetch all M items and their occupations
print("SPARQL: all M items and their occupations.")
# - fetch all P21: male (Q6581097) + occupation
query <- 'SELECT ?item ?occupation WHERE { ?item wdt:P21 wd:Q6581097; wdt:P31 wd:Q5. OPTIONAL { ?item wdt:P106 ?occupation } }'
cQ <- 0
repeat {
  
  cQ <- cQ + 1
  
  if (cQ > 10) {
    print("CRITICAL: more 10 attempts at WDQS failed - QUITING REGULAR UPDATE NOW.")
    quit()
  } else {
    print(paste0("NOTE: Massive intake from WDQS; attempt num. ", cQ, "."))
    rQ <- tryCatch(
      {
        res <- GET(url = paste0(endPointURL, URLencode(query)))
        rc <- fromJSON(rawToChar(res$content), simplifyDataFrame = T)
        rm(res); gc()
        list(error = FALSE, wdqs = rc)
        },
      error = function(cond) {
        message(cond)
        return(list(error = TRUE, wdqs = NULL))
      },
      warning = function(cond) {
        message(cond)
        return(list(error = TRUE, wdqs = NULL))
      }
      )
    # - check result and report:
    if (rQ$error == FALSE) {
      print(paste0("rQ$error:", rQ$error, " - Success."))
      break
    } else {
      print(paste0("rQ$error:", rQ$error, " - Failure."))
      print("Clean up...")
      rm(res); rm(rc); gc(); 
      print("Pausing for 1 minute.")
      Sys.sleep(60)
    }
  }
}

# - as.data.frame:
items <- data.frame(item = rQ$wdqs$results$bindings$item$value,
                    gender = 'M',
                    occupation = rQ$wdqs$results$bindings$occupation$value,
                    stringsAsFactors = F)
rm(rQ); gc()
items$item <- str_extract(items$item, "Q[[:digit:]]+")
items$occupation <- str_extract(items$occupation, "Q[[:digit:]]+")
write.csv(items, "maleOccupations_IndividualItems.csv")
rm(items); gc()
print("DONE.")

### --- 5.C fetch all F items and their occupations
print("SPARQL: all F items and their occupations.")
# - fetch all P21: female (Q6581072) + occupation
query <- 'SELECT ?item ?occupation WHERE { ?item wdt:P21 wd:Q6581072; wdt:P31 wd:Q5. OPTIONAL { ?item wdt:P106 ?occupation } }'
cQ <- 0
repeat {
  
  cQ <- cQ + 1
  
  if (cQ > 10) {
    print("CRITICAL: more 10 attempts at WDQS failed - QUITING REGULAR UPDATE NOW.")
    quit()
  } else {
    print(paste0("NOTE: Massive intake from WDQS; attempt num. ", cQ, "."))
    rQ <- tryCatch(
      {
        res <- GET(url = paste0(endPointURL, URLencode(query)))
        rc <- fromJSON(rawToChar(res$content), simplifyDataFrame = T)
        rm(res); gc()
        list(error = FALSE, wdqs = rc)
      },
      error = function(cond) {
        message(cond)
        return(list(error = TRUE, wdqs = NULL))
      },
      warning = function(cond) {
        message(cond)
        return(list(error = TRUE, wdqs = NULL))
      }
    )
    # - check result and report:
    if (rQ$error == FALSE) {
      print(paste0("rQ$error:", rQ$error, " - Success."))
      break
    } else {
      print(paste0("rQ$error:", rQ$error, " - Failure."))
      print("Clean up...")
      rm(res); rm(rc); gc(); 
      print("Pausing for 1 minute.")
      Sys.sleep(60)
    }
  }
}

# - as.data.frame:
items <- data.frame(item = rQ$wdqs$results$bindings$item$value,
                    gender = 'F',
                    occupation = rQ$wdqs$results$bindings$occupation$value,
                    stringsAsFactors = F)
rm(rQ); gc()
items$item <- str_extract(items$item, "Q[[:digit:]]+")
items$occupation <- str_extract(items$occupation, "Q[[:digit:]]+")
write.csv(items, "femaleOccupations_IndividualItems.csv")
rm(items); gc()
print("DONE.")

### --- Usage per occupation: M items
print("Wrangling...")
# - load M items: WDCM usage statistics
setwd(tempDataDir)
lF <- list.files()
lF <- lF[grepl("TotalUsage", lF, fixed = T)]
lF <- lF[grepl("^M_", lF)]
mGenItems <- vector(mode = "list", length = length(lF))
for (i in 1:length(mGenItems)) {
  mGenItems[[i]] <- fread(lF[i],
                          header = T,
                          quote = "",
                          sep = "\t")

}
mGenItems <- rbindlist(mGenItems)
mGenItems$gender <- 'M'
# - load M items: occupations
setwd(dataDir)
mOccItems <- fread('maleOccupations_IndividualItems.csv', header = T)
mOccItems$V1 <- NULL
# - join
mOccItems <- left_join(mOccItems, mGenItems, by = c('item' = 'eu_entity_id'))
rm(mGenItems); gc()
mOccItems <- mOccItems %>%
  select(occupation, eu_count) %>%
  group_by(occupation) %>%
  summarise(usage = sum(eu_count, na.rm = T))
mOccItems$gender <- 'M'
write.csv(mOccItems, 'M_Occupation_usage.csv')
rm(mOccItems); gc()

### --- Usage per occupation: F items
# - load F items: WDCM usage statistics
setwd(tempDataDir)
lF <- list.files()
lF <- lF[grepl("TotalUsage", lF, fixed = T)]
lF <- lF[grepl("^F_", lF)]
fGenItems <- vector(mode = "list", length = length(lF))
for (i in 1:length(fGenItems)) {
  fGenItems[[i]] <- fread(lF[i],
                          header = T,
                          quote = "",
                          sep = "\t")

}
fGenItems <- rbindlist(fGenItems)
fGenItems$gender <- 'F'
# - load F items: occupations
setwd(dataDir)
fOccItems <- fread('femaleOccupations_IndividualItems.csv', header = T)
fOccItems$V1 <- NULL
# - join
fOccItems <- left_join(fOccItems, fGenItems, by = c('item' = 'eu_entity_id'))
rm(fGenItems); gc()
fOccItems <- fOccItems %>%
  select(occupation, eu_count) %>%
  group_by(occupation) %>%
  summarise(usage = sum(eu_count, na.rm = T))
fOccItems$gender <- 'F'
write.csv(fOccItems, 'F_Occupation_usage.csv')
rm(fOccItems); gc()
print("DONE.")

# - toLog:
print(paste0("Step 5: M and F Occupations... DONE.",
             as.character(Sys.time())))

### ---------------------------------------------------------------------------
### --- Step 6: Analytics/Outputs
### ---------------------------------------------------------------------------

# - toLog:
print(paste0("### --- Step 6: Analytics/Outputs...",
             as.character(Sys.time())))

### --- 6.A For Gender vs Geography Tab

print("Gender and Geography Section...")

# - load geoLocalized data sets
geoItems <- fread('geoItemsUsage.csv', header = T)
geoItems$V1 <- NULL
colnames(geoItems)[3:4] <- c('Lat', 'Lon')

# - {ggplot2} for M items
filename <- 'M_Items_Distribution.png'
png(filename = filename,
    width = 800, height = 607, units = "px",
    bg = "white",
    res = 72,
    type = c("cairo-png")
)
pFrame <- geoItems[geoItems$gender == 'M', ]
ggplot(pFrame, aes(x = Lon,
                   y = Lat)) +
  geom_point(size = pFrame$usage/max(pFrame$usage)*5,
             alpha = log(pFrame$usage)/max(log(pFrame$usage)),
             color = "deepskyblue") +
  xlim(-180, 180) + ylim(-90, 90) +
  theme_bw() +
  theme(axis.text.x = element_blank()) +
  theme(axis.text.y = element_blank()) +
  theme(axis.title.x = element_blank()) +
  theme(axis.title.y = element_blank()) +
  theme(axis.ticks = element_blank()) +
  theme(panel.background = element_rect(color = "black", fill = "black")) +
  theme(panel.border = element_blank()) +
  theme(panel.grid = element_blank()) +
  theme(legend.title = element_text(size = 11)) +
  theme(legend.position = "bottom")
dev.off()

# - {ggplot2} for F items
filename <- 'F_Items_Distribution.png'
png(filename = filename,
    width = 800, height = 607, units = "px",
    bg = "white",
    res = 72,
    type = c("cairo-png")
)
pFrame <- geoItems[geoItems$gender == 'F', ]
ggplot(pFrame, aes(x = Lon,
                   y = Lat)) +
  geom_point(size = pFrame$usage/max(pFrame$usage)*5,
             alpha = log(pFrame$usage)/max(log(pFrame$usage)),
             color = "indianred1") +
  xlim(-180, 180) + ylim(-90, 90) +
  theme_bw() +
  theme(axis.text.x = element_blank()) +
  theme(axis.text.y = element_blank()) +
  theme(axis.title.x = element_blank()) +
  theme(axis.title.y = element_blank()) +
  theme(axis.ticks = element_blank()) +
  theme(panel.background = element_rect(color = "black", fill = "black")) +
  theme(panel.border = element_blank()) +
  theme(panel.grid = element_blank()) +
  theme(legend.title = element_text(size = 11)) +
  theme(legend.position = "bottom")
dev.off()

### -----------------------------------
# - indicators
### -----------------------------------
globalGenderProportion_M <- sum(geoItems$usage[geoItems$gender == 'M'])
globalGenderProportion_F <- sum(geoItems$usage[geoItems$gender == 'F'])
globalGenderProportion_N <- sum(geoItems$usage[geoItems$Lat > 0])
globalGenderProportion_S <- sum(geoItems$usage[geoItems$Lat < 0])
genderPropotion_M_N <- sum(geoItems$usage[geoItems$gender == 'M' & geoItems$Lat > 0])
genderPropotion_M_S <- sum(geoItems$usage[geoItems$gender == 'M' & geoItems$Lat < 0])
genderPropotion_F_N <- sum(geoItems$usage[geoItems$gender == 'F' & geoItems$Lat > 0])
genderPropotion_F_S <- sum(geoItems$usage[geoItems$gender == 'F' & geoItems$Lat < 0])

### --- 6.B For Gender by Project Tab

print("Gender and Project Section...")
genderProject <- fread('genderProjectDataSet.csv', header = T)
genderProject$V1 <- NULL
genderProject <- genderProject[, c(
  'project', 'usageM', 'usageF', 'propM', 'propF', 
  'percentM', 'percentF', 'projectType')]

# - Bayesian Binomial test w. Beta(1,1)
BBT <- apply(genderProject[, 2:3], 1, function(x) {
  if (x[1] == 0 | x[2] == 0) {
    data.frame(pMF = NA,
               CI5 = NA,
               CI95 = NA)
  } else
  {
    mData <- rep(0, x[1] + x[2])
    mOnes <- sample(1:length(mData), x[1])
    mData[mOnes] <- 1
    fData <- rep(0, x[1] + x[2])
    fOnes <- sample(1:length(fData), size = x[2])
    fData[fOnes] <- 1
    AB1 <- summary(bayesTest(mData, fData,
                             priors = c('alpha' = 1, 'beta' = 1),
                             distribution = 'bernoulli',
                             n_samples = 1e6))
    data.frame(pMF = unlist(AB1$probability),
               CI5 = AB1$interval$Probability[1],
               CI95 = AB1$interval$Probability[2])
  }
})
BBT <- rbindlist(BBT)
genderProject <- cbind(genderProject, BBT)
write.csv(genderProject, 'genderProjectDataSet.csv')

# - MF proportion per project table
mfPropProject <- genderProject %>%
  select(usageM, usageF, projectType) %>%
  group_by(projectType) %>%
  summarise(usageM = sum(usageM), usageF = sum(usageF))
# - add proportions and percents
mfPropProject$propM <- mfPropProject$usageM/(mfPropProject$usageM + mfPropProject$usageF)
mfPropProject$propF <- mfPropProject$usageF/(mfPropProject$usageM + mfPropProject$usageF)
mfPropProject$percentM <- round(mfPropProject$usageM/(mfPropProject$usageM + mfPropProject$usageF)*100, 2)
mfPropProject$percentF <- round(mfPropProject$usageF/(mfPropProject$usageM + mfPropProject$usageF)*100, 2)
# - Bayesian Binomial test w. Beta(1,1)
BBT <- apply(mfPropProject[, 2:3], 1, function(x) {
  if (x[1] == 0 | x[2] == 0) {
    data.frame(pMF = NA,
               CI5 = NA,
               CI95 = NA)
  } else
  {
    mData <- rep(0, x[1] + x[2])
    mOnes <- sample(1:length(mData), x[1])
    mData[mOnes] <- 1
    fData <- rep(0, x[1] + x[2])
    fOnes <- sample(1:length(fData), size = x[2])
    fData[fOnes] <- 1
    AB1 <- summary(bayesTest(mData, fData,
                             priors = c('alpha' = 1, 'beta' = 1),
                             distribution = 'bernoulli',
                             n_samples = 1e6))
    data.frame(pMF = unlist(AB1$probability),
               CI5 = AB1$interval$Probability[1],
               CI95 = AB1$interval$Probability[2])
  }
})
BBT <- rbindlist(BBT)
mfPropProject <- cbind(mfPropProject, BBT)
write.csv(mfPropProject, 'mfPropProject.csv')

# - global MF distribution

# - load F items: WDCM usage statistics
setwd(tempDataDir)
lF <- list.files()
lF <- lF[grepl("TotalUsage", lF, fixed = T)]
lF <- lF[grepl("^F_", lF)]
fGenItems <- vector(mode = "list", length = length(lF))
for (i in 1:length(fGenItems)) {
  fGenItems[[i]] <- fread(lF[i],
                         header = T,
                         quote = "",
                         sep = "\t")

}
fGenItems <- rbindlist(fGenItems)
fGenItems$gender <- 'F'
# - load M items: WDCM usage statistics
lF <- list.files()
lF <- lF[grepl("TotalUsage", lF, fixed = T)]
lF <- lF[grepl("^M_", lF)]
mGenItems <- vector(mode = "list", length = length(lF))
for (i in 1:length(mGenItems)) {
  mGenItems[[i]] <- fread(lF[i],
                          header = T,
                          quote = "",
                          sep = "\t")

}
mGenItems <- rbindlist(mGenItems)
mGenItems$gender <- 'M'

# - combine M and F data sets
fGenItems <- arrange(fGenItems, desc(eu_count))
fGenItems$rank <- 1:dim(fGenItems)[1]
mGenItems <- arrange(mGenItems, desc(eu_count))
mGenItems$rank <- 1:dim(mGenItems)[1]
genItems <- rbind(fGenItems, mGenItems)
rm(mGenItems); rm(fGenItems); gc()

### -----------------------
### --- indicators
### -----------------------
totalUsage_M <- sum(genItems$eu_count[genItems$gender == 'M'])
totalUsage_F <- sum(genItems$eu_count[genItems$gender == 'F'])

# - {ggplot2} M and F usage distributions
setwd(dataDir)
filename <- 'genderUsage_Distribution.png'
png(filename = filename,
    width = 800, height = 607, units = "px",
    bg = "white",
    res = 72,
    type = c("cairo-png")
)
ggplot(genItems, aes(x = rank,
                   y = log(eu_count),
                   fill = gender,
                   color = gender,
                   group = gender)) +
  geom_line(size = .25) +
  scale_color_manual(values = c('indianred1', 'deepskyblue')) +
  scale_x_continuous(labels = comma) +
  theme_minimal() +
  ylab("log(Wikidata Usage)") + xlab("Rank") +
  labs(y = "log(Wikidata Usage)",
       x = "Item Usage Rank",
       title = "Wikidata Usage: Items per Gender") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 90, size = 10, hjust = 1)) +
  theme(axis.text.y = element_text(size = 10, hjust = 1)) +
  theme(axis.title.x = element_text(size = 11)) +
  theme(axis.title.y = element_text(size = 11)) +
  theme(plot.title = element_text(size = 12))
dev.off()

# - {ggplot2} M and F usage distributions
filename <- 'genderUsage_Distribution_jitterp.png'
png(filename = filename,
    width = 800, height = 607, units = "px",
    bg = "white",
    res = 72,
    type = c("cairo-png")
)
ggplot(genItems, aes(x = gender,
                     y = log(eu_count),
                     fill = gender,
                     color = gender,
                     group = gender)) +
  geom_jitter(aes(alpha = eu_count), size = .25, width = .1) +
  scale_color_manual(values = c('indianred1', 'deepskyblue')) +
  scale_alpha(guide = 'none') +
  theme_minimal() +
  ylab("Wikidata Usage") + xlab("gender") +
  labs(y = "log(Wikidata Usage)",
       x = "Item Usage Rank",
       title = "Wikidata Usage: Items per Gender") +
  theme_minimal() +
  theme(axis.text.x = element_text(size = 10, hjust = 1)) +
  theme(axis.text.y = element_text(size = 10, hjust = 1)) +
  theme(axis.title.x = element_text(size = 11)) +
  theme(axis.title.y = element_text(size = 11)) +
  theme(plot.title = element_text(size = 12))
dev.off()

### --- 6.C For the Diversity Tab
print("Diveristy Section...")

### --- Gini coefficient and Lorentz curve for M and F items Wikidata usage

fItems <- genItems %>%
  filter(gender == 'F')
mItems <- genItems %>%
  filter(gender == 'M')

### --- Gini
giniF <- round(ineq(fItems$eu_count, type = "Gini"), 2)
giniM <- round(ineq(mItems$eu_count, type = "Gini"), 2)

### --- Lorentz

fLor <- Lc(fItems$eu_count)
mLor <- Lc(mItems$eu_count)
pFrame <- data.frame(p = c(fLor$p, mLor$p),
                     L = c(fLor$L, mLor$L),
                     gender = c(rep('F', length(fLor$p)), rep('M', length(mLor$p))))
mfSample <- sample(1:length(pFrame$gender), 10000)
pFrame <- pFrame[mfSample, ]
add01 <- data.frame(p = c(0,1, 0, 1),
                    L = c(0,1, 0, 1),
                    gender = c('F', 'F', 'M', 'M'))
pFrame <- rbind(pFrame, add01)
# - {ggplot2} Lorenz Curves M and F usage
filename <- 'Gender_LorenzCurves.png'
png(filename = filename,
    width = 800, height = 607, units = "px",
    bg = "white",
    res = 72,
    type = c("cairo-png")
)
ggplot(pFrame, aes(x = p, y = L, color = gender)) +
  geom_segment(x = 0, y = 0, xend = 1, yend = 1, size = .02, color = "black", linetype = "dotted") +
  geom_line(size = 1) +
  geom_segment(x = 0, y = 0, xend = 1, yend = 1, size = .1, color = "black", linetype = "dashed") +
  scale_color_manual(values = c("indianred1", "deepskyblue")) +
  ggtitle(paste0("Wikidata Usage Lorenz Curves\n",
                 "Gini(F) = ", giniF, ", Gini(M) = ", giniM)) +
  xlab("Proportion of Items") + ylab("Proportion of Wikidata Usage") +
  theme_minimal() +
  theme(axis.text.x = element_text(size = 12)) +
  theme(axis.text.y = element_text(size = 12)) +
  theme(axis.title.x = element_text(size = 12)) +
  theme(axis.title.y = element_text(size = 12)) +
  theme(legend.position = "right") +
  theme(legend.title = element_blank()) +
  theme(strip.background = element_blank()) +
  theme(strip.text = element_text(face = "bold")) +
  theme(plot.title = element_text(hjust = 0.5, size = 13))
dev.off()

### --- 6.D For the Occupation Tab

print("Occupation Section...")

### --- Occupation Analytics
# - load M occupations and usage
mOccUsage <- fread('M_Occupation_usage.csv',
                   header = T)
mOccUsage$V1 <- NULL
mOccLabels <- fread('maleOccupationWD.csv',
                    header = T)
mOccLabels <- select(mOccLabels, occupationLabel, occupation)
mOccUsage <- left_join(mOccUsage, mOccLabels, by = 'occupation')
rm(mOccLabels)
mOccUsage <- arrange(mOccUsage, desc(usage))
mOccUsage <- filter(mOccUsage, usage > 0, !is.na(occupation))
# - load F occupations and usage
fOccUsage <- fread('F_Occupation_usage.csv',
                   header = T)
fOccUsage$V1 <- NULL
fOccLabels <- fread('femaleOccupationWD.csv',
                    header = T)
fOccLabels <- select(fOccLabels, occupationLabel, occupation)
fOccUsage <- left_join(fOccUsage, fOccLabels, by = 'occupation')
rm(fOccLabels)
fOccUsage <- arrange(fOccUsage, desc(usage))
fOccUsage <- filter(fOccUsage, usage > 0, !is.na(occupation))
# - join
occUsage <- full_join(mOccUsage, fOccUsage, by = 'occupation')
occUsage <- select(occUsage,
                   occupation, usage.x, usage.y, occupationLabel.x)
colnames(occUsage) <- c('occupation', 'usageM', 'usageF', 'label')
occUsage$usageM[is.na(occUsage$usageM)] <- 0
occUsage$usageF[is.na(occUsage$usageF)] <- 0
occUsage$totalUsage <- occUsage$usageM + occUsage$usageF
occUsage <- arrange(occUsage, desc(totalUsage))
# - add num. of Wikidata items per occupation
mOccLabels <- fread('maleOccupationWD.csv',
                    header = T)
occUsage$numItems_M <- as.numeric(sapply(occUsage$occupation, function(x) {
  mOccLabels$count[which(mOccLabels$occupation %in% x)]
}))
fOccLabels <- fread('femaleOccupationWD.csv',
                    header = T)
occUsage$numItems_F <- as.numeric(sapply(occUsage$occupation, function(x) {
  fOccLabels$count[which(fOccLabels$occupation %in% x)]
}))
rm(mOccLabels); rm(fOccLabels); gc()
# - write occUsage
write.csv(occUsage, 'occUsage.csv')
rm(fOccUsage); rm(mOccUsage); gc()
# - Bayesian Binomial test w. Beta(1,1)
BBT <- apply(occUsage[, 2:3], 1, function(x) {
  if (x[1] == 0 | x[2] == 0) {
    data.frame(pMF = NA,
               CI5 = NA,
               CI95 = NA)
  } else
  {
    mData <- rep(0, x[1] + x[2])
    mOnes <- sample(1:length(mData), x[1])
    mData[mOnes] <- 1
    fData <- rep(0, x[1] + x[2])
    fOnes <- sample(1:length(fData), size = x[2])
    fData[fOnes] <- 1
    AB1 <- summary(bayesTest(mData, fData,
                             priors = c('alpha' = 1, 'beta' = 1),
                             distribution = 'bernoulli',
                             n_samples = 1e6))
    data.frame(pMF = unlist(AB1$probability),
               CI5 = AB1$interval$Probability[1],
               CI95 = AB1$interval$Probability[2])
  }
})
BBT <- rbindlist(BBT)
occUsage <- cbind(occUsage, BBT)
# - re-arrange occUsage
occUsage <- data.frame(occupation = occUsage$occupation,
                       label = occUsage$label,
                       usageM = occUsage$usageM,
                       usageF = occUsage$usageF,
                       totalUsage = occUsage$totalUsage,
                       numItems_M = occUsage$numItems_M,
                       numItems_F = occUsage$numItems_F,
                       pMF = occUsage$pMF,
                       CI5 = occUsage$CI5,
                       CI95 = occUsage$CI95,
                       stringsAsFactors = F)
write.csv(occUsage, 'occUsage.csv')

### -----------------------------------
### --- write global indicators --- ###
### -----------------------------------
globalIndicators <- data.frame(nMaleItems, nFemaleItems, nIntersexItems, nTransMItems, nTransFItems,
                               totalUsage_M, totalUsage_F, globalGenderProportion_M, globalGenderProportion_F,
                               globalGenderProportion_N, globalGenderProportion_S,
                               genderPropotion_M_N, genderPropotion_M_S,
                               genderPropotion_F_N, genderPropotion_F_S, giniM, giniF)
write.csv(globalIndicators, "globalIndicators.csv")

# - toLog:
print(paste0("Step 6: Analytics/Outputs... DONE.",
             as.character(Sys.time())))

### -----------------------------------
### --- write update --- ###
### -----------------------------------
# - toLog:
print(paste0("### --- write update --- ###",
             as.character(Sys.time())))
update <- data.frame(update = as.character(Sys.time()))
write.csv(update, 'update_WDCMBiases.csv')
# - toLog:
print(paste0("REGULAR UPDATE DONE: ",
             as.character(Sys.time())))


### --- Public data sets:

# - Data:

# - genderProjectDataSet.csv
# - globalIndicators.csv
# - mfPropProject.csv
# - occUsage.csv

# - Visuals:

# - genderUsage_Distribution.png
# - genderUsage_Distribution_jitterp.png
# - M_Items_Distribution.png
# - F_Items_Distribution.png
# - Gender_LorenzCurves.png
