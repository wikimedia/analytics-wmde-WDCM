#!/usr/bin/env Rscript

### ---------------------------------------------------------------------------
### --- WDCM EngineGeo, v. Beta 0.1
### --- Script: WDCM_EngineGeo.R, v. Beta 0.1
### --- Author: Goran S. Milovanovic, Data Analyst, WMDE
### --- Developed under the contract between Goran Milovanovic PR Data Kolektiv
### --- and WMDE.
### --- Contact: goran.milovanovic_ext@wikimedia.de
### ---------------------------------------------------------------------------
### --- DESCRIPTION:
### --- WDCM_Engine_Geo contacts the WDQS SPARQL end-point
### --- and fetches the item IDs for several Wikidata concepts
### --- that have geographical co-ordinates. 
### --- WDCM_Pre-Process.R
### --- The remainder of the script searches the Hive goransm.wdcm_maintable
### --- for usage data and prepares the export .tsv files
### --- that migrate to Labs (wikidataconcepts, currently) where they are
### --- additionaly processed and stored to MariaDB to support
### --- the WDCM Geo Dashboard.
### --- NOTE: the execution of this WDCM script is always dependent upon the
### --- previous WDCM_Sqoop_Clients.R run from stat1004 (currently).
### ---------------------------------------------------------------------------
### --- RUN FROM: /home/goransm/RScripts/WDCM_R
### --- nohup Rscript WDCM_EngineGeo_goransm.R &
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

### ---------------------------------------------------------------------------
### --- Step 1: Collect from WQDS
### ---------------------------------------------------------------------------

### --- Setup
# - contact:
library(httr)
library(XML)
# - wrangling:
library(dplyr)
library(stringr)
library(readr)
library(data.table)
library(tidyr)
library(jsonlite)

### --- Directories
# - fPath: where the scripts is run from?
fPath <- '/home/goransm/RScripts/WDCM_R'
# - form paths:
ontologyDir <- paste(fPath, '/WDCM_Ontology', sep = "")
logDir <- paste(fPath, '/WDCM_Logs', sep = "")
itemsDir <- paste(fPath, '/WDCM_CollectedGeoItems', sep = "")
# - stat1005 published-datasets, maps onto 
# - https://analytics.wikimedia.org/datasets/wdcm/
dataDir <- '/srv/published-datasets/wdcm'

# - to runtime Log:
print(paste("--- UPDATE RUN STARTED ON:", Sys.time(), sep = " "))

### --- Set proxy
Sys.setenv(
  http_proxy = "http://webproxy.eqiad.wmnet:8080",
  https_proxy = "http://webproxy.eqiad.wmnet:8080")

### --- Read WDCM_GeoItems
# - to runtime Log:
print("--- Reading Ontology.")
setwd(ontologyDir)
wdcmGeoItems <- read.csv("WDCM_GeoItems_Belgrade_12152017.csv",
                         header = T,
                         check.names = F,
                         stringsAsFactors = F)

### --- Select all instances accross all sub-classes of searchItems:
# - endPoint:
endPointURL <- "https://query.wikidata.org/bigdata/namespace/wdq/sparql?format=json&query="

# - set itemsDir:
setwd(itemsDir)

# - clear output dir:
lF <- list.files()
rmF <- file.remove(lF)

# - track uncompleted queries:
qErrors <- character()

# - startTime (WDCM Main Report)
startTime <- as.character(Sys.time())

for (i in 1:length(wdcmGeoItems$item)) {

  # - to runtime Log:
  print(paste("--- SPARQL category:", i, sep = " "))
  
  searchItems <- str_trim(
  strsplit(wdcmGeoItems$item[i],
           split = ",", fixed = T)[[1]],
  "both")
  
  # - Construct Query:
  query <- paste0(
    'PREFIX wd: <http://www.wikidata.org/entity/> ',
    'PREFIX wdt: <http://www.wikidata.org/prop/direct/> ',
    'SELECT ?item ?coordinate ?itemLabel WHERE {?item (wdt:P31|(wdt:P31/wdt:P279*)) wd:',
    searchItems, '. ',
    '?item wdt:P625 ?coordinate. ',
    'SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }', 
    ' }'
  )
  
  # Run Query:
  res <- GET(url = paste0(endPointURL, URLencode(query)))
  
  if (res$status_code == 200) {
    
    # - to runtime Log:
    print(paste("Parsing now SPARQL category:", i, sep = ""))
  
    # - JSON:
    rc <- rawToChar(res$content)
    # clear:
    rm(res); gc()
    # - fromJSON:
    rc <- fromJSON(rc, simplifyDataFrame = T)
    # - extract:
    item <- rc$results$bindings$item$value
    coordinate <- rc$results$bindings$coordinate$value
    label <- rc$results$bindings$itemLabel$value
    # - as.data.frame:
    items <- data.frame(item = item,
                        coordinate = coordinate, 
                        label = label,
                        stringsAsFactors = F)
    # - clear:
    rm(item); rm(coordinate); rm(label); rm(rc); gc()
    # - keep unique result set:
    w <- which(duplicated(items$item))
    if (length(w) > 0) {items <- items[-w, ]}
    # - clear possible NAs from coordinates
    w <- which(is.na(items$coordinate) | (items$coordinate == ""))
    if (length(w) > 0) {items <- items[-w, ]}
    # - fix items
    items$item <- gsub("http://www.wikidata.org/entity/", "", items$item, fixed = T)
    # - fix coordinates (lon, lat)
    items$coordinate <- gsub("Point(", "", items$coordinate, fixed = T)
    items$coordinate <- gsub(")", "", items$coordinate, fixed = T)
    lon <- str_extract(items$coordinate, "^.+\\s")
    lat <- str_extract(items$coordinate, "\\s.+$")
    items$coordinate <- NULL
    items$lon <- lon
    items$lat <- lat
    # clear:
    rm(lon); rm(lat); gc()
    
  } else {
    qErrors <- append(qErrors, searchItems)
  }
  
  # store as CSV
  write_csv(items, path = paste0(wdcmGeoItems$itemLabel[i],"_ItemIDs.csv"))
  
  # clear:
  rm(items); gc()

}

### --- log Collect_Items step:
# - to runtime Log:
print("--- LOG: Collect_Items step completed.")
# - set log dir:
setwd(logDir)
# - log uncompleted queries
write.csv(qErrors, "WDCM_Collect_GeoItems_SPARQL_Errors.csv")
# - write to WDCM main reporting file:
lF <- list.files()
if ('WDCM_GeoReport.csv' %in% lF) {
  mainReport <- read.csv('WDCM_GeoReport.csv',
                         header = T,
                         row.names = 1,
                         check.names = F,
                         stringsAsFactors = F)
  newReport <- data.frame(Step = 'CollectItems',
                          Time = as.character(Sys.time()),
                          stringsAsFactors = F)
  mainReport <- rbind(mainReport, newReport)
  write.csv(mainReport, 'WDCM_GeoReport.csv')
} else {
  newReport <- data.frame(Step = 'CollectItems',
                          Time = as.character(Sys.time()),
                          stringsAsFactors = F)
  write.csv(newReport, 'WDCM_GeoReport.csv')
}

### ---------------------------------------------------------------------------
### --- Step 2: ETL: Wikidata usage statistics from WDCM Maintable
### ---------------------------------------------------------------------------

# - to runtime Log:
print("--- ETL: Wikidata usage statistics from wdcm_maintable")

### --- read item categories:
setwd(itemsDir)
idFiles <- list.files()
idFiles <- idFiles[grepl(".csv$", idFiles)]
categories <- unname(sapply(idFiles, function(x) {
  strsplit(x, split = "_")[[1]][1]
}))

for (i in 1:length(categories)) {
  
  # - filename
  filename <- paste("wdcm_geoitem_",
                    gsub(" ", "", categories[i], fixed = T), ".tsv",
                    sep = "")
  
  # - geoitems
  geoitems <- read.csv(idFiles[i],
                       header = T,
                       check.names = F,
                       stringsAsFactors = F)
  searchitems <- geoitems$item
  
  # - to runtime Log:
  print(paste("--- processing: ", i, ": ", filename, sep = ""))
  
  # - to runtime Log:
  print("--- RUNNING HiveQL Query to search for geoitems.")
  hiveQLQuery_1 <- "USE goransm; SET hive.mapred.mode=unstrict;"
  hiveQLQuery_2 <- paste("SELECT eu_entity_id, SUM(eu_count) AS usage FROM wdcm_maintable WHERE eu_entity_id IN (",
                         paste("\"", searchitems, "\"", collapse = ", ", sep = ""),
                         ") GROUP BY eu_entity_id ORDER BY usage DESC LIMIT 10000;",
                         sep = "")
  hiveQLQuery <- paste(hiveQLQuery_1, hiveQLQuery_2, sep = " ")
  
  # - write hiveQLQuery locally:
  setwd(fPath)
  write(hiveQLQuery, "hiveQL_geoQuery.hql")
  
  # - execute HiveQLQuery:
  hiveQLQueryCommand <- paste("/usr/local/bin/beeline --silent -f ", 
                              getwd(), "/hiveQL_geoQuery.hql", 
                              " > ", dataDir, "/", filename, 
                              sep = "")
  # - [query01Err]
  # - to runtime Log:
  print("--- Running query [query01Err].")
  query01Err <- system(command = hiveQLQueryCommand, wait = TRUE)
  if (query01Err != 0) {
    # - to runtime Log:
    print("--- (!!!) query01Err failed: waiting for 1h before next attempt...")
    # - sleep for one hour
    Sys.sleep(time = 60*60)
    # - re-run query
    query01Err <- system(command = hiveQLQueryCommand, wait = TRUE)
    # - check errors:
    if (query01Err != 0) {
      # - to runtime Log:
      print("--- (!!!) query01Err failed AGAIN: quiting.")
      quit()
    }
  }
  
  # - back to itemsDir
  setwd(itemsDir)
  
}

### --- join coordinates, items, labels, and usage
setwd(dataDir)

# - list .tsv files from dataDir
lF <- list.files()
w <- which(grepl("^wdcm_geoitem", lF))
lF <- lF[w]
w <- which(grepl(".tsv", lF, fixed = T))
lF <- lF[w]

# - remove old .csv files from dataDir
rmF <- list.files()
w <- which(grepl("^wdcm_geoitem", rmF))
rmF <- rmF[w]
w <- which(grepl(".csv", rmF, fixed = T))
rmF <- rmF[w]
file.remove(rmF)

for (i in 1:length(lF)) {
  
  # - to runtime Log:
  print(paste("Joining dataset: ", lF[i], sep = ""))
  
  # - usage data
  usage <- readLines(lF[i])
  if (length(which(grepl("null", usage, fixed = T))) > 0) {
    usage <- usage[-which(grepl("null", usage, fixed = T))]
  }
  if (length(which(usage %in% "")) > 0) {
    usage <- usage[-which(usage %in% "")]
  }
  if (length(which(usage %in% " ")) > 0) {
    usage <- usage[-which(usage %in% " ")]
  }
  usageList <- lapply(usage, function(x) {
    udata <- strsplit(x, split = "\t", fixed = T)[[1]]
    udata <- data.frame(item = udata[1],
                        usage = udata[2],
                        stringsAsFactors = F)
  })
  usage <- rbindlist(usageList)
  # - remove previous headers
  usage <- usage[-1, ]
  rm(usageList)
  # - coordinates data and labels
  setwd(itemsDir)
  rF <- list.files()
  # - find coordinates data and labels file
  catUsage <- strsplit(
    strsplit(lF[i], split = ".", fixed = T)[[1]][1],
    split = "_",
    fixed = T)[[1]][3]
  rFcheck <- sapply(rF, function(x) {
    gsub(" ", "", strsplit(x, split = "_", fixed = T)[[1]][1], fixed = T)
  })
  w <- sapply(rFcheck, function(x) {grepl(catUsage, x)})
  geo <- read.csv(rF[w], 
                  header = T,
                  check.names = F,
                  stringsAsFactors = F)
  # - join
  usage <- left_join(usage, geo, by = 'item')
  rm(geo)
  # - write: back to dataDir
  setwd(dataDir)
  file.remove(lF[i])
  write.csv(usage, file = gsub(".tsv", ".csv", 
                               gsub("_ItemIDs", "", paste("wdcm_geoitem_", rF[i], sep = ""), fixed = T)
                               ))
}

### --- log ETL step:
# - to runtime Log:
print("--- LOG: ETL from wdcm_maintable step completed.")
# - set log dir:
setwd(logDir)
# - write to WDCM main reporting file:
lF <- list.files()
if ('WDCM_GeoReport.csv' %in% lF) {
  mainReport <- read.csv('WDCM_GeoReport.csv',
                         header = T,
                         row.names = 1,
                         check.names = F,
                         stringsAsFactors = F)
  newReport <- data.frame(Step = 'ETL_wdcm_maintable',
                          Time = as.character(Sys.time()),
                          stringsAsFactors = F)
  mainReport <- rbind(mainReport, newReport)
  write.csv(mainReport, 'WDCM_GeoReport.csv')
} else {
  newReport <- data.frame(Step = 'CollectItems',
                          Time = as.character(Sys.time()),
                          stringsAsFactors = F)
  write.csv(newReport, 'WDCM_GeoReport.csv')
}

### ---------------------------------------------------------------------------
### --- Step 3: toLabsGeoReport
### ---------------------------------------------------------------------------

### --- toLabsGeoReport
toLabsGeoReport <- data.frame(timeStamp = as.character(Sys.time()),
                              statbox = "stat1005",
                              sqoopbox = "stat1004",
                              stringsAsFactors = F)
write.csv(toLabsGeoReport, "toLabsGeoReport.csv")

# - to runtime Log:
print(paste("--- UPDATE RUN COMPLETED ON:", Sys.time(), sep = " "))

### --- copy reports to /srv/published-datasets/wdcm:

# - WDCM_MainReport
system(command = 'cp /home/goransm/RScripts/WDCM_R/WDCM_Logs/WDCM_GeoReport.csv /srv/published-datasets/wdcm/', wait = T)
# - toLabsReport
system(command = 'cp /home/goransm/RScripts/WDCM_R/WDCM_Logs/toLabsGeoReport.csv /srv/published-datasets/wdcm/', wait = T)

