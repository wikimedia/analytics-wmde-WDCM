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
library(stringr)
library(readr)
library(data.table)
library(tidyr)
library(jsonlite)

### --- Directories
# - fPath: where the scripts is run from?
fPath <- '/home/goransm/Work/___DataKolektiv/Projects/WikimediaDEU/_WMDE_Projects/WDCM_Dev/WDCM/'
# - form paths:
ontologyDir <- paste(fPath, 'WDCM_Ontology', sep = "")
logDir <- paste(fPath, 'WDCM_Logs', sep = "")
itemsDir <- paste(fPath, 'WDCM_CollectedGeoItems', sep = "")
# - stat1005 published-datasets, maps onto 
# - https://analytics.wikimedia.org/datasets/wdcm/
dataDir <- '/srv/published-datasets/wdcm'

# - to runtime Log:
print(paste("--- UPDATE RUN STARTED ON:", Sys.time(), sep = " "))

### --- Set proxy
# Sys.setenv(
#   http_proxy = "http://webproxy.eqiad.wmnet:8080",
#   https_proxy = "http://webproxy.eqiad.wmnet:8080")

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
  
  itemsOut <- list()
  
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
    items <- items[-w, ]
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
if ('WDCM_MainReport.csv' %in% lF) {
  mainReport <- read.csv('WDCM_MainReport.csv',
                         header = T,
                         row.names = 1,
                         check.names = F,
                         stringsAsFactors = F)
  newReport <- data.frame(Step = 'CollectItems',
                          Time = as.character(Sys.time()),
                          stringsAsFactors = F)
  mainReport <- rbind(mainReport, newReport)
  write.csv(mainReport, 'WDCM_MainReport.csv')
} else {
  newReport <- data.frame(Step = 'CollectItems',
                          Time = as.character(Sys.time()),
                          stringsAsFactors = F)
  write.csv(newReport, 'WDCM_MainReport.csv')
}

### ---------------------------------------------------------------------------
### --- Script 2: WDCM_Search_Clients.R, WDCM Search Module
### ---------------------------------------------------------------------------

### ---------------------------------------------------------------------------
### --- WDCM Search Module, v. Beta 0.1
### --- Script: WDCM_Search_Clients.R, v. Beta 0.1
### --- Author: Goran S. Milovanovic, Data Analyst, WMDE
### --- Developed under the contract between Goran Milovanovic PR Data Kolektiv
### --- and WMDE.
### --- Contact: goran.milovanovic_ext@wikimedia.de
### ---------------------------------------------------------------------------
### --- DESCRIPTION:
### --- WDCM_Search_Clients.R takes a list of
### --- item IDs from Wikidata (the list is delivered by
### --- the WDCM_Collect_Items.R script) and searches for their
### --- usage across the Wikimedia projects in Hadoop:
### --- database: goransm
### --- directory: wdcmsqoop
### --- table: wdcm_clients_wb_entity_usage
### --- from production (currently: stat1005.eqiad.wmnet).
### --- NOTE: wdcm_clients_wb_entity_usage is produced by
### --- WDCM_Sqoop_Clients.R (currently run from: stat1004.eqiad.wmnet)
### ---------------------------------------------------------------------------
### --- INPUT:
### --- the WDCM_Search_Clients_HiveQL.R reads the list of item IDs
### --- to search for from /WDCM_CollectedItems
### --- This folder contains the .csv files that specify the item IDs
### --- to search for; the files are produced by Scrpt 1: WDCM_Collect_Items.R
### ---------------------------------------------------------------------------
### --- OUTPUT:
### --- wdcm_maintable Hive table on hdfs, database: goransm
### ---------------------------------------------------------------------------

### --- read item categories:
setwd(itemsDir)
idFiles <- list.files()
idFiles <- idFiles[grepl(".csv$", idFiles)]
idFilesSize <- file.size(idFiles)/1024^2

### --- Track all categories under processing:
# - check for the existence of the wdcmSearchReport file
# - and delete the old file if it's found:
setwd(logDir)
lF <- list.files()
w <- which(grepl("^wdcmSearchReport", lF))
if (length(w) == 1) {
  file.remove(lF[w])
}
### --- generate wdcmSearchReport data.frame
wdcmSearchReport <- data.frame(category = idFiles,
                               fileSize = idFilesSize,
                               startTime = character(length(idFiles)),
                               endTime = character(length(idFiles)),
                               stringsAsFactors = F
)
wdcmSearchReport <- wdcmSearchReport[order(-wdcmSearchReport$fileSize), ]

### --- PREPARATION: delete goransm.wdcm_maintable if exists,
### --- delete all from EXTERNAL Hive table from /user/goransm/wdcmtables (hdfs path)
### --- make directory for EXTERNAL Hive table /user/goransm/wdcmtables (hdfs path)

### --- check if goransm.wdcm_maintable exists in Hadoop; if yes, drop it:
# - NOTE: drop wdcm_maintable == erase metastore data:
# - [query01Err]

# - to runtime Log:
print("Running query [query01Err].")

query01Err <- system(command = '/usr/local/bin/beeline --silent -e "USE goransm; DROP TABLE IF EXISTS wdcm_maintable;"', wait = T)
if (query01Err != 0) {
  # - to runtime Log:
  print("--- (!!!) query01Err failed: waiting for 1h before next attempt...")
  # - sleep for one hour
  Sys.sleep(time = 60*60)
  # - re-run query
  query01Err <- system(command = '/usr/local/bin/beeline --silent -e "USE goransm; DROP TABLE IF EXISTS wdcm_maintable;"', wait = T)
  # - check errors:
  if (query01Err != 0) {
    # - to runtime Log:
    print("--- (!!!) query01Err failed AGAIN: quiting.")
    quit()
  }
}

### --- delete files for EXTERNAL Hive table from /user/goransm/wdcmtables (hdfs path)
# - [query02Err]
# - to runtime Log:
print("--- Running query [query02Err].")
query02Err <- system(command = 'hdfs dfs -rm -r /user/goransm/wdcmtables', wait = T)
if (query02Err != 0) {
  # - to runtime Log:
  print("--- (!!!) query02Err failed: waiting for 1h before next attempt...")
  # - sleep for one hour
  Sys.sleep(time = 60*60)
  # - re-run query
  query02Err <- system(command = 'hdfs dfs -rm -r /user/goransm/wdcmtables', wait = T)
  # - check errors:
  if (query02Err != 0) {
    # - to runtime Log:
    print("--- (!!!) query02Err failed AGAIN: quiting.")
    quit()
  }
}

### --- make directory for EXTERNAL Hive table /user/goransm/wdcmtables (hdfs path)
# - [query03Err]
# - to runtime Log:
print("--- Running query [query03Err].")
query03Err <- system(command = 'hdfs dfs -mkdir /user/goransm/wdcmtables', wait = T)
if (query03Err != 0) {
  # - to runtime Log:
  print("--- (!!!) query03Err failed: waiting for 1h before next attempt...")
  # - sleep for one hour
  Sys.sleep(time = 60*60)
  # - re-run query
  query03Err <- system(command = 'hdfs dfs -mkdir /user/goransm/wdcmtables', wait = T)
  # - check errors:
  if (query03Err != 0) {
    # - to runtime Log:
    print("--- (!!!) query03Err failed AGAIN: quiting.")
    quit()
  }
}

#### --- loop over item categories:

# - back to itemsDir for the loop:
setwd(itemsDir)

# - to runtime Log:
print("--- LOOP: Producing wdcm_maintable from wdcm_clients_wb_entity_usage now.")

for (i in 1:length(wdcmSearchReport$category)) {

  # - start time for this category:
  wdcmSearchReport$startTime[i] <- as.character(Sys.time())

  ### --- read item IDs:
  wFile <- which(grepl(wdcmSearchReport$category[i], idFiles, fixed = T))
  qIDs <- read.csv(idFiles[wFile],
                   header = T,
                   check.names = F,
                   stringsAsFactors = F)
  qIDs <- qIDs$item
  qIDs <- qIDs[grepl("^Q[[:digit:]]+", qIDs)]

  ### --- cut into batches (if necessary)
  # - cut into batches (5MB max. batch size)
  batchNum <- ceiling(wdcmSearchReport$fileSize[i]/10)
  batchSize <- round(length(qIDs)/batchNum)
  startBatchIx <- c(1:batchNum) * batchSize - batchSize + 1
  stopBatchIx <- c(1:batchNum) * batchSize
  stopBatchIx[batchNum] <- length(qIDs)

  for (batch in 1:batchNum) {

    # - to runtime Log:
    print(paste("------------- Processing category: ", i, ": ", wdcmSearchReport$category[i], sep = ""))
    print(paste("------------- Processing batch: ", batch, " out of ", batchNum, sep = ""))
    
    # - create goransm.wdcm_maintable Hive table if this is the first entry:
    # - (create wdcm_maintable Hive Table on (hdfs path): /user/goransm/wdcmtables)

    if ((i == 1) & (batch == 1)) {

      hiveCommand <- "\"USE goransm; CREATE EXTERNAL TABLE \\\`goransm.wdcm_maintable\\\`(
      \\\`eu_entity_id\\\`        string      COMMENT '',
      \\\`eu_project\\\`           string      COMMENT '',
      \\\`eu_count\\\`          bigint      COMMENT ''
      )
      COMMENT
      ''
      PARTITIONED BY (
      \\\`category\\\` string COMMENT 'The item category')
      ROW FORMAT SERDE
      'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
      STORED AS INPUTFORMAT
      'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
      OUTPUTFORMAT
      'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
      LOCATION
      'hdfs://analytics-hadoop/user/goransm/wdcmtables';\""
      hiveCommand <- paste("/usr/local/bin/beeline --silent -e ", hiveCommand, sep = "")
      # - [query04AErr]
      # - to runtime Log:
      print("--- Running query [query04AErr].")
      query04AErr <- system(command = hiveCommand, wait = TRUE)
      if (query04AErr != 0) {
        # - to runtime Log:
        print("--- (!!!) query04AErr failed: waiting for 1h before next attempt...")
        # - sleep for one hour
        Sys.sleep(time = 60*60)
        # - re-run query
        query04AErr <- system(command = hiveCommand, wait = TRUE)
        # - check errors:
        if (query04AErr != 0) {
          # - to runtime Log:
          print("--- (!!!) query04AErr failed AGAIN: quiting.")
          quit()
        }
      }
    }

    # - construct HiveQL query to search for category i items
    # - across all wiki_db:

    # - to runtime Log:
    print("--- RUNNING HiveQL Query to search for category items.")
    hiveQLQuery_1 <- "USE goransm; SET hive.mapred.mode=unstrict;"
    hiveQLQuery_2 <- paste("INSERT INTO TABLE wdcm_maintable
                           PARTITION (category='",
                           strsplit(wdcmSearchReport$category[i], split = "_", fixed = T)[[1]][1],
                           "') ",
                           "SELECT eu_entity_id, wiki_db AS eu_project, COUNT(*) AS eu_count FROM (
                           SELECT DISTINCT eu_entity_id, eu_page_id, wiki_db FROM wdcm_clients_wb_entity_usage
                           WHERE eu_entity_id IN (",
                           paste("\"", qIDs[startBatchIx[batch]:stopBatchIx[batch]], "\"", collapse = ", ", sep = ""),
                           ")) AS t
                           GROUP BY eu_entity_id, wiki_db;",
                           sep = "")
    hiveQLQuery <- paste(hiveQLQuery_1, hiveQLQuery_2, sep = " ")
    # - write hiveQLQuery locally:
    setwd(fPath)
    write(hiveQLQuery, "hiveQLQuery.hql")
    # - execute HiveQLQuery:
    hiveQLQueryCommand <- paste("/usr/local/bin/beeline --silent -f ", getwd(), "/hiveQLQuery.hql", sep = "")
    # - [query04BErr]
    # - to runtime Log:
    print("--- Running query [query04BErr].")
    
    query04BErr <- system(command = hiveQLQueryCommand, wait = TRUE)
    if (query04BErr != 0) {
      # - to runtime Log:
      print("--- (!!!) query04BErr failed: waiting for 1h before next attempt...")
      # - sleep for one hour
      Sys.sleep(time = 60*60)
      # - re-run query
      query04BErr <- system(command = hiveQLQueryCommand, wait = TRUE)
      # - check errors:
      if (query04BErr != 0) {
        # - to runtime Log:
        print("--- (!!!) query04BErr failed AGAIN: quiting.")
        quit()
      }
    }

    # - to runtime Log:
    print("--- REPAIR TABLE.")

    # - repair partitions:
    # - query04CErr
    # - to runtime Log:
    print("Running query [query04CErr].")
    query04CErr <- system(command =
                            '/usr/local/bin/beeline --silent -e "USE goransm; SET hive.mapred.mode = nonstrict; MSCK REPAIR TABLE wdcm_maintable;"',
                          wait = TRUE)
    if (query04CErr != 0) {
      # - to runtime Log:
      print("--- (!!!) query04CErr failed: waiting for 1h before next attempt...")
      # - sleep for one hour
      Sys.sleep(time = 60*60)
      # - re-run query
      query04CErr <- system(command =
                              '/usr/local/bin/beeline --silent -e "USE goransm; SET hive.mapred.mode = nonstrict; MSCK REPAIR TABLE wdcm_maintable;"',
                            wait = TRUE)
      # - check errors:
      if (query04CErr != 0) {
        # - to runtime Log:
        print("--- (!!!) query04CErr failed AGAIN: quiting.")
        quit()
      }
    }

    # - end time for this category:
    wdcmSearchReport$endTime[i] <- as.character(Sys.time())

    # - back to item categories:
    setwd(itemsDir)

  }

}

### --- store report:

# - to runtime Log:
print("--- LOG Search Phase completed.")

# - to wdcmSearchReport:
setwd(logDir)
write.csv(wdcmSearchReport,
          paste("wdcmSearchReport_",
                strsplit(as.character(Sys.time()),
                         split = " ")[[1]][1],
                ".csv",
                sep = ""))

# - write to WDCM main reporting file:
mainReport <- read.csv('WDCM_MainReport.csv',
                       header = T,
                       row.names = 1,
                       check.names = F,
                       stringsAsFactors = F)
newReport <- data.frame(Step = 'SearchItems',
                        Time = as.character(Sys.time()),
                        stringsAsFactors = F)
mainReport <- rbind(mainReport, newReport)
write.csv(mainReport, 'WDCM_MainReport.csv')

### ---------------------------------------------------------------------------
### --- Script 3: WDCM_Pre-Process.R, WDCM Process Module
### ---------------------------------------------------------------------------

### ---------------------------------------------------------------------------
### --- WDCM Process Module, v. Beta 0.1
### --- Script: WDCM_Pre-Process.R, v. Beta 0.1
### --- Author: Goran S. Milovanovic, Data Analyst, WMDE
### --- Developed under the contract between Goran Milovanovic PR Data Kolektiv
### --- and WMDE.
### --- Contact: goran.milovanovic_ext@wikimedia.de
### ---------------------------------------------------------------------------
### --- DESCRIPTION:
### --- WDCM_Pre-Process.R works with the
### --- wdcm_maintable Hive table on hdfs, database: goransm
### --- to produce the .tsv files that migrate to
### --- the wikidataconcepts.wmflabs.org Cloud VPS instance
### --- from production (currently: stat1005.eqiad.wmnet).
### ---------------------------------------------------------------------------
### --- INPUT:
### --- wdcm_maintable Hive table on hdfs, database: goransm
### ---------------------------------------------------------------------------
### --- OUTPUT:
### --- Results are stored locally as .tsv files on production -
### --- - on stat1005.eqiad.wmnet - in:
### --- /WDCM_dataOut
### --- These output .tsv files migrate to Labs:
### --- wikidataconcepts.wmflabs.org Cloud VPS instance
### --- where they are then processed by the WDCM Process Module.
### ---------------------------------------------------------------------------

# - to runtime Log:
print("--- START: PRE-PROCESS")

### --- functions

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

### --- produce wdcm_item.tsv from wdcm_maintable (hdfs, database: goransm)
### --- NOTE: one .tsv file per category (~14M rows, causes Java gc overflow from hive...)

# - to runtime Log:
print("--- STEP: produce wdcm_item.tsv from wdcm_maintable")

### --- read item categories:
setwd(itemsDir)

idFiles <- list.files()
idFiles <- idFiles[grepl(".csv$", idFiles)]
categories <- unname(sapply(idFiles, function(x) {
  strsplit(x, split = "_")[[1]][1]
}))

for (i in 1:length(categories)) {

  filename <- paste("wdcm_item_",
                    gsub(" ", "", categories[i], fixed = T), ".tsv",
                    sep = "")

  # - to runtime Log:
  print("--- processing: ", filename, sep = "")
  
  hiveQLquery <- paste(
    'USE goransm; SELECT eu_entity_id, SUM(eu_count) AS eu_count FROM wdcm_maintable WHERE category=\\"',
    categories[i],
    '\\" GROUP BY eu_entity_id ORDER BY eu_count DESC LIMIT 100000;',
    sep = "")

  # - query05Err
  # - to runtime Log:
  print("--- Running query [query05Err].")
  query05Err <- system(command = paste('/usr/local/bin/beeline --silent -e "',
                                       hiveQLquery,
                                       '" > ', dataDir,
                                       "/", filename,
                                       sep = ""),
                       wait = TRUE)
  if (query05Err != 0) {
    # - to runtime Log:
    print("--- (!!!) query05Err failed: waiting for 1h before next attempt...")
    # - sleep for one hour
    Sys.sleep(time = 60*60)
    # - re-run query
    query05Err <- system(command = paste('/usr/local/bin/beeline --silent -e "',
                                         hiveQLquery,
                                         '" > ', dataDir,
                                         "/", filename,
                                         sep = ""),
                         wait = TRUE)
    # - check errors:
    if (query05Err != 0) {
      # - to runtime Log:
      print("--- (!!!) query05Err failed AGAIN: quiting.")
      quit()
    }
  }
  
}

### --- ETL Phase

# - to runtime Log:
print("--- STEP: ETL PHASE")

### --- to dataDir (and EVERYTHING ELSE goes to dataDir)
setwd(dataDir)

# - to runtime Log:
print("--- STEP: produce wdcm_project_category.tsv")

### --- produce wdcm_project_category.tsv from wdcm_maintable (hdfs, database: goransm)
hiveQLquery <- 'USE goransm;
                SET hive.mapred.mode=unstrict;
                SELECT eu_project, category, SUM(eu_count) AS eu_count
                FROM wdcm_maintable
                GROUP BY eu_project, category ORDER BY eu_count DESC LIMIT 10000000;'
# - [query06Err]
# - to runtime Log:
print("--- Running query [query06Err].")

query06Err <- system(command = paste('/usr/local/bin/beeline --silent -e "',
                                     hiveQLquery,
                                     '" > ', getwd(), '/wdcm_project_category.tsv',
                                     sep = ""),
                     wait = TRUE)
if (query06Err != 0) {
  # - to runtime Log:
  print("--- (!!!) query06Err failed: waiting for 1h before next attempt...")
  # - sleep for one hour
  Sys.sleep(time = 60*60)
  # - re-run query
  query06Err <- system(command = paste('/usr/local/bin/beeline --silent -e "',
                                       hiveQLquery,
                                       '" > ', getwd(), '/wdcm_project_category.tsv',
                                       sep = ""),
                       wait = TRUE)
  # - check errors:
  if (query06Err != 0) {
    # - to runtime Log:
    print("--- (!!!) query06Err failed AGAIN: quiting.")
    quit()
  }
}
# - add projecttype to wdcm_project_category.tsv
wdcm_project_category <- as.data.frame(fread('wdcm_project_category.tsv'))
wdcm_project_category$projectype <- projectType(wdcm_project_category$eu_project)
write.csv(wdcm_project_category, "wdcm_project_category.csv")

# - to runtime Log:
print("--- STEP: produce wdcm_project.tsv")

### --- produce wdcm_project.tsv from wdcm_maintable (hdfs, database: goransm)
hiveQLquery <- 'USE goransm;
                SET hive.mapred.mode=unstrict;
                SELECT eu_project, SUM(eu_count) AS eu_count
                FROM wdcm_maintable
                GROUP BY eu_project ORDER BY eu_count DESC LIMIT 10000000;'
# - [query07Err]
# - to runtime Log:
print("Running query [query07Err].")
query07Err <- system(command = paste('/usr/local/bin/beeline --silent -e "',
                                     hiveQLquery,
                                     '" > ', getwd(), '/wdcm_project.tsv',
                                     sep = ""),
                     wait = TRUE)
if (query07Err != 0) {
  # - to runtime Log:
  print("query07Err failed: waiting for 1h before next attempt...")
  # - sleep for one hour
  Sys.sleep(time = 60*60)
  # - re-run query
  query07Err <- system(command = paste('/usr/local/bin/beeline --silent -e "',
                                       hiveQLquery,
                                       '" > ', getwd(), '/wdcm_project.tsv',
                                       sep = ""),
                       wait = TRUE)
  # - check errors:
  if (query07Err != 0) {
    print("query07Err failed AGAIN: quiting.")
    quit()
  }
}

# - add projecttype to wdcm_project.tsv
wdcm_project <- as.data.frame(fread('wdcm_project.tsv'))
wdcm_project$projectype <- projectType(wdcm_project$eu_project)
write.csv(wdcm_project, "wdcm_project.csv")

# - to runtime Log:
print("STEP: wdcm_category.tsv")

### --- produce wdcm_category.tsv from wdcm_maintable (hdfs, database: goransm)
hiveQLquery <- 'USE goransm;
                SET hive.mapred.mode=unstrict;
                SELECT category, SUM(eu_count) AS eu_count
                FROM wdcm_maintable
                GROUP BY category ORDER BY eu_count DESC LIMIT 10000000;'
# - [query08Err]
# - to runtime Log:
print("Running query [query08Err].")
query08Err <- system(command = paste('/usr/local/bin/beeline --silent -e "',
                                     hiveQLquery,
                                     '" > ', getwd(), '/wdcm_category.tsv',
                                     sep = ""),
                     wait = TRUE)
if (query08Err != 0) {
  # - to runtime Log:
  print("query08Err failed: waiting for 1h before next attempt...")
  # - sleep for one hour
  Sys.sleep(time = 60*60)
  # - re-run query
  query08Err <- system(command = paste('/usr/local/bin/beeline --silent -e "',
                                       hiveQLquery,
                                       '" > ', getwd(), '/wdcm_category.tsv',
                                       sep = ""),
                       wait = TRUE)
  # - check errors:
  if (query08Err != 0) {
    # - to runtime Log:
    print("query08Err failed AGAIN: quiting.")
    quit()
  }
}
# - save wdcm_category.tsv as .csv
wdcm_category <- as.data.frame(fread('wdcm_category.tsv'))
write.csv(wdcm_category, "wdcm_category.csv")

# - to runtime Log:
print("STEP: produce wdcm_project_category_item100.tsv")

### --- produce wdcm_project_category_item100.tsv from wdcm_maintable (hdfs, database: goransm)
hiveQLquery <- 'USE goransm;
                SET hive.mapred.mode=unstrict;
                SELECT * FROM (
                  SELECT eu_project, category, eu_entity_id, eu_count, ROW_NUMBER() OVER (PARTITION BY eu_project, category ORDER BY eu_count DESC) AS row
                    FROM wdcm_maintable) t
                WHERE row <= 100;'
# - [query09Err]
# - to runtime Log:
print("Running query [query09Err].")
query09Err <- system(command = paste('/usr/local/bin/beeline --silent -e "',
                                     hiveQLquery,
                                     '" > ', getwd(), '/wdcm_project_category_item100.tsv',
                                     sep = ""),
                     wait = TRUE)
if (query09Err != 0) {
  # - to runtime Log:
  print("query09Err failed: waiting for 1h before next attempt...")
  # - sleep for one hour
  Sys.sleep(time = 60*60)
  # - re-run query
  query09Err <- system(command = paste('/usr/local/bin/beeline --silent -e "',
                                       hiveQLquery,
                                       '" > ', getwd(), '/wdcm_project_category_item100.tsv',
                                       sep = ""),
                       wait = TRUE)
  # - check errors:
  if (query09Err != 0) {
    # - to runtime Log:
    print("query09Err failed AGAIN: quiting.")
    quit()
  }
}

# - add projecttype to wdcm_project_category_item100.tsv
wdcm_project_category_item100 <- as.data.frame(fread('wdcm_project_category_item100.tsv'))
wdcm_project_category_item100$projectype <- projectType(wdcm_project_category_item100$t.eu_project)
write.csv(wdcm_project_category_item100, "wdcm_project_category_item100.csv")

# - to runtime Log:
print("STEP: produce wdcm_project_item100.tsv")

### --- produce wdcm_project_item100.tsv from wdcm_maintable (hdfs, database: goransm)
hiveQLquery <- 'USE goransm;
                SET hive.mapred.mode=unstrict;
                SELECT * FROM (
                  SELECT eu_project, eu_entity_id, eu_count, ROW_NUMBER() OVER (PARTITION BY eu_project ORDER BY eu_count DESC) AS row
                FROM wdcm_maintable) t
                WHERE row <= 100;'
# - [query10Err]
# - to runtime Log:
print("Running query [query10Err].")
query10Err <- system(command = paste('/usr/local/bin/beeline --silent -e "',
                                     hiveQLquery,
                                     '" > ', getwd(), '/wdcm_project_item100.tsv',
                                     sep = ""),
                     wait = TRUE)
if (query10Err != 0) {
  # - to runtime Log:
  print("query10Err failed: waiting for 1h before next attempt...")
  # - sleep for one hour
  Sys.sleep(time = 60*60)
  # - re-run query
  query10Err <- system(command = paste('/usr/local/bin/beeline --silent -e "',
                                       hiveQLquery,
                                       '" > ', getwd(), '/wdcm_project_item100.tsv',
                                       sep = ""),
                       wait = TRUE)
  # - check errors:
  if (query10Err != 0) {
    # - to runtime Log:
    print("query10Err failed AGAIN: quiting.")
    quit()
  }
}

# - add projecttype to wdcm_project_item100.tsv
wdcm_project_item100 <- as.data.frame(fread('wdcm_project_item100.tsv'))
wdcm_project_item100$projectype <- projectType(wdcm_project_item100$t.eu_project)
write.csv(wdcm_project_item100, "wdcm_project_item100.csv")


### --- Semantic Modeling Phase
### --- produce project-item matrices for semantic topic modeling

# - to runtime Log:
print("STEP: Semantic Modeling Phase: TDF MATRICES")
itemFiles <- list.files()
itemFiles <- itemFiles[grepl("^wdcm_item", itemFiles)]
for (i in 1:length(itemFiles)) {
  # - to runtime Log:
  print(paste("----------------------- TDF matrix formation: category ", i, ".", sep = ""))
  # - load categoryFile[i].tsv as data.frame
  categoryName <- strsplit(itemFiles[i], ".", fixed = T)[[1]][1]
  categoryName <- strsplit(categoryName, "_", fixed = T)[[1]][3]
  categoryName <- gsub("([[:lower:]])([[:upper:]])", "\\1 \\2", categoryName)
  # - load items
  # - NOTE: AN ARBITRARY DECISION TO MODEL TOP 5000 MOST FREQUENTLY USED ITEMS:
  # - nrows = 5000
  categoryFile <- fread(itemFiles[i], nrows = 5000)
  # - list of items to fetch
  itemList <- categoryFile$eu_entity_id
  # - hiveQL:
  hiveQLquery <- paste('USE goransm; SELECT eu_project, eu_entity_id, eu_count FROM wdcm_maintable WHERE eu_entity_id IN (',
                       paste0("'", itemList, "'", collapse = ", ", sep = ""),
                       ') AND category = \\"',
                       categoryName,
                       '\\";',
                       sep = "")
  fileName <- gsub(" ", "", categoryName, fixed = T)
  fileName <- paste("tfMatrix_", fileName, ".tsv", sep = "")
  # - [query11Err]
  # - to runtime Log:
  print("Running query [query11Err].")
  query11Err <- system(command = paste('/usr/local/bin/beeline --silent -e "',
                                       hiveQLquery,
                                       '" > ', getwd(), '/',
                                       fileName,
                                       sep = ""),
                       wait = TRUE)
  if (query11Err != 0) {
    # - to runtime Log:
    print("query11Err failed: waiting for 1h before next attempt...")
    # - sleep for one hour
    Sys.sleep(time = 60*60)
    # - re-run query
    query11Err <- system(command = paste('/usr/local/bin/beeline --silent -e "',
                                         hiveQLquery,
                                         '" > ', getwd(), '/',
                                         fileName,
                                         sep = ""),
                         wait = TRUE)
    # - check errors:
    if (query11Err != 0) {
      # - to runtime Log:
      print("query11Err failed AGAIN: quiting.")
      quit()
    }
  }
}

### --- reshape project-item matrices for semantic topic modeling

# - to runtime Log:
print("STEP: Semantic Modeling Phase: RESHAPING TDF MATRICES")
itemFiles <- list.files()
itemFiles <- itemFiles[grepl("^tfMatrix_", itemFiles)]
itemFiles <- itemFiles[grepl(".tsv", itemFiles, fixed = T)]
for (i in 1:length(itemFiles)) {
  # - to runtime Log:
  print(paste("----------------------- Reshaping TDF matrix: category ", i, ".", sep = ""))
  # - load categoryFile[i].tsv as data.frame
  categoryFile <- fread(itemFiles[i])
  categoryFile <- spread(categoryFile,
                         key = eu_entity_id,
                         value = eu_count,
                         fill = 0)
  rownames(categoryFile) <- categoryFile$eu_project
  categoryFile$eu_project <- NULL
  w <- which(colSums(categoryFile) == 0)
  if (length(w) > 0) {
    categoryFile <- categoryFile[, -w]
  }
  w <- which(rowSums(categoryFile) == 0)
  if (length(w) > 0) {
    categoryFile <- categoryFile[-w, ]
  }
  fileName <- paste(strsplit(itemFiles[i], split = ".", fixed = T)[[1]][1], ".csv", sep = "")
  write.csv(categoryFile, fileName)
}

### --- semantic topic models for each category

### --- to nohup.out
# - to runtime Log:
print("STEP: Semantic Modeling Phase: LDA estimation")
itemFiles <- list.files()[grepl(".csv", x = list.files(), fixed = T)]
itemFiles <- itemFiles[grepl("^tfMatrix_", itemFiles)]
for (i in 1:length(itemFiles)) {

  categoryName <- strsplit(itemFiles[i], split = ".", fixed = T)[[1]][1]
  categoryName <- strsplit(categoryName, split = "_", fixed = T)[[1]][2]

  # - topic modeling:
  itemCat <- read.csv(itemFiles[i],
                      header = T,
                      check.names = F,
                      row.names = 1,
                      stringsAsFactors = F)
  itemCat <- as.simple_triplet_matrix(itemCat)
  
  ## -- run on K = seq(2,20) semantic topics
  
  # - to runtime Log:
  print(paste("----------------------- LDA model: category ", i, ".", sep = ""))  
  topicModel <- list()
  numTopics <- seq(2, 10, by = 1)
  for (k in 1:length(numTopics)) {
    topicModel[[k]] <- maptpx::topics(counts = itemCat,
                                      K = numTopics[k],
                                      shape = NULL,
                                      initopics = NULL,
                                      tol = 0.1,
                                      bf = T,
                                      kill = 0,
                                      ord = TRUE,
                                      verb = 2)
  }
  # - clear:
  rm(itemCat); gc()
  # - determine model from Bayes Factor against Null:
  wModel <- which.max(sapply(topicModel, function(x) {x$BF}))
  topicModel <- topicModel[[wModel]]

  # - collect matrices:
  wdcm_itemtopic <- as.data.frame(topicModel$theta)
  colnames(wdcm_itemtopic) <- paste("topic", seq(1, dim(wdcm_itemtopic)[2]), sep = "")
  itemTopicFileName <- paste('wdcm2_itemtopic',
                             paste(categoryName, ".csv", sep = ""),
                             sep = "_")
  write.csv(wdcm_itemtopic, itemTopicFileName)

  wdcm_projecttopic <- as.data.frame(topicModel$omega)
  colnames(wdcm_projecttopic) <- paste("topic", seq(1, dim(wdcm_projecttopic)[2]), sep = "")
  wdcm_projecttopic$project <- rownames(wdcm_projecttopic)
  wdcm_projecttopic$projecttype <- projectType(wdcm_projecttopic$project)
  projectTopicFileName <- paste('wdcm2_projecttopic',
                                paste(categoryName, ".csv", sep = ""),
                                sep = "_")
  write.csv(wdcm_projecttopic, projectTopicFileName)

  # - clear:
  rm(topicModel); rm(wdcm_projecttopic); rm(wdcm_itemtopic); gc()

}

### --- t-SNE 2D maps from wdcm2_projectttopic files: projects similarity structure
# - to runtime Log:
print("STEP: Semantic Modeling Phase: t-SNE 2D MAPS")
projectFiles <- list.files()
projectFiles <- projectFiles[grepl("^wdcm2_projecttopic", projectFiles)]
for (i in 1:length(projectFiles)) {
  # filename:
  fileName <- strsplit(projectFiles[i], split = ".", fixed = T)[[1]][1]
  fileName <- strsplit(fileName, split = "_", fixed = T)[[1]][3]
  fileName <- paste("wdcm2_tsne2D_project_", fileName, ".csv", sep = "")
  # load:
  projectTopics <- read.csv(projectFiles[i],
                            header = T,
                            check.names = F,
                            row.names = 1,
                            stringsAsFactors = F)
  projectTopics$project <- NULL
  projectTopics$projecttype <- NULL
  # - Distance space, metric: Hellinger
  projectDist <- as.matrix(dist(projectTopics, method = "Hellinger", by_rows = T))
  # - t-SNE 2D map
  tsneProject <- Rtsne(projectDist,
                       theta = .5,
                       is_distance = T,
                       perplexity = 10)
  # - store:
  tsneProject <- as.data.frame(tsneProject$Y)
  colnames(tsneProject) <- paste("D", seq(1:dim(tsneProject)[2]), sep = "")
  tsneProject$project <- rownames(projectTopics)
  tsneProject$projecttype <- projectType(tsneProject$project)
  write.csv(tsneProject, fileName)
  # - clear:
  rm(projectTopics); rm(projectDist); rm(tsneProject)
}

### --- {visNetwork} graphs from wdcm2_projectttopic files: projects similarity structure

# - to runtime Log:
print("STEP: {visNetwork} graphs from wdcm2_projectttopic files")
projectFiles <- list.files()
projectFiles <- projectFiles[grepl("^wdcm2_projecttopic", projectFiles)]
for (i in 1:length(projectFiles)) {
  # - load:
  projectTopics <- read.csv(projectFiles[i],
                            header = T,
                            check.names = F,
                            row.names = 1,
                            stringsAsFactors = F)
  projectTopics$project <- NULL
  projectTopics$projecttype <- NULL
  # - Distance space, metric: Hellinger
  projectDist <- as.matrix(dist(projectTopics, method = "Hellinger", by_rows = T))
  # - {visNetwork} nodes data.frame:
  indexMinDist <- sapply(rownames(projectDist), function(x) {
    w <- which(rownames(projectDist) %in% x)
    y <- sort(projectDist[w, -w], decreasing = T)
    names(y)[length(y)]
  })
  id <- 1:length(colnames(projectDist))
  label <- colnames(projectDist)
  nodes <- data.frame(id = id,
                      label = label,
                      stringsAsFactors = F)
  # - {visNetwork} edges data.frame:
  edges <- data.frame(from = names(indexMinDist),
                      to = unname(indexMinDist),
                      stringsAsFactors = F)
  edges$from <- sapply(edges$from, function(x) {
    nodes$id[which(nodes$label %in% x)]
  })
  edges$to <- sapply(edges$to, function(x) {
    nodes$id[which(nodes$label %in% x)]
  })
  edges$arrows <- rep("to", length(edges$to))
  # filenames:
  fileName <- strsplit(projectFiles[i], split = ".", fixed = T)[[1]][1]
  fileName <- strsplit(fileName, split = "_", fixed = T)[[1]][3]
  nodesFileName <- paste("wdcm2_visNetworkNodes_project_", fileName, ".csv", sep = "")
  edgesFileName <- paste("wdcm2_visNetworkEdges_project_", fileName, ".csv", sep = "")
  # store:
  write.csv(nodes, nodesFileName)
  write.csv(edges, edgesFileName)
  # - clear:
  rm(projectTopics); rm(projectDist); rm(nodes); rm(edges); gc()
}

# - write to WDCM main reporting file:
setwd(logDir)
mainReport <- read.csv('WDCM_MainReport.csv',
                       header = T,
                       row.names = 1,
                       check.names = F,
                       stringsAsFactors = F)
newReport <- data.frame(Step = 'Pre-Process',
                        Time = as.character(Sys.time()),
                        stringsAsFactors = F)
mainReport <- rbind(mainReport, newReport)
write.csv(mainReport, 'WDCM_MainReport.csv')

### --- toLabsReport
toLabsReport <- data.frame(timeStamp = as.character(Sys.time()),
                           statbox = "stat1005",
                           sqoopbox = "stat1004",
                           stringsAsFactors = F)
write.csv(toLabsReport, "toLabsReport.csv")

# - to runtime Log:
print(paste("--- UPDATE RUN COMPLETED ON:", Sys.time(), sep = " "))

### --- copy reports to /srv/published-datasets/wdcm:

# - WDCM_MainReport
system(command = 'cp /home/goransm/RScripts/WDCM_R/WDCM_Logs/WDCM_MainReport.csv /srv/published-datasets/wdcm/', wait = T)
# - toLabsReport
system(command = 'cp /home/goransm/RScripts/WDCM_R/WDCM_Logs/toLabsReport.csv /srv/published-datasets/wdcm/', wait = T)
