#!/usr/bin/env Rscript

### ---------------------------------------------------------------------------
### --- WDCM Engine, v. Beta 0.1
### --- Script: WDCM_Engine.R, v. Beta 0.1
### --- Author: Goran S. Milovanovic, Data Analyst, WMDE
### --- Developed under the contract between Goran Milovanovic PR Data Kolektiv
### --- and WMDE.
### --- Contact: goran.milovanovic_ext@wikimedia.de
### ---------------------------------------------------------------------------
### --- DESCRIPTION:
### --- WDCM_Engine_goransm.R unifies the previous
### --- three WDCM Engine scripts:
### --- WDCM_Collect_Items.R
### --- WDCM_Search_Clients.R
### --- WDCM_Pre-Process.R
### --- NOTE: the execution of this WDCM script is always dependent upon the
### --- previous WDCM_Sqoop_Clients.R run from stat1004 (currently).
### --- Each section in WDCM_Engine.R provides additional explanation.
### --- NOTE: WDCM_Engine.R is the only WDCM R script
### --- that is run from the statboxes (stat1005 currently)
### --- to produce the WDCM update.
### ---------------------------------------------------------------------------
### --- RUN FROM: /home/goransm/RScripts/WDCM_R
### --- nohup Rscript WDCM_Engine_goransm.R &
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
### --- Script 1: WDCM_Collect_Items.R, WDCM Search Module
### ---------------------------------------------------------------------------

### ---------------------------------------------------------------------------
### --- WDCM Search Module, v. Beta 0.1
### --- Script: WDCM_Collect_Items.R, v. Beta 0.1
### --- Author: Goran S. Milovanovic, Data Analyst, WMDE
### --- Developed under the contract between Goran Milovanovic PR Data Kolektiv
### --- and WMDE.
### --- Contact: goran.milovanovic_ext@wikimedia.de
### ---------------------------------------------------------------------------
### --- DESCRIPTION:
### --- WDCM_Collect_Items.R takes a list of concepts (categories)
### --- defined by a given WDCM Ontology (human input) and then
### --- contacts the Wikidata Query Service to fetch all relevant item IDs.
### ---------------------------------------------------------------------------
### --- INPUT:
### --- the WDCM_Collect_Items.R reads the WDCM Ontology file (csv)
### --- from /WDCM_Ontology
### --- on stat1005
### --- ACTIVE WDCM TAXONOMY: WDCM_Ontology_Berlin_05032017.csv
### ---------------------------------------------------------------------------
### --- OUTPUT:
### --- Results are stored locally as .csv files on stat1005:
### --- in: /WDCM_CollectedItems
### ---------------------------------------------------------------------------

# - to nohup.out
print(paste("--- UPDATE RUN STARTED ON:", Sys.time(), sep = " "))

### --- Setup
# - contact:
library(httr)
library(XML)
# - wrangling:
library(stringr)
library(readr)
library(data.table)
library(tidyr)
# - modeling:
library(maptpx)
library(Rtsne)
library(proxy)

### --- Directories
ontologyDir <- '/WDCM_Ontology' # - NOTE: starting dir, not '..' 
logDir <- '../WDCM_Logs'
itemsDir <- '../WDCM_CollectedItems/'
dataDir <- '../WDCM_dataOut'

### --- Set proxy
Sys.setenv(
  http_proxy = "http://webproxy.eqiad.wmnet:8080",
  https_proxy = "http://webproxy.eqiad.wmnet:8080")

### --- Read WDCM_Ontology
wDir <- paste(getwd(), ontologyDir, sep = "")
setwd(wDir)

wdcmOntology <- read.csv("WDCM_Ontology_Berlin_05032017.csv",
                         header = T,
                         check.names = F,
                         stringsAsFactors = F)

### --- Select all instances accross all sub-classes of searchItems:
# - endPoint:
endPointURL <- "https://query.wikidata.org/bigdata/namespace/wdq/sparql?format=xml&query="

# - track the number of items fetched:
totalN <- numeric()

# - set itemsDir:
setwd(itemsDir)
# - clear output dir:
lF <- list.files()
rmF <- file.remove(lF)

# - track uncompleted queries:
qErrors <- character()

# - startTime (WDCM Main Report)
startTime <- as.character(Sys.time())

for (i in 1:length(wdcmOntology$CategoryItems)) {

  # - to nohup.out
  print(paste("SPARQL category:", i, sep = ""))

  searchItems <- str_trim(
    strsplit(wdcmOntology$CategoryItems[i],
             split = ",", fixed = T)[[1]],
    "both")

  itemsOut <- list()

  for (k in 1:length(searchItems)) {

    # - Construct Query:
    query <- paste0(
      'PREFIX wd: <http://www.wikidata.org/entity/> ',
      'PREFIX wdt: <http://www.wikidata.org/prop/direct/> ',
      'SELECT ?item WHERE {?item (wdt:P31|(wdt:P31/wdt:P279*)) wd:',
      searchItems[k],
      '.  }'
    )

    # Run Query:
    res <- GET(paste0(endPointURL, URLencode(query)))

    # If res$status_code == 200, store:

    if (res$status_code == 200) {

      # XML:
      rc <- rawToChar(res$content)
      rc <- htmlParse(rc)

      # clear:
      rm(res); gc()

      # extract:
      items <- xpathSApply(rc, "//uri", xmlValue)
      items <- unname(sapply(items, function(x) {
        strsplit(x, split = "/", fixed = T)[[1]][length(strsplit(x, split = "/", fixed = T)[[1]])]
      }))

      # - as.data.frame:
      items <- data.frame(item = items,
                          stringsAsFactors = F)

      # - to itemsOut:
      itemsOut[[k]] <- items

      # - clear:
      rm(items); gc()

    } else {
      qErrors <- append(qErrors, searchItems[k])
    }

  }

  # - itemsOut as data.frame:
  itemsOut <- rbindlist(itemsOut)

  # - keep only unique items:
  w <- which(!(duplicated(itemsOut$item)))
  itemsOut <- itemsOut[w]

  # store as CSV
  write_csv(itemsOut, path = paste0(wdcmOntology$Category[i],"_ItemIDs.csv"))

  # total numeber of concepts ++:
  totalN <- append(totalN, length(itemsOut$item))

  # clear:
  rm(itemsOut); gc()

}

### --- Fix WDCM_Ontology (Phab T174896#3762820)
# - remove Geographical Object from Organization:
organizationItems <- read.csv('Organization_ItemIDs.csv',
                              header = T,
                              check.names = F,
                              stringsAsFactors = F)
geoObjItems <- read.csv('Geographical Object_ItemIDs.csv',
                              header = T,
                              check.names = F,
                              stringsAsFactors = F)
w <- which(organizationItems$item %in% geoObjItems$item)
if (length(w) > 0) {
  organizationItems <- organizationItems[-w, ]
}
organizationItems <- data.frame(item = organizationItems,
                                stringsAsFactors = F)
# - store:
write.csv(organizationItems, 'Organization_ItemIDs.csv')
# - clear:
rm(organizationItems); rm(geoObjItems); gc()
# - remove Book from Work of Art:
bookItems <- read.csv('Book_ItemIDs.csv',
                      header = T,
                      check.names = F,
                      stringsAsFactors = F)
WorkOfArtItems <- read.csv('Work Of Art_ItemIDs.csv',
                           header = T,
                           check.names = F,
                           stringsAsFactors = F)
w <- which(WorkOfArtItems$item %in% bookItems$item)
if (length(w) > 0) {
  WorkOfArtItems <- WorkOfArtItems[-w, ]
}
WorkOfArtItems <- data.frame(item = WorkOfArtItems,
                             stringsAsFactors = F)
# - store:
write.csv(WorkOfArtItems, 'Work Of Art_ItemIDs.csv')
# - clear:
rm(WorkOfArtItems); rm(bookItems); gc()
# - remove Architectural Structure from Geographical Object:
architectureItems <- read.csv('Architectural Structure_ItemIDs.csv',
                              header = T,
                              check.names = F,
                              stringsAsFactors = F)
geoObjItems <- read.csv('Geographical Object_ItemIDs.csv',
                        header = T,
                        check.names = F,
                        stringsAsFactors = F)
w <- which(geoObjItems$item %in% architectureItems$item)
if (length(w) > 0) {
  geoObjItems <- geoObjItems[-w, ]
}
geoObjItems <- data.frame(item = geoObjItems,
                          stringsAsFactors = F)
# - store:
write.csv(geoObjItems, 'Geographical Object_ItemIDs.csv')
# - clear:
rm(geoObjItems); rm(architectureItems); gc()

# - log uncompleted queries;
# - set log dir:
setwd(logDir)
write.csv(qErrors, "WDCM_CollectItems_SPARQL_Errors.csv")
# - write to WDCM main reporting file:
# - check whether WDCM_MainReport.csv exists:
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

### --- Setup
scriptDir <- '../'

# - read item categories:
setwd(itemsDir)
idFiles <- list.files()
idFiles <- idFiles[grepl(".csv$", idFiles)]
idFilesSize <- file.size(idFiles)/1024^2

# - Track all categories under processing:
wdcmSearchReport <- data.frame(category = idFiles,
                               fileSize = idFilesSize,
                               startTime = character(length(idFiles)),
                               endTime = character(length(idFiles)),
                               stringsAsFactors = F
)
wdcmSearchReport <- wdcmSearchReport[order(-wdcmSearchReport$fileSize), ]

# - check if goransm.wdcm_maintable exists in Hadoop; if yes, drop it:
# - beeline drop wdcm_maintable (erase metastore data):
system(command = 'beeline --silent -e "USE goransm; DROP TABLE IF EXISTS wdcm_maintable;"', wait = T)
# - delete files for EXTERNAL Hive table from /user/goransm/wdcmtables (hdfs path)
system(command = 'hdfs dfs -rm -r /user/goransm/wdcmtables', wait = T)
# - make directory for EXTERNAL Hive table /user/goransm/wdcmtables (hdfs path)
system(command = 'hdfs dfs -mkdir /user/goransm/wdcmtables', wait = T)

# - loop over item categories:
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
    
    # - short report:
    print("--------------------------------------------------------")
    print(paste("------------- Processing category: ", i, ": ", wdcmSearchReport$category[i], sep = ""))
    print("--------------------------------------------------------")
    print(paste("------------- Processing batch: ", batch, " out of ", batchNum, sep = ""))
    print("--------------------------------------------------------")
    
    # - create goransm.wdcm_maintable Hive table if this is the first entry:
    # - (create wdcm_maintable Hive Table on (hdfs path): /user/goransm/wdcmtables)
    
    if ((i == 1) & (batch == 1)) {
      
      print("--------------------------------------------------------")
      print("------------- CREATE wdcm_maintable TABLE --------------")
      print("--------------------------------------------------------")
      
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
      hiveCommand <- paste("beeline --silent -e ", hiveCommand, sep = "")
      system(command = hiveCommand, wait = TRUE)
    }
    
    # - construct HiveQL query to search for category i items
    # - across all wiki_db:
    
    print("--------------------------------------------------------")
    print("------------- RUNNING HiveQL Query ---------------------")
    print("------------- to search for category items -------------")
    print("--------------------------------------------------------")
    
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
    setwd(scriptDir)
    write(hiveQLQuery, "hiveQLQuery.hql")
    # - execute HiveQLQuery:
    hiveQLQueryCommand <- "beeline --silent -f /home/goransm/RScripts/WDCM_R/hiveQLQuery.hql"
    system(command = hiveQLQueryCommand, wait = TRUE)
    
    print("--------------------------------------------------------")
    print("------------- REPAIR TABLE -----------------------------")
    print("--------------------------------------------------------")
    
    # - repair partitions:
    system(command =
             'beeline --silent -e "USE goransm; SET hive.mapred.mode = nonstrict; MSCK REPAIR TABLE wdcm_maintable;"',
           wait = TRUE)
    
    # - end time for this category:
    wdcmSearchReport$endTime[i] <- as.character(Sys.time())
    
    # - back to item categories:
    setwd(paste(getwd(), gsub("..", "", itemsDir, fixed = T), sep = ""))
    
  }
  
}

# - store report:
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
### --- NOTE: one .tsv file per category (~14M rows, causes Java gc overflow from beeline...)

# - read item categories:
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
  hiveQLquery <- paste(
    'USE goransm; SELECT eu_entity_id, SUM(eu_count) AS eu_count FROM wdcm_maintable WHERE category=\\"',
    categories[i],
    '\\" GROUP BY eu_entity_id ORDER BY eu_count DESC LIMIT 10000000;',
    sep = "")

  system(command = paste('beeline --silent -e "',
                         hiveQLquery,
                         '" > /home/goransm/RScripts/WDCM_R/WDCM_dataOut/',
                         filename,
                         sep = ""),
         wait = TRUE)
}


### --- to dataDir:
setwd(dataDir)
# - clear dataDir:
lF <- list.files()
rmF <- file.remove(lF)

### --- ETL Phase

### --- produce wdcm_project_category.tsv from wdcm_maintable (hdfs, database: goransm)
hiveQLquery <- 'USE goransm; 
                SET hive.mapred.mode=unstrict; 
                SELECT eu_project, category, SUM(eu_count) AS eu_count 
                FROM wdcm_maintable 
                GROUP BY eu_project, category ORDER BY eu_count DESC LIMIT 10000000;'
system(command = paste('beeline --silent -e "',
                       hiveQLquery,
                       '" > /home/goransm/RScripts/WDCM_R/WDCM_dataOut/wdcm_project_category.tsv',
                       sep = ""),
       wait = TRUE)
# - add projecttype to wdcm_project_category.tsv
wdcm_project_category <- as.data.frame(fread('wdcm_project_category.tsv'))
wdcm_project_category$projectype <- projectType(wdcm_project_category$eu_project)
write.csv(wdcm_project_category, "wdcm_project_category.csv")

### --- produce wdcm_project.tsv from wdcm_maintable (hdfs, database: goransm)
hiveQLquery <- 'USE goransm; 
                SET hive.mapred.mode=unstrict; 
                SELECT eu_project, SUM(eu_count) AS eu_count 
                FROM wdcm_maintable 
                GROUP BY eu_project ORDER BY eu_count DESC LIMIT 10000000;'
system(command = paste('beeline --silent -e "',
                       hiveQLquery,
                       '" > /home/goransm/RScripts/WDCM_R/WDCM_dataOut/wdcm_project.tsv',
                       sep = ""),
       wait = TRUE)
# - add projecttype to wdcm_project.tsv
wdcm_project <- as.data.frame(fread('wdcm_project.tsv'))
wdcm_project$projectype <- projectType(wdcm_project$eu_project)
write.csv(wdcm_project, "wdcm_project.csv")

### --- produce wdcm_category.tsv from wdcm_maintable (hdfs, database: goransm)
hiveQLquery <- 'USE goransm; 
                SET hive.mapred.mode=unstrict; 
                SELECT category, SUM(eu_count) AS eu_count 
                FROM wdcm_maintable 
                GROUP BY category ORDER BY eu_count DESC LIMIT 10000000;'
system(command = paste('beeline --silent -e "',
                       hiveQLquery,
                       '" > /home/goransm/RScripts/WDCM_R/WDCM_dataOut/wdcm_category.tsv',
                       sep = ""),
       wait = TRUE)
# - save wdcm_category.tsv as .csv
wdcm_category <- as.data.frame(fread('wdcm_category.tsv'))
write.csv(wdcm_category, "wdcm_category.csv")

### --- produce wdcm_project_category_item100.tsv from wdcm_maintable (hdfs, database: goransm)
hiveQLquery <- 'USE goransm; 
                SET hive.mapred.mode=unstrict; 
                SELECT * FROM (
                  SELECT eu_project, category, eu_entity_id, eu_count, ROW_NUMBER() OVER (PARTITION BY eu_project, category ORDER BY eu_count DESC) AS row 
                    FROM wdcm_maintable) t 
                WHERE row <= 100;'
system(command = paste('beeline --silent -e "',
                       hiveQLquery,
                       '" > /home/goransm/RScripts/WDCM_R/WDCM_dataOut/wdcm_project_category_item100.tsv',
                       sep = ""),
       wait = TRUE)
# - add projecttype to wdcm_project_category_item100.tsv
wdcm_project_category_item100 <- as.data.frame(fread('wdcm_project_category_item100.tsv'))
wdcm_project_category_item100$projectype <- projectType(wdcm_project_category_item100$t.eu_project)
write.csv(wdcm_project_category_item100, "wdcm_project_category_item100.csv")

### --- produce wdcm_project_item100.tsv from wdcm_maintable (hdfs, database: goransm)
hiveQLquery <- 'USE goransm; 
                SET hive.mapred.mode=unstrict; 
                SELECT * FROM (
                  SELECT eu_project, eu_entity_id, eu_count, ROW_NUMBER() OVER (PARTITION BY eu_project ORDER BY eu_count DESC) AS row 
                FROM wdcm_maintable) t 
                WHERE row <= 100;'
system(command = paste('beeline --silent -e "',
                       hiveQLquery,
                       '" > /home/goransm/RScripts/WDCM_R/WDCM_dataOut/wdcm_project_item100.tsv',
                       sep = ""),
       wait = TRUE)
# - add projecttype to wdcm_project_item100.tsv
wdcm_project_item100 <- as.data.frame(fread('wdcm_project_item100.tsv'))
wdcm_project_item100$projectype <- projectType(wdcm_project_item100$t.eu_project)
write.csv(wdcm_project_item100, "wdcm_project_item100.csv")


### --- Semantic Modeling Phase

### --- produce project-item matrices for semantic topic modeling
print("Semantic Modeling Phase: TDF MATRICES")
itemFiles <- list.files()
itemFiles <- itemFiles[grepl("^wdcm_item", itemFiles)]
for (i in 1:length(itemFiles)) {
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
  system(command = paste('beeline --silent -e "',
                         hiveQLquery,
                         '" > /home/goransm/RScripts/WDCM_R/WDCM_dataOut/',
                         fileName,
                         sep = ""),
         wait = TRUE)
}

### --- reshape project-item matrices for semantic topic modeling
print("Semantic Modeling Phase: RESHAPING TDF MATRICES")
itemFiles <- list.files()
itemFiles <- itemFiles[grepl("^tfMatrix_", itemFiles)]
itemFiles <- itemFiles[grepl(".tsv", itemFiles, fixed = T)]
for (i in 1:length(itemFiles)) {
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
print("Semantic Modeling Phase: LDA estimation")
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
  # - run on K = seq(2,20) semantic topics
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
print("Semantic Modeling Phase: t-SNE 2D MAPS")
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

print(paste("--- UPDATE RUN COMPLETED ON:", Sys.time(), sep = " "))

