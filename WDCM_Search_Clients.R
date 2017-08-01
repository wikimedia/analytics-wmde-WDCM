
### ---------------------------------------------------------------------------
### --- WDCM Search Module, v. Beta 0.1
### --- Script: WDCM_Search_Clients.R, v. Beta 0.1
### --- Author: Goran S. Milovanovic, Data Analyst, WMDE
### --- Developed under the contract between Goran Milovanovic PR Data Kolektiv 
### --- and WMDE.
### --- Contact: goran.milovanovic_ext@wikimedia.de
### ---------------------------------------------------------------------------
### --- DESCRIPTION:
### --- WDCM_Search_Clients_HiveQL.R takes a list of
### --- item IDs from Wikidata (the list is delivered by
### --- the WDCM_Search_Items.R script) and searches for their
### --- usage across the Wikimedia projects in Hadoop:
### --- database: goransm
### --- directory: wdcmsqoop
### --- table: wdcm_clients_wb_entity_usage
### --- from production (currently: stat1005.eqiad.wmnet).
### ---------------------------------------------------------------------------
### --- INPUT: 
### --- the WDCM_Search_Clients_HiveQL.R reads the list of item IDs
### --- to search for from /home/goransm/WDCM_DataIN/WDCM_DataIN_SearchItems
### --- on stat1005.
### --- This folder contains the .csv files that specify the item IDs
### --- to search for; the files are produced by WDCM_Collect_Items.R 
### --- running on wikidataconcepts.wmflabs.org Labs instance.
### ---------------------------------------------------------------------------
### --- OUTPUT: 
### --- Results are stored locally as .tsv files on production,
### --- with each file encompassing the data for one client project.
### --- The outputs are stored locally on stat1005.eqiad.wmnet in:
### --- /a/published-datasets/WDCM/Search_Items_DataOUT
### --- These output .tsv files migrate to Labs:
### --- wikidataconcepts.wmflabs.org Cloud VPS instance
### --- where they are then processed by the WDCM Process Module.
### ---------------------------------------------------------------------------
### --- RUN:
### --- nohup Rscript /home/goransm/RScripts/WDCM_R/WDCM_Search_Clients.R &
### ---------------------------------------------------------------------------

### ---------------------------------------------------------------------------
### --- GPL v2
### ---------------------------------------------------------------------------
### --- This file is part of Wikidata Concepts Monitor (WDCM)
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
rm(list = ls())
auxDir <- '/home/goransm/RScripts/WDCM_R/wdcmAuxDir/'
dataInputDir <- '/home/goransm/WDCM_DataIN/WDCM_DataIN_SearchItems/'
scriptDir <- '/home/goransm/RScripts/WDCM_R/'
setwd(auxDir)

# - read item categories:
setwd(dataInputDir)
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
system(command = 'beeline -e "USE goransm; DROP TABLE IF EXISTS wdcm_maintable;"', wait = T)
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
      print("------------- CREATE TABLE -----------------------------")
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
      hiveCommand <- paste("beeline -e ", hiveCommand, sep = "")
      system(command = hiveCommand, wait = TRUE)
    }
    
    # - construct HiveQL query to search for category i items
    # - across all wiki_db:
    
    print("--------------------------------------------------------")
    print("------------- RUNNING HiveQL Query ---------------------")
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
             'beeline -e "USE goransm; SET hive.mapred.mode = nonstrict; MSCK REPAIR TABLE wdcm_maintable;"',
           wait = TRUE)
    
    # - end time for this category:
    wdcmSearchReport$endTime[i] <- as.character(Sys.time())
    
    # - back to item categories:
    setwd(dataInputDir)
    
  }
  
  # - wait 2 minutes before executing next batch:
  print("--------------------------------------------------------")
  print("------------- WAIT 2 minutes ---------------------------")
  print("--------------------------------------------------------")
  Sys.sleep(120)
      
}

# - store report:
setwd(auxDir)
write.csv(wdcmSearchReport, 
          paste("wdcmSearchReport_", Sys.time(), ".csv", sep = ""))




