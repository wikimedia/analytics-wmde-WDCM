
### ---------------------------------------------------------------------------
### --- WDCM Search Module, v. Beta 0.1
### --- Script: WDCM_Sqoop_Clients.R, v. Beta 0.1
### --- Author: Dan Andreescu, Software Engineer, WMF (Sqoop, HiveQL)
### --- Author: Goran S. Milovanovic, Data Analyst, WMDE (R code)
### --- Developed under the contract between Goran Milovanovic PR Data Kolektiv 
### --- and WMDE.
### --- Contact: goran.milovanovic_ext@wikimedia.de
### ---------------------------------------------------------------------------
### --- DESCRIPTION:
### --- WDCM_Sqoop_Clients.R takes a list of client projects
### --- that maintain the wbc_entity_usage tables
### --- and sqoopes these tables into a single Hadoop Avro file
### --- on production (currently: stat1005.eqiad.wmnet).
### ---------------------------------------------------------------------------
### --- OUTPUT: 
### --- Results are stored on the Hadoop cluster
### --- database: goransm
### --- directory: wdcmsqoop
### --- table: wdcm_clients_wb_entity_usage
### ---------------------------------------------------------------------------
### --- RUN:
### --- nohup Rscript /home/goransm/RScripts/WDCM_R/WDCM_Sqoop_Clients.R &
### ---------------------------------------------------------------------------

### ---------------------------------------------------------------------------
### --- GPL v2
# This file is part of Wikidata Concepts Monitor (WDCM)
# 
# WDCM is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.
# 
# WDCM is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with WDCM. If not, see <http://www.gnu.org/licenses/>.
### ---------------------------------------------------------------------------

### --- Setup
rm(list = ls())

### --- Collect all client projects that maintain wbc_entitiy_usage
# - show all databases
mySqlArgs <- 
  '--defaults-file=/etc/mysql/conf.d/analytics-research-client.cnf -h analytics-store.eqiad.wmnet -A'
mySqlInput <- '"SHOW DATABASES;" > /home/goransm/RScripts/WDCM_R/wdcmAuxDir/databases.tsv'
# - command:
mySqlCommand <- paste0("mysql ", mySqlArgs, " -e ", mySqlInput, collapse = "")
system(command = mySqlCommand, wait = TRUE)
# - get databases
setwd('/home/goransm/RScripts/WDCM_R/wdcmAuxDir')
clients <- read.table('databases.tsv', header = T, check.names = F, stringsAsFactors = F, sep = "\t")
# - select client projects
wClients <- which(grepl("wiki$|books$|voyage$|source$|quote$|wiktionary$|news$|media$", clients$Database))
clients <- clients$Database[wClients]
# - look-up for wbc_entity_usage tables
projectsTracking <- character()
for (i in 1:length(clients)) {
  # - show tables from the i-th client project
  mySqlArgs <- 
    '--defaults-file=/etc/mysql/conf.d/analytics-research-client.cnf -h analytics-store.eqiad.wmnet -A'
  mySqlInput1 <- paste('"USE ', clients[i], ';', sep = "")
  mySqlInput2 <- 'SHOW TABLES;\" > /home/goransm/RScripts/WDCM_R/wdcmAuxDir/clienttables.tsv'
  mySqlInput <- paste(mySqlInput1, mySqlInput2, sep = "")
  # - command:
  mySqlCommand <- paste0("mysql ", mySqlArgs, " -e ", mySqlInput, collapse = "")
  system(command = mySqlCommand, wait = TRUE)
  tables <- read.table('clienttables.tsv', header = T, check.names = F, stringsAsFactors = F, sep = "\t")
  if("wbc_entity_usage" %in% tables[, 1]) {
    projectsTracking <- append(projectsTracking, clients[i]) 
  }
}
# - store projectsTracking
write.csv(projectsTracking, "projectsTracking.csv")

### --- Sqoop all client projects that maintain wbc_entitiy_usage
wdcmSqoopReport <- data.frame(project = projectsTracking,
                              startTime = character(length(projectsTracking)),
                              endTime = character(length(projectsTracking)),
                              stringsAsFactors = F
                              )
for (i in 1:length(projectsTracking)) {
  wdcmSqoopReport$project[i] <- projectsTracking[i]
  wdcmSqoopReport$startTime[i] <- as.character(Sys.time())
  # - sqoop command:
  sqoopCommand <- paste("sqoop import --connect jdbc:mysql://analytics-store.eqiad.wmnet/",
                        projectsTracking[i],
                        ' --password-file /user/goransm/mysql-analytics-research-client-pw.txt --username research -m 4 ',
                        '--query "select * from wbc_entity_usage where \\$CONDITIONS" --split-by eu_row_id --as-avrodatafile --target-dir /user/goransm/wdcmsqoop/wdcm_clients_wb_entity_usage/wiki_db=',
                        projectsTracking[i],
                        ' --delete-target-dir',
                        sep = ""
                        )
  system(command = sqoopCommand, wait = TRUE)
  # - create Hive table if this is the first entry:
  if (i == 1) {
    hiveCommand <- "\"USE goransm; CREATE EXTERNAL TABLE \\\`goransm.wdcm_clients_wb_entity_usage\\\`(
                        \\\`eu_row_id\\\`           bigint      COMMENT '',
                        \\\`eu_entity_id\\\`        string      COMMENT '',
                        \\\`eu_aspect\\\`           string      COMMENT '',
                        \\\`eu_page_id\\\`          bigint      COMMENT '',
                        \\\`eu_touched\\\`          string      COMMENT ''
                      )
                      COMMENT
                        ''
                      PARTITIONED BY (
                        \\\`wiki_db\\\` string COMMENT 'The wiki_db project')
                      ROW FORMAT SERDE
                        'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
                      STORED AS INPUTFORMAT
                        'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
                      OUTPUTFORMAT
                        'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
                      LOCATION
                        'hdfs://analytics-hadoop/user/goransm/wdcmsqoop/wdcm_clients_wb_entity_usage';\""
    hiveCommand <- paste("beeline -e ", hiveCommand, sep = "")
    system(command = hiveCommand, wait = TRUE)
  }
  # - repair partitions:
  system(command = 'beeline -e "USE goransm; SET hive.mapred.mode = nonstrict; MSCK REPAIR TABLE wdcm_clients_wb_entity_usage;"', wait = TRUE)

  # - REPORT
  wdcmSqoopReport$endTime[i] <- as.character(Sys.time())
  print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
  print("--------------------------------------------------------------------------------")
  print(paste("FINISHED PROJECT:", i, sep = " "))
  print("--------------------------------------------------------------------------------")
  print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
  
}

# - save wdcmSqoopReport
fileName <- paste("wdcmSqoopReport_", strsplit(as.character(Sys.time()), split = " ")[[1]][1], ".csv", sep = "")
write.csv(wdcmSqoopReport, fileName)

