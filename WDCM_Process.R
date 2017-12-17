
### ---------------------------------------------------------------------------
### --- WDCM Process Module, v. Beta 0.1
### --- Script: WDCM_Process_v2.R, v. Beta 0.1
### ---------------------------------------------------------------------------
### --- DESCRIPTION:
### --- WDCM_Process_v2.R takes a list of .tsv files that present
### --- the data from wbc_entity_usage tables accross the client projects
### --- fetched from production (stat1005) by WDCM_Search_Clients.R and 
### --- further pre-processed by WDCM_Pre-Process.R (also on production).
### --- The goal of this WDCM module/script is to produce (or update) 
### --- the WDCM Stats Dashboard database.
### ---------------------------------------------------------------------------
### --- INPUT: 
### --- the WDCM_Process_v2.R reads the .tsv input files from:
### --- /home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_DataIN_ClientUsage_v2/
### --- on the wikidataconcepts.eqiad.wmflabs Cloud VPS instance
### --- These files are brought to Labs directly from productio
### --- (currently the stat1005.eqiad.wmnet statbox)
### ---------------------------------------------------------------------------
### --- OUTPUT: the WDCM Dashboards MariaDB database is update
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
library(RMySQL)
library(httr)
library(XML)
library(data.table)
library(dplyr)
library(tidyr)
library(readr)
library(htmltab)
library(snowfall)
library(maptpx)
library(Rtsne)

# - mysql --defaults-file=/home/goransm/mySQL_Credentials/replica.my.cnf -h tools.labsdb u16664__wdcm_p
# - database: u16664__wdcm_p

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

### ---------------------------------------------------------------------------
### --- NOTE:
### --- TABLE NAMING CONVENTION FOR v2 (WDCM Stats Dashboard)
### --- wdcm2_something
### ---------------------------------------------------------------------------

# - to nohup.out
print(paste("WDCM Process.R update started at: ", Sys.time(), sep = ""))

# - to working directory:
# - credentials on tools.labsdb
setwd('/home/goransm/WMDE/WDCM/WDCM_RScripts')
mySQLCreds <- read.csv("mySQLCreds.csv",
                       header = T,
                       check.names = F,
                       row.names = 1,
                       stringsAsFactors = F)

### --- list existing tables
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- "SHOW TABLES;"
res <- dbSendQuery(con, q)
st <- fetch(res, -1)
dbClearResult(res)
dbDisconnect(con)
colnames(st) <- "tables"
# - keep only v2 tables:
st <- st[which(grepl("^wdcm2", st$tables)), ]

### ---------------------------------------------------------------------------
### ---------------------------------------------------------------------------
### --- CREATE TABLES for WDCM Projects Stats Dashboard
### ---------------------------------------------------------------------------
### ---------------------------------------------------------------------------

# - to /home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_DataIN_ClientUsage_v2/
inputDir <- '/home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_DataIN_ClientUsage_v2/'
setwd(inputDir)

### ---------------------------------------------------------------------------
### --- wdcm2_project
### ---------------------------------------------------------------------------

# - to nohup.out
print(paste("wdcm2_project: ", Sys.time(), sep = ""))

# - load wdcm_project.tsv as data.frame
wdcm2_project <- fread('wdcm_project.csv', data.table = F)
wdcm2_project$V1 <- NULL
# - check whether wdcm2_project table exists:
checkTable <- which(st %in% "wdcm2_project")
# - DROP wdcm2_project if it exists
if (length(checkTable) == 1) {
  con <- dbConnect(MySQL(), 
                   host = "tools.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "u16664__wdcm_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  q <- "DROP TABLE wdcm2_project;"
  res <- dbSendQuery(con, q)
  dbClearResult(res)
  dbDisconnect(con)
}
# - CREATE wdcm2_project:
# - write to wdcm2_project
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
dbWriteTable(conn = con,
             name = "wdcm2_project",
             value = wdcm2_project,
             row.names = F,
             append = T)
dbDisconnect(con)
rm(wdcm2_project); gc()

### ---------------------------------------------------------------------------
### --- wdcm2_category
### ---------------------------------------------------------------------------

# - to nohup.out
print(paste("wdcm2_category: ", Sys.time(), sep = ""))

# - load wdcm_category.tsv as data.frame
wdcm2_category <- fread('wdcm_category.csv', data.table = F)
wdcm2_category$V1 <- NULL
# - check whether wdcm2_category table exists:
checkTable <- which(st %in% "wdcm2_category")
# - DROP wdcm2_category if it exists
if (length(checkTable) == 1) {
  con <- dbConnect(MySQL(), 
                   host = "tools.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "u16664__wdcm_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  q <- "DROP TABLE wdcm2_category;"
  res <- dbSendQuery(con, q)
  dbClearResult(res)
  dbDisconnect(con)
}
# - CREATE wdcm2_category
# - write to wdcm2_category
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
dbWriteTable(conn = con,
             name = "wdcm2_category",
             value = wdcm2_category,
             row.names = F,
             append = T)
dbDisconnect(con)
rm(wdcm2_category); gc()

### ---------------------------------------------------------------------------
### --- wdcm2_project_category
### ---------------------------------------------------------------------------

# - to nohup.out
print(paste("wdcm2_project_category: ", Sys.time(), sep = ""))

# - load wdcm_category.tsv as data.frame
wdcm2_project_category <- fread('wdcm_project_category.csv', data.table = F)
wdcm2_project_category$V1 <- NULL
### --- FIX THIS (!!!)
colnames(wdcm2_project_category)[4] <- 'projecttype'
### --- FIX THIS (!!!)
# - check whether wdcm2_project_category table exists:
checkTable <- which(st %in% "wdcm2_project_category")
# - DROP wdcm2_project_category if it exists
if (length(checkTable) == 1) {
  con <- dbConnect(MySQL(), 
                   host = "tools.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "u16664__wdcm_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  q <- "DROP TABLE wdcm2_project_category;"
  res <- dbSendQuery(con, q)
  dbClearResult(res)
  dbDisconnect(con)
}
# - CREATE wdcm2_project_category
# - write to wdcm2_project_category
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
qCreate <- 'CREATE TABLE wdcm2_project_category ( 
              eu_project VARCHAR(255) NOT NULL,
              category VARCHAR(37) NOT NULL,
              eu_count INT, 
              projecttype VARCHAR(255) NOT NULL
            ) 
            ENGINE=InnoDB 
            ROW_FORMAT=COMPRESSED;'
res <- dbSendQuery(con, qCreate)
dbClearResult(res)
# - populate wdcm2_project_category
dbWriteTable(conn = con,
             name = "wdcm2_project_category",
             value = wdcm2_project_category,
             row.names = F,
             append = T)
dbDisconnect(con)
rm(wdcm2_project_category); gc()
# - CREATE INDEX on wdcm2_project_category
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
# - CREATE INDEX
q <- "CREATE INDEX wdcm2_project_category_eu_project_category ON wdcm2_project_category (eu_project, category); "
res <- dbSendQuery(con, q)
dbClearResult(res)
dbDisconnect(con)

### ---------------------------------------------------------------------------
### --- wdcm2_project_item100
### ---------------------------------------------------------------------------

# - to nohup.out
print(paste("wdcm2_project_item100: ", Sys.time(), sep = ""))

# - load wdcm_project_item100.tsv as data.frame
wdcm2_project_item100 <- fread('wdcm_project_item100.csv', data.table = F)
wdcm2_project_item100$V1 <- NULL
wdcm2_project_item100$t.row <- NULL
colnames(wdcm2_project_item100) <- c('eu_project', 'eu_entity_id', 'eu_count', 'projecttype')
# - check whether wdcm2_project_item100 table exists:
checkTable <- which(st %in% "wdcm2_project_item100")
# - DROP wdcm2_project_category if it exists
if (length(checkTable) == 1) {
  con <- dbConnect(MySQL(), 
                   host = "tools.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "u16664__wdcm_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  q <- "DROP TABLE wdcm2_project_item100;"
  res <- dbSendQuery(con, q)
  dbClearResult(res)
  dbDisconnect(con)
}
# - get item labels for wdcm2_project_item100
items <- gsub("Q", "", unique(wdcm2_project_item100$eu_entity_id), fixed = T)
itemList <- paste0(items, collapse = ", ", sep = "")
# - fetch English labels where available from wb_terms
con <- dbConnect(MySQL(), 
                 host = "wikidatawiki.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "wikidatawiki_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- paste("SELECT term_entity_id, term_text FROM wb_terms ",
           "WHERE ((term_entity_id IN (", itemList, ")) AND ",
           "(term_language = 'en') AND ",
           "(term_entity_type = 'item') AND (term_type = 'label'));", 
           sep = "");
rm(itemList)
res <- dbSendQuery(con, q)
itemLabels <- fetch(res, -1)
dbClearResult(res)
dbDisconnect(con)
itemLabels$term_entity_id <- paste("Q", itemLabels$term_entity_id, sep = "")
colnames(itemLabels) <- c('eu_entity_id', 'eu_label')
# - insert labels to wdcm2_project_item100 + 
wdcm2_project_item100 <- left_join(wdcm2_project_item100, itemLabels, by = 'eu_entity_id')
# - recognize missing English labels and replace w. the respective eu_entity_id value
wdcm2_project_item100$eu_label[is.na(wdcm2_project_item100$eu_label)] <- 
  wdcm2_project_item100$eu_entity_id[is.na(wdcm2_project_item100$eu_label)]
rm(itemLabels); gc()
# - CREATE wdcm2_project_item100
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
qCreate <- 'CREATE TABLE wdcm2_project_item100 ( 
              eu_project VARCHAR(255) NOT NULL, 
              eu_entity_id VARCHAR(37) NOT NULL, 
              eu_count INT,
              projecttype VARCHAR(255) NOT NULL,
              eu_label VARCHAR(255) NOT NULL) 
              ENGINE=InnoDB 
              ROW_FORMAT=COMPRESSED;'
res <- dbSendQuery(con, qCreate)
dbClearResult(res)
# - populate wdcm2_project_item100
dbWriteTable(conn = con,
             name = "wdcm2_project_item100",
             value = wdcm2_project_item100,
             row.names = F,
             append = T)
dbDisconnect(con)
rm(wdcm2_project_item100); gc()
# - CREATE INDEX on wdcm2_project_item100
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- "CREATE INDEX wdcm2_project_item100_eu_project ON wdcm2_project_item100 (eu_project); "
res <- dbSendQuery(con, q)
dbClearResult(res)
q <- "CREATE INDEX wdcm2_project_item100_projecttype ON wdcm2_project_item100 (projecttype); "
res <- dbSendQuery(con, q)
dbClearResult(res)
dbDisconnect(con)

### ---------------------------------------------------------------------------
### --- wdcm2_project_category_item100
### ---------------------------------------------------------------------------

# - to nohup.out
print(paste("wdcm2_project_category_item100: ", Sys.time(), sep = ""))

# - load wdcm_project_category_item100.tsv as data.frame
wdcm2_project_category_item100 <- fread('wdcm_project_category_item100.csv', data.table = F)
wdcm2_project_category_item100$V1 <- NULL
wdcm2_project_category_item100$t.row <- NULL
colnames(wdcm2_project_category_item100) <- c('eu_project', 'category', 'eu_entity_id', 'eu_count', 'projecttype')
# - check whether wdcm2_project_item100 table exists:
checkTable <- which(st %in% "wdcm2_project_category_item100")
# - DROP wdcm2_project_category if it exists
if (length(checkTable) == 1) {
  con <- dbConnect(MySQL(), 
                   host = "tools.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "u16664__wdcm_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  q <- "DROP TABLE wdcm2_project_category_item100;"
  res <- dbSendQuery(con, q)
  dbClearResult(res)
  dbDisconnect(con)
}
# - get item labels for wdcm2_project_category_item100
items <- gsub("Q", "", unique(wdcm2_project_category_item100$eu_entity_id), fixed = T)
itemList <- paste0(items, collapse = ", ", sep = "")
# - fetch English labels where available from wb_terms
con <- dbConnect(MySQL(), 
                 host = "wikidatawiki.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "wikidatawiki_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- paste("SELECT term_entity_id, term_text FROM wb_terms ",
           "WHERE ((term_entity_id IN (", itemList, ")) AND ",
           "(term_language = 'en') AND ",
           "(term_entity_type = 'item') AND (term_type = 'label'));", 
           sep = "");
rm(itemList); gc()
res <- dbSendQuery(con, q)
itemLabels <- fetch(res, -1)
dbClearResult(res)
dbDisconnect(con)
itemLabels$term_entity_id <- paste("Q", itemLabels$term_entity_id, sep = "")
colnames(itemLabels) <- c('eu_entity_id', 'eu_label')
# - insert labels to wdcm2_project_category_item100 + 
wdcm2_project_category_item100 <- left_join(wdcm2_project_category_item100, itemLabels, 
                                            by = 'eu_entity_id')
# - recognize missing English labels and replace w. the respective eu_entity_id value
wdcm2_project_category_item100$eu_label[is.na(wdcm2_project_category_item100$eu_label)] <- 
  wdcm2_project_category_item100$eu_entity_id[is.na(wdcm2_project_category_item100$eu_label)]
rm(itemLabels); gc()
# - CREATE wdcm2_project_category_item100
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
qCreate <- 'CREATE TABLE wdcm2_project_category_item100 (
              eu_project VARCHAR(255) NOT NULL, 
              category VARCHAR(255) NOT NULL,
              eu_entity_id VARCHAR(37) NOT NULL,
              eu_count INT,
              projecttype VARCHAR(255) NOT NULL,
              eu_label VARCHAR(255) NOT NULL) 
              ENGINE=InnoDB 
              ROW_FORMAT=COMPRESSED;'
res <- dbSendQuery(con, qCreate)
dbClearResult(res)
# - populate wdcm2_project_category_item100
dbWriteTable(conn = con,
             name = "wdcm2_project_category_item100",
             value = wdcm2_project_category_item100,
             row.names = F,
             append = T)
dbDisconnect(con)
rm(wdcm2_project_category_item100); gc()
# - CREATE INDEX on wdcm2_project_category_item100
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- "CREATE INDEX wdcm2_project_category_item100_eu_project_category ON wdcm2_project_category_item100 (eu_project, category);"
res <- dbSendQuery(con, q)
dbClearResult(res)
q <- "CREATE INDEX wdcm2_project_category_item100_eu_project ON wdcm2_project_category_item100 (eu_project);"
res <- dbSendQuery(con, q)
dbClearResult(res)
q <- "CREATE INDEX wdcm2_project_category_item100_category ON wdcm2_project_category_item100 (category);"
res <- dbSendQuery(con, q)
dbClearResult(res)
q <- "CREATE INDEX wdcm2_project_category_item100_projecttype ON wdcm2_project_category_item100 (projecttype);"
res <- dbSendQuery(con, q)
dbClearResult(res)
dbDisconnect(con)

### ---------------------------------------------------------------------------
### --- wdcm2_category_item100
### ---------------------------------------------------------------------------

# - to nohup.out
print(paste("wdcm2_category_item100: ", Sys.time(), sep = ""))

# - check whether wdcm2_category_item100 table exists:
checkTable <- which(st %in% "wdcm2_category_item100")
# - DROP wdcm2_category_item100 if it exists
if (length(checkTable) == 1) {
  con <- dbConnect(MySQL(), 
                   host = "tools.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "u16664__wdcm_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  q <- "DROP TABLE wdcm2_category_item100;"
  res <- dbSendQuery(con, q)
  dbClearResult(res)
  dbDisconnect(con)
}
# - CREATE wdcm2_category_item100
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
qCreate <- 'CREATE TABLE wdcm2_category_item100 (
              eu_entity_id VARCHAR(255) NOT NULL,
              eu_count INT, 
              category VARCHAR(255) NOT NULL, 
              eu_label VARCHAR(255) NOT NULL) 
              ENGINE=InnoDB 
              ROW_FORMAT=COMPRESSED;'
res <- dbSendQuery(con, qCreate)
dbClearResult(res)
dbDisconnect(con)
# - populate wdcm2_category_item100
itemFiles <- list.files()
itemFiles <- itemFiles[grepl("^wdcm_item", itemFiles)]
for (i in 1:length(itemFiles)) {
  # - load categoryFile[i].tsv as data.frame
  categoryName <- strsplit(itemFiles[i], ".", fixed = T)[[1]][1]
  categoryName <- strsplit(categoryName, "_", fixed = T)[[1]][3]
  categoryName <- gsub("([[:lower:]])([[:upper:]])", "\\1 \\2", categoryName)
  ### --- FIX THIS (!!!)
  if (categoryName == "Workof Art") {
    categoryName <- "Work of Art"
  }
  ### --- FIX ABOVE (!!!)
  categoryFile <- fread(itemFiles[i], nrows = 100)
  categoryFile$category <- categoryName
  # - get item labels for categoryFile[i]
  items <- gsub("Q", "", unique(categoryFile$eu_entity_id), fixed = T)
  itemList <- paste0(items, collapse = ", ", sep = "")
  # - fetch English labels where available from wb_terms
  con <- dbConnect(MySQL(), 
                   host = "wikidatawiki.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "wikidatawiki_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  q <- paste("SELECT term_entity_id, term_text FROM wb_terms ",
             "WHERE ((term_entity_id IN (", itemList, ")) AND ",
             "(term_language = 'en') AND ",
             "(term_entity_type = 'item') AND (term_type = 'label'));", 
             sep = "");
  rm(itemList); gc()
  res <- dbSendQuery(con, q)
  itemLabels <- fetch(res, -1)
  dbClearResult(res)
  dbDisconnect(con)
  itemLabels$term_entity_id <- paste("Q", itemLabels$term_entity_id, sep = "")
  colnames(itemLabels) <- c('eu_entity_id', 'eu_label')
  # - insert labels to categoryFile[i] + 
  categoryFile <- left_join(categoryFile, itemLabels, by = 'eu_entity_id')
  # - recognize missing English labels and replace w. the respective eu_entity_id value
  categoryFile$eu_label[is.na(categoryFile$eu_label)] <- 
    categoryFile$eu_entity_id[is.na(categoryFile$eu_label)]
  rm(itemLabels); gc()
  # - populate wdcm2_category_item100 by categoryFile[i]
  con <- dbConnect(MySQL(), 
                   host = "tools.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "u16664__wdcm_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  dbWriteTable(conn = con,
               name = "wdcm2_category_item100",
               value = categoryFile,
               row.names = F,
               append = T)
  dbDisconnect(con)
  rm(categoryFile); gc()
}
# - CREATE INDEX on wdcm2_category_item100
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- "CREATE INDEX wdcm2_category_item100_category ON wdcm2_category_item100 (category); "
res <- dbSendQuery(con, q)
dbClearResult(res)
dbDisconnect(con)

### ---------------------------------------------------------------------------
### --- wdcm2_projects_2dmaps
### ---------------------------------------------------------------------------

# - to nohup.out
print(paste("wdcm2_projects_2dmaps: ", Sys.time(), sep = ""))

# - check whether wdcm2_projects_2dmaps table exists:
checkTable <- which(st %in% "wdcm2_projects_2dmaps")
# - DROP wdcm2_projects_2dmaps if it exists
if (length(checkTable) == 1) {
  con <- dbConnect(MySQL(), 
                   host = "tools.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "u16664__wdcm_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  q <- "DROP TABLE wdcm2_projects_2dmaps;"
  res <- dbSendQuery(con, q)
  dbClearResult(res)
  dbDisconnect(con)
}
# - CREATE wdcm2_projects_2dmaps
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
qCreate <- 'CREATE TABLE wdcm2_projects_2dmaps (
              D1 FLOAT,
              D2 FLOAT,
              eu_project VARCHAR(255) NOT NULL, 
              projecttype VARCHAR(255) NOT NULL,
              category VARCHAR(37) NOT NULL) 
              ENGINE=InnoDB 
              ROW_FORMAT=COMPRESSED;'
res <- dbSendQuery(con, qCreate)
dbClearResult(res)
dbDisconnect(con)
# - populate wdcm2_projects_2dmaps
itemFiles <- list.files()
itemFiles <- itemFiles[grepl("^wdcm2_tsne", itemFiles)]
for (i in 1:length(itemFiles)) {
  # - load categoryFile[i].tsv as data.frame
  categoryName <- strsplit(itemFiles[i], ".", fixed = T)[[1]][1]
  categoryName <- strsplit(categoryName, "_", fixed = T)[[1]][4]
  categoryName <- gsub("([[:lower:]])([[:upper:]])", "\\1 \\2", categoryName)
  ### --- FIX THIS (!!!)
  if (categoryName == "Workof Art") {
    categoryName <- "Work of Art"
  }
  ### --- FIX ABOVE (!!!)
  categoryFile <- fread(itemFiles[i], data.table = F)
  categoryFile$V1 <- NULL
  categoryFile$category <- categoryName
  colnames(categoryFile)[3] <- 'eu_project'
  con <- dbConnect(MySQL(), 
                   host = "tools.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "u16664__wdcm_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  dbWriteTable(conn = con,
               name = "wdcm2_projects_2dmaps",
               value = categoryFile,
               row.names = F,
               append = T)
  dbDisconnect(con)
  rm(categoryFile); gc()
}
# - CREATE INDEX on wdcm2_projects_2dmaps
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- "CREATE INDEX wdcm2_projects_2dmaps_category_euproject ON wdcm2_projects_2dmaps (category, eu_project);"
res <- dbSendQuery(con, q)
dbClearResult(res)
dbDisconnect(con)

### ---------------------------------------------------------------------------
### --- wdcm2_project_category_2dmap
### ---------------------------------------------------------------------------

# - to nohup.out
print(paste("wdcm2_project_category_2dmap: ", Sys.time(), sep = ""))

# - check whether wdcm2_project_category_2dmap table exists:
checkTable <- which(st %in% "wdcm2_project_category_2dmap")
# - DROP wdcm2_project_category_2dmap if it exists
if (length(checkTable) == 1) {
  con <- dbConnect(MySQL(), 
                   host = "tools.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "u16664__wdcm_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  q <- "DROP TABLE wdcm2_project_category_2dmap;"
  res <- dbSendQuery(con, q)
  dbClearResult(res)
  dbDisconnect(con)
}
# - fetch wdcm2_project_category:
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
qCreate <- 'SELECT * FROM wdcm2_project_category;'
res <- dbSendQuery(con, qCreate)
wdcm2_project_category <- fetch(res, -1)
dbClearResult(res)
dbDisconnect(con)
# - wrangle wdcm2_project_category for t-SNE:
tsneData <- wdcm2_project_category[, 1:3]
tsneData <- spread(tsneData,
                   key = category,
                   value = eu_count, 
                   fill = 0)
rownames(tsneData) <- tsneData$eu_project
tsneData$eu_project <- NULL
tsneData <- as.matrix(dist(tsneData, method = "euclidean"))
projects <- rownames(tsneData)
# - t-SNE 2D reduction:
tsneData <- Rtsne(tsneData, theta = .5, is_distance = T)
tsneData <- as.data.frame(tsneData$Y)
tsneData$projects <- projects
tsneData$projecttype <- projectType(tsneData$projects)
colnames(tsneData)[1:2] <- c('D1', 'D2')
# - populate wdcm2_project_category_2dmap
con <- dbConnect(MySQL(),
                 host = "tools.labsdb",
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
dbWriteTable(conn = con,
             name = "wdcm2_project_category_2dmap",
             value = tsneData,
             row.names = F,
             append = T)
dbDisconnect(con)
rm(wdcm2_project_category); rm(tsneData); gc()

### ---------------------------------------------------------------------------
### --- wdcm2_category_project_2dmap
### ---------------------------------------------------------------------------

# - to nohup.out
print(paste("wdcm2_category_project_2dmap: ", Sys.time(), sep = ""))

# - check whether wdcm2_category_project_2dmap table exists:
checkTable <- which(st %in% "wdcm2_category_project_2dmap")
# - DROP wdcm2_category_project_2dmap if it exists
if (length(checkTable) == 1) {
  con <- dbConnect(MySQL(), 
                   host = "tools.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "u16664__wdcm_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  q <- "DROP TABLE wdcm2_category_project_2dmap;"
  res <- dbSendQuery(con, q)
  dbClearResult(res)
  dbDisconnect(con)
}
# - fetch wdcm2_project_category:
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
qCreate <- 'SELECT * FROM wdcm2_project_category;'
res <- dbSendQuery(con, qCreate)
wdcm2_project_category <- fetch(res, -1)
dbClearResult(res)
dbDisconnect(con)
# - wrangle wdcm2_project_category for t-SNE:
tsneData <- wdcm2_project_category[, 1:3]
tsneData <- spread(tsneData,
                   key = category,
                   value = eu_count, 
                   fill = 0)
rownames(tsneData) <- tsneData$eu_project
tsneData$eu_project <- NULL
tsneData <- as.matrix(dist(t(tsneData), method = "euclidean"))
category <- rownames(tsneData)
# - t-SNE 2D reduction:
tsneData <- Rtsne(tsneData, theta = .5, is_distance = T, perplexity = 4)
tsneData <- as.data.frame(tsneData$Y)
tsneData$category <- category
colnames(tsneData)[1:2] <- c('D1', 'D2')
# - populate wdcm2_category_project_2dmap
con <- dbConnect(MySQL(),
                 host = "tools.labsdb",
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
dbWriteTable(conn = con,
             name = "wdcm2_category_project_2dmap",
             value = tsneData,
             row.names = F,
             append = T)
dbDisconnect(con)
rm(wdcm2_project_category); rm(tsneData); gc()

### ---------------------------------------------------------------------------
### --- wdcm2_itemtopic_ tables
### ---------------------------------------------------------------------------

# - to nohup.out
print(paste("wdcm2_itemtopic_ tables: ", Sys.time(), sep = ""))

lF <- list.files()
lF <- lF[grepl("wdcm2_itemtopic_", lF, fixed = T)]
for (i in 1:length(lF)) {
  # - check whether the current wdcm2_itemtopic_ table exists:
  tName <- gsub(".csv", "", lF[i])
  checkTable <- which(st %in% tName)
  # - DROP current wdcm2_itemtopic_ table if it exists
  if (length(checkTable) == 1) {
    con <- dbConnect(MySQL(), 
                     host = "tools.labsdb", 
                     defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                     dbname = "u16664__wdcm_p",
                     user = mySQLCreds$user,
                     password = mySQLCreds$password)
    q <- paste("DROP TABLE ", tName, ";", sep = "")
    res <- dbSendQuery(con, q)
    dbClearResult(res)
    dbDisconnect(con)
  }
  # - populate current wdcm2_itemtopic_
  cTable <- fread(lF[i])
  colnames(cTable)[1] <- 'eu_entity_id'
  # - get item labels for current wdcm2_itemtopic_
  items <- gsub("Q", "", unique(cTable$eu_entity_id), fixed = T)
  itemList <- paste0(items, collapse = ", ", sep = "")
  # - fetch English labels where available from wb_terms
  con <- dbConnect(MySQL(), 
                   host = "wikidatawiki.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "wikidatawiki_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  q <- paste("SELECT term_entity_id, term_text FROM wb_terms ",
             "WHERE ((term_entity_id IN (", itemList, ")) AND ",
             "(term_language = 'en') AND ",
             "(term_entity_type = 'item') AND (term_type = 'label'));", 
             sep = "");
  rm(itemList)
  res <- dbSendQuery(con, q)
  itemLabels <- fetch(res, -1)
  dbClearResult(res)
  dbDisconnect(con)
  itemLabels$term_entity_id <- paste("Q", itemLabels$term_entity_id, sep = "")
  colnames(itemLabels) <- c('eu_entity_id', 'eu_label')
  # - insert labels to current wdcm2_itemtopic_
  cTable <- left_join(cTable, itemLabels, by = 'eu_entity_id')
  # - recognize missing English labels and replace w. the respective eu_entity_id value
  cTable$eu_label[is.na(cTable$eu_label)] <- 
    cTable$eu_entity_id[is.na(cTable$eu_label)]
  rm(itemLabels); gc()
  # - write current wdcm2_itemtopic_
  con <- dbConnect(MySQL(),
                   host = "tools.labsdb",
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "u16664__wdcm_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  dbWriteTable(conn = con,
               name = tName,
               value = cTable,
               row.names = F,
               append = F)
  dbDisconnect(con)
}

### --- copy toLabsReport.csv to all WDCM Dashboards:
# - to nohup.out
print(paste("copy toLabsReport.csv to all WDCM Dashboards: ", Sys.time(), sep = ""))

# - Overview Dashboard Update File
system('sudo cp /home/goransm/WMDE/WDCM/WDCM_SystemUpdate/toLabsReport.csv /srv/shiny-server/WDCM_OverviewDashboard/update/', 
       wait = F)
# - Usage Dashboard Update File
system('sudo cp /home/goransm/WMDE/WDCM/WDCM_SystemUpdate/toLabsReport.csv /srv/shiny-server/WDCM_UsageDashboard/update/', 
       wait = F)
# - Semantics Dashboard Update File
system('sudo cp /home/goransm/WMDE/WDCM/WDCM_SystemUpdate/toLabsReport.csv /srv/shiny-server/WDCM_SemanticsDashboard/update/', 
       wait = F)
# - Semantics Dashboard Data Sets
lF <- list.files()
lF <- lF[grepl("wdcm2_projecttopic_", lF, fixed = T)]
for (i in 1:length(lF)) {
  system(paste('sudo cp ', inputDir, 
               lF[i], 
               ' /srv/shiny-server/WDCM_SemanticsDashboard/data/',
               sep = ''),
         wait = F)
}
lF <- list.files()
lF <- lF[grepl("wdcm2_visNetworkNodes_project", lF, fixed = T)]
for (i in 1:length(lF)) {
  system(paste('sudo cp ', inputDir, 
               lF[i], 
               ' /srv/shiny-server/WDCM_SemanticsDashboard/data/',
               sep = ''),
         wait = F)
}
lF <- list.files()
lF <- lF[grepl("wdcm2_visNetworkEdges_project", lF, fixed = T)]
for (i in 1:length(lF)) {
  system(paste('sudo cp ', inputDir, 
               lF[i], 
               ' /srv/shiny-server/WDCM_SemanticsDashboard/data/',
               sep = ''),
         wait = F)
}

### --- Report: Update completed
# - to nohup.out
print(paste("WDCM Dashboards Report Completed: ", Sys.time(), sep = ""))



