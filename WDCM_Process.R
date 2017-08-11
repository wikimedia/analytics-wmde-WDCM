
### ---------------------------------------------------------------------------
### --- WDCM Process Module, v. Beta 0.1
### --- Script: WDCM_Process.R, v. Beta 0.1
### ---------------------------------------------------------------------------
### --- DESCRIPTION:
### --- WDCM_Process.R takes a list of ,tsv files that present
### --- the data from wbc_entity_usage tables accross the client projects
### --- fetched from production (stat1003) by WDCM_Search_Clients.R.
### --- Each files stands for (1) a particular item category from the respective
### --- WDCM Ontology, (2) a particular project, and (3) a particular batch
### --- (batches are described by the "_b1", "_b2", etc. sufix).
### --- The goal of this WDCM module/script is to produce (or update) 
### --- the WDCM Dashboards database.
### ---------------------------------------------------------------------------
### --- INPUT: 
### --- the WDCM_Process.R reads the ,tsv input files from:
### --- https://analytics.wikimedia.org/datasets/WDCM/Search_Items_DataOUT/
### --- The data are actualy in:
### --- stat1003.eqiad.wmnet:22/a/published-datasets/WDCM/Search_Items_DataOUT/
### --- on the stat1003.eqiad.wmnet production server 
### ---------------------------------------------------------------------------
### --- OUTPUT: 
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

### --- git/gerrit 08/12/2017.

### --- Setup
rm(list = ls()); gc()
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

# - mysql --defaults-file=/home/goransm/mySQL_Credentials/replica.my.cnf -h tools.labsdb
# - database: u16664__wdcm_p

### ---------------------------------------------------------------------------
### --- GET UPDATE:

# - update timestamp:
timestamp <- unlist(strsplit(as.character(Sys.time()), split = "\\s"))
timestamp[1] <- paste(strsplit(timestamp[1], split = "-")[[1]], collapse = "")
timestamp[2] <- paste(strsplit(timestamp[2], split = ":")[[1]], collapse = "")
timestamp <- paste(timestamp, collapse = "")

# - create folder for local update storage:
dataInDir <- '/home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_DataIN_ClientUsage/'
setwd(dataInDir)
dirName <- paste("update_", timestamp, sep = "")
dir.create(dirName)
# - change working dir to dirName
setwd(paste(getwd(), "/", dirName, sep = ""))

### --- Collect item usage files:
fileLoc <- "https://analytics.wikimedia.org/datasets/WDCM/Search_Items_DataOUT/"
lf <- htmltab(doc = fileLoc)
lf <- lf[-1, ]
lf <- filter(lf, !(Size ==0))

# - create file index:
indexLF <- data.frame(filename = lf$Name, stringsAsFactors = F)
indexLF$project <- sapply(indexLF$filename, function(x) {
  strsplit(x, split = "_", fixed = T)[[1]][1]
})
indexLF$category <- sapply(indexLF$filename, function(x) {
  strsplit(x, split = "_", fixed = T)[[1]][2]
})
indexLF$batch <- sapply(indexLF$filename, function(x) {
  strsplit(strsplit(x, split = "_", fixed = T)[[1]][3], split = ".", fixed = T)[[1]][1]
})

projects <- unique(indexLF$project)
categories <- unique(indexLF$category)

### --- collect update files (per WDCM Ontology category):
for (p in 1:length(projects)) {
  for (c in 1:length(categories)) {
    w <- which((indexLF$project == projects[p]) & (indexLF$category == categories[c]))
    if (length(w) > 0) {
      locLoad <- vector(mode = "list", length = length(w))
      for (l in 1:length(locLoad)) {
        tryCatch(
          {locLoad[[l]] <- read.table(paste(fileLoc, indexLF$filename[w[l]], sep = ""),
                                      sep = "\t",
                                      header = T,
                                      check.names = F,
                                      stringsAsFactors = F)
          },
          error = function(cond) {
            message(paste("The following URL cause an error:", 
                          paste(fileLoc, indexLF$filename[w[l]], sep = "")))
            message("Message:")
            message(cond)
            w <- which(lf$Name %in% indexLF$filename[w[l]])
            lf$Description[w] <- 'Error'
            return(data.frame(x = "x", y = "y", z = "z"))},
          warning = function(cond) {
            message(paste("The following URL caused a warning:", 
                          paste(fileLoc, indexLF$filename[w[l]], sep = "")))
            message("Message:")
            message(cond)
            w <- which(lf$Name %in% indexLF$filename[w[l]])
            lf$Description[w] <- 'Warning'
            return(data.frame(x = "x", y = "y", z = "z"))}        
          )
        if (length(locLoad[[l]]) == 0) {
          locLoad[[l]] <- data.frame(x = "x", y = "y", z = "z")
          w <- which(lf$Name %in% indexLF$filename[w[l]])
          lf$Description[w] <- 'Malformed'
        } else if (!(dim(locLoad[[l]])[2] == 3)) {
          locLoad[[l]] <- data.frame(x = "x", y = "y", z = "z")
          locLoad[[l]] <- data.frame(x = "x", y = "y", z = "z")
          w <- which(lf$Name %in% indexLF$filename[w[l]])
          lf$Description[w] <- 'Malformed'
        }
      }
      locLoad <- rbindlist(locLoad)
      locLoad$category <- categories[c]
      locLoad$project <- projects[p]
      locLoad$timestamp <- timestamp
      w <- which(locLoad[, 1] == 'x')
      if (length(w) > 0) {locLoad <- locLoad[-w, ]}
      # - store locally:
      write_csv(locLoad, 
                path = paste(getwd(), "/", paste(categories[c], "_", projects[p], ".csv", sep =""), sep = "")
                )
    }
  }
}
# - clear:
rm(locLoad); rm(lf); rm(indexLF); gc()


### ---------------------------------------------------------------------------
### --- CREATE TABLES

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

### --- wdcm_datatable

# - check whether wdcm_datatable table exists:
checkTable <- which(st$tables %in% "wdcm_datatable")
# - DROP wdcm_datatable if it exists
if (length(checkTable) == 1) {
  con <- dbConnect(MySQL(), 
                   host = "tools.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "u16664__wdcm_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  q <- "DROP TABLE wdcm_datatable;"
  res <- dbSendQuery(con, q)
  dbClearResult(res)
  dbDisconnect(con)
}
# - CREATE wdcm_datatable:
ccommand <- 
  'mysql --defaults-file=/home/goransm/mySQL_Credentials/replica.my.cnf -h tools.labsdb u16664__wdcm_p'
sqlFile <- '/home/goransm/WMDE/WDCM/WDCM_SQL/create_WDCM_tables.sql'
mySqlCommand <- paste(ccommand, sqlFile, sep = " < ")
system(command = mySqlCommand, wait = TRUE)
# - populate wdcm_datatable:
# - to update directory:
setwd(paste(dataInDir, dirName, sep = ""))
updates <- list.files()
# - loop over updates, ETL, store in wdcm_datatable
for (i in 1:length(updates)) {
  print(i)
  locLoad <- fread(updates[i])
  locLoad$timestamp <- NULL
  colnames(locLoad) <- c('en_id', 'en_aspect', 'en_count', 'en_category', 'en_project')
  con <- dbConnect(MySQL(), 
                   host = "tools.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "u16664__wdcm_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  dbWriteTable(conn = con,
               name = "wdcm_datatable",
               value = locLoad,
               row.names = F,
               append = T)
  dbDisconnect(con)
  rm(locLoad); gc()
}


### --- wdcm_item_project

# - check whether wdcm_item_project table exists:
checkTable <- which(st$tables %in% "wdcm_item_project")
# - DROP wdcm_item_project if it exists
if (length(checkTable) == 1) {
  con <- dbConnect(MySQL(), 
                   host = "tools.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "u16664__wdcm_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  q <- "DROP TABLE wdcm_item_project;"
  res <- dbSendQuery(con, q)
  dbClearResult(res)
  dbDisconnect(con)
}
# - populate wdcm_item_project:
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- "CREATE TABLE wdcm_item_project 
          SELECT wdcm_datatable.en_id, wdcm_datatable.en_project, SUM(wdcm_datatable.en_count) as en_count 
          FROM wdcm_datatable 
          GROUP BY wdcm_datatable.en_id, wdcm_datatable.en_project;"
res <- dbSendQuery(con, q)
dbClearResult(res)
dbDisconnect(con)
# - created indexes on wdcm_item_project:
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- "CREATE INDEX ipix_en_id ON wdcm_item_project (en_id); "
res <- dbSendQuery(con, q)
dbClearResult(res)
q <- "CREATE INDEX ipix_en_project ON wdcm_item_project (en_project);" 
res <- dbSendQuery(con, q)
dbClearResult(res)
q <- "CREATE INDEX ipix_en_id_project ON wdcm_item_project (en_id, en_project); "
res <- dbSendQuery(con, q)
dbClearResult(res)
dbDisconnect(con)

### --- wdcm_category_project

# - check whether wdcm_category_project table exists:
checkTable <- which(st$tables %in% "wdcm_category_project")
# - DROP wdcm_category_project if it exists
if (length(checkTable) == 1) {
  con <- dbConnect(MySQL(), 
                   host = "tools.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "u16664__wdcm_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  q <- "DROP TABLE wdcm_category_project;"
  res <- dbSendQuery(con, q)
  dbClearResult(res)
  dbDisconnect(con)
}
# - populate wdcm_category_project:
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- "CREATE TABLE wdcm_category_project 
        SELECT wdcm_datatable.en_category, wdcm_datatable.en_project, SUM(wdcm_datatable.en_count) as en_count 
        FROM wdcm_datatable 
        GROUP BY wdcm_datatable.en_category, wdcm_datatable.en_project;"
res <- dbSendQuery(con, q)
dbClearResult(res)
dbDisconnect(con)
# - created indexes on wdcm_category_project:
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- "CREATE INDEX cpix_en_category ON wdcm_category_project (en_category); "
res <- dbSendQuery(con, q)
dbClearResult(res)
q <- "CREATE INDEX cpix_en_project ON wdcm_category_project (en_project);" 
res <- dbSendQuery(con, q)
dbClearResult(res)
q <- "CREATE INDEX cpix_en_category_project ON wdcm_category_project (en_category, en_project); "
res <- dbSendQuery(con, q)
dbClearResult(res)
dbDisconnect(con)


### --- wdcm_project_aspect

# - check whether wdcm_project_aspect table exists:
checkTable <- which(st$tables %in% "wdcm_project_aspect")
# - DROP wdcm_category_project if it exists
if (length(checkTable) == 1) {
  con <- dbConnect(MySQL(), 
                   host = "tools.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "u16664__wdcm_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  q <- "DROP TABLE wdcm_project_aspect;"
  res <- dbSendQuery(con, q)
  dbClearResult(res)
  dbDisconnect(con)
}
# - populate wdcm_project_aspect:
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- "CREATE TABLE wdcm_project_aspect 
        SELECT wdcm_datatable.en_project, wdcm_datatable.en_aspect, SUM(wdcm_datatable.en_count) as en_count 
        FROM wdcm_datatable 
        GROUP BY wdcm_datatable.en_project, wdcm_datatable.en_aspect;"
res <- dbSendQuery(con, q)
dbClearResult(res)
dbDisconnect(con)
# - created indexes on wdcm_project_aspect:
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- "CREATE INDEX paix_en_project ON wdcm_project_aspect (en_project); "
res <- dbSendQuery(con, q)
dbClearResult(res)
q <- "CREATE INDEX paix_en_aspect ON wdcm_project_aspect (en_aspect);" 
res <- dbSendQuery(con, q)
dbClearResult(res)
q <- "CREATE INDEX paix_en_project_aspect ON wdcm_project_aspect (en_project, en_aspect); "
res <- dbSendQuery(con, q)
dbClearResult(res)
dbDisconnect(con)

### --- wdcm_category_aspect

# - check whether wdcm_category_aspect table exists:
checkTable <- which(st$tables %in% "wdcm_category_aspect")
# - DROP wdcm_category_aspect if it exists
if (length(checkTable) == 1) {
  con <- dbConnect(MySQL(), 
                   host = "tools.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "u16664__wdcm_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  q <- "DROP TABLE wdcm_category_aspect;"
  res <- dbSendQuery(con, q)
  dbClearResult(res)
  dbDisconnect(con)
}
# - populate wdcm_category_aspect:
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- "CREATE TABLE wdcm_category_aspect 
        SELECT wdcm_datatable.en_category, wdcm_datatable.en_aspect, SUM(wdcm_datatable.en_count) as en_count 
        FROM wdcm_datatable 
        GROUP BY wdcm_datatable.en_category, wdcm_datatable.en_aspect;"
res <- dbSendQuery(con, q)
dbClearResult(res)
dbDisconnect(con)
# - created indexes on wdcm_category_aspect:
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- "CREATE INDEX caix_en_category ON wdcm_category_aspect (en_category); "
res <- dbSendQuery(con, q)
dbClearResult(res)
q <- "CREATE INDEX caix_en_aspect ON wdcm_category_aspect (en_aspect);" 
res <- dbSendQuery(con, q)
dbClearResult(res)
q <- "CREATE INDEX caix_en_category_aspect ON wdcm_category_aspect (en_category, en_aspect); "
res <- dbSendQuery(con, q)
dbClearResult(res)
dbDisconnect(con)

### --- wdcm_item

# - check whether wdcm_item table exists:
checkTable <- which(st$tables %in% "wdcm_item")
# - DROP wdcm_item if it exists
if (length(checkTable) == 1) {
  con <- dbConnect(MySQL(), 
                   host = "tools.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "u16664__wdcm_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  q <- "DROP TABLE wdcm_item;"
  res <- dbSendQuery(con, q)
  dbClearResult(res)
  dbDisconnect(con)
}
# - populate wdcm_item:
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- "CREATE TABLE wdcm_item 
        SELECT wdcm_datatable.en_id, SUM(wdcm_datatable.en_count) as en_count 
        FROM wdcm_datatable 
        GROUP BY wdcm_datatable.en_id;"
res <- dbSendQuery(con, q)
dbClearResult(res)
dbDisconnect(con)
# - created indexes on wdcm_item:
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- "CREATE INDEX iix_en_category ON wdcm_item (en_id); "
res <- dbSendQuery(con, q)
dbClearResult(res)

### --- wdcm_item_aspect

# - check whether wdcm_item_aspect table exists:
checkTable <- which(st$tables %in% "wdcm_item_aspect")
# - DROP wdcm_item_aspect if it exists
if (length(checkTable) == 1) {
  con <- dbConnect(MySQL(), 
                   host = "tools.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "u16664__wdcm_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  q <- "DROP TABLE wdcm_item_aspect;"
  res <- dbSendQuery(con, q)
  dbClearResult(res)
  dbDisconnect(con)
}
# - populate wdcm_item_aspect:
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- "CREATE TABLE wdcm_item_aspect 
        SELECT wdcm_datatable.en_id, wdcm_datatable.en_aspect, SUM(wdcm_datatable.en_count) as en_count 
        FROM wdcm_datatable 
        GROUP BY wdcm_datatable.en_id, wdcm_datatable.en_aspect;"
res <- dbSendQuery(con, q)
dbClearResult(res)
dbDisconnect(con)
# - created indexes on wdcm_item_aspect:
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- "CREATE INDEX iaix_en_id ON wdcm_item_aspect (en_id);"
res <- dbSendQuery(con, q)
dbClearResult(res)
q <- "CREATE INDEX iaix_en_aspect ON wdcm_item_aspect (en_aspect);"
res <- dbSendQuery(con, q)
dbClearResult(res)
q <- "CREATE INDEX iaix_en_id_aspect ON wdcm_item_id, wdcm_item_aspect (en_id, en_aspect);"
res <- dbSendQuery(con, q)
dbClearResult(res)
dbDisconnect(con)

### --- wdcm_category_aspect_l

# - check whether wdcm_category_aspect_l table exists:
checkTable <- which(st$tables %in% "wdcm_category_aspect_l")
# - DROP wdcm_category_aspect_l if it exists
if (length(checkTable) == 1) {
  con <- dbConnect(MySQL(), 
                   host = "tools.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "u16664__wdcm_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  q <- "DROP TABLE wdcm_category_aspect_l;"
  res <- dbSendQuery(con, q)
  dbClearResult(res)
  dbDisconnect(con)
}
# - fetch wdcm_category_aspect 
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- "SELECT * FROM wdcm_category_aspect;"
res <- dbSendQuery(con, q)
wdcm_category_aspect_l <- fetch(res, -1)
dbClearResult(res)
# - produce wdcm_category_aspect_l
wdcm_category_aspect_l$en_aspect[which(grepl("L", wdcm_category_aspect_l$en_aspect))] <- "L"
wdcm_category_aspect_l <- wdcm_category_aspect_l %>% 
  group_by(en_category, en_aspect) %>% 
  summarise(en_count = sum(en_count))
# - create wdcm_category_aspect_l
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- "CREATE TABLE wdcm_category_aspect_l (
  
  -- en_category
  en_category varchar(255) NOT NULL,
  
  -- en_aspect
  en_aspect varchar(37) NOT NULL,
  
  -- count
  en_count int
  );"
res <- dbSendQuery(con, q)
dbClearResult(res)

# - write to wdcm_category_aspect_l
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
dbWriteTable(conn = con,
             name = "wdcm_category_aspect_l",
             value = wdcm_category_aspect_l,
             row.names = F,
             append = T)
dbDisconnect(con)
rm(wdcm_category_aspect_l); gc()
# - created indexes on wdcm_category_aspect_l:
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- "CREATE INDEX calix_en_id ON wdcm_category_aspect_l (en_category);"
res <- dbSendQuery(con, q)
dbClearResult(res)
q <- "CREATE INDEX calix_en_aspect ON wdcm_category_aspect_l (en_aspect);"
res <- dbSendQuery(con, q)
dbClearResult(res)
q <- "CREATE INDEX calix_en_id_aspect ON wdcm_category_aspect_l (en_category, en_aspect);"
res <- dbSendQuery(con, q)
dbClearResult(res)
dbDisconnect(con)

### --- wdcm_project_aspect_l

# - check whether wdcm_category_aspect_l table exists:
checkTable <- which(st$tables %in% "wdcm_project_aspect_l")
# - DROP wdcm_project_aspect_l if it exists
if (length(checkTable) == 1) {
  con <- dbConnect(MySQL(), 
                   host = "tools.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "u16664__wdcm_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  q <- "DROP TABLE wdcm_project_aspect_l;"
  res <- dbSendQuery(con, q)
  dbClearResult(res)
  dbDisconnect(con)
}
# - fetch wdcm_project_aspect 
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- "SELECT * FROM wdcm_project_aspect;"
res <- dbSendQuery(con, q)
wdcm_project_aspect_l <- fetch(res, -1)
dbClearResult(res)
# - produce wdcm_project_aspect_l
wdcm_project_aspect_l$en_aspect[which(grepl("L", wdcm_project_aspect_l$en_aspect))] <- "L"
wdcm_project_aspect_l <- wdcm_project_aspect_l %>% 
  group_by(en_project, en_aspect) %>% 
  summarise(en_count = sum(en_count))
# - create wdcm_project_aspect_l
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- "CREATE TABLE wdcm_project_aspect_l (

    -- en_project
    en_project varchar(255) NOT NULL,

    -- en_aspect
    en_aspect varchar(37) NOT NULL,

    -- count
    en_count int
    );"
res <- dbSendQuery(con, q)
dbClearResult(res)
# - write to wdcm_project_aspect_l
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
dbWriteTable(conn = con,
             name = "wdcm_project_aspect_l",
             value = wdcm_project_aspect_l,
             row.names = F,
             append = T)
dbDisconnect(con)
rm(wdcm_project_aspect_l); gc()
# - created indexes on wdcm_category_aspect_l:
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- "CREATE INDEX palix_en_id ON wdcm_category_aspect_l (en_project);"
res <- dbSendQuery(con, q)
dbClearResult(res)
q <- "CREATE INDEX palix_en_aspect ON wdcm_category_aspect_l (en_aspect);"
res <- dbSendQuery(con, q)
dbClearResult(res)
q <- "CREATE INDEX palix_en_id_aspect ON wdcm_category_aspect_l (en_category, en_aspect);"
res <- dbSendQuery(con, q)
dbClearResult(res)
dbDisconnect(con)

### --- wdcm_item_aspect_l

# - check whether wdcm_item_aspect_l table exists:
checkTable <- which(st$tables %in% "wdcm_item_aspect_l")
# - DROP wdcm_item_aspect_l if it exists
if (length(checkTable) == 1) {
  con <- dbConnect(MySQL(), 
                   host = "tools.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "u16664__wdcm_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  q <- "DROP TABLE wdcm_item_aspect_l;"
  res <- dbSendQuery(con, q)
  dbClearResult(res)
  dbDisconnect(con)
}
# - fetch wdcm_item_aspect 
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- "SELECT * FROM wdcm_item_aspect;"
res <- dbSendQuery(con, q)
wdcm_item_aspect_l <- fetch(res, -1)
dbClearResult(res)
# - produce wdcm_item_aspect_l
wdcm_item_aspect_l$en_aspect[which(grepl("L", wdcm_item_aspect_l$en_aspect))] <- "L"
wdcm_item_aspect_l <- wdcm_item_aspect_l %>% 
  group_by(en_id, en_aspect) %>% 
  summarise(en_count = sum(en_count))
# - create wdcm_item_aspect_l
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- "CREATE TABLE wdcm_item_aspect_l (

      -- en_id
      en_id varchar(255) NOT NULL,

      -- en_aspect
      en_aspect varchar(37) NOT NULL,
      
      -- count
      en_count int
      );"
res <- dbSendQuery(con, q)
dbClearResult(res)
dbDisconnect(con)
# - write to wdcm_item_aspect_l
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
dbWriteTable(conn = con,
             name = "wdcm_item_aspect_l",
             value = wdcm_item_aspect_l,
             row.names = F,
             append = T)
dbDisconnect(con)
# - created indexes on wdcm_item_aspect_l:
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- "CREATE INDEX ialix_en_id ON wdcm_item_aspect_l (en_id);"
res <- dbSendQuery(con, q)
dbClearResult(res)
q <- "CREATE INDEX ialix_en_aspect ON wdcm_item_aspect_l (en_aspect);"
res <- dbSendQuery(con, q)
dbClearResult(res)
q <- "CREATE INDEX ialix_en_id_aspect ON wdcm_item_aspect_l (en_id, en_aspect);"
res <- dbSendQuery(con, q)
dbClearResult(res)
dbDisconnect(con)
rm(wdcm_item_aspect_l); gc()

### ---------------------------------------------------------------------------
### --- GET ITEM LABELS

### --- wdcm_itemlabel table

# - check whether wdcm_itemlabel table exists:
checkTable <- which(st$tables %in% "wdcm_itemlabel")
# - DROP wdcm_itemlabel if it exists
if (length(checkTable) == 1) {
  con <- dbConnect(MySQL(), 
                   host = "tools.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "u16664__wdcm_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  q <- "DROP TABLE wdcm_itemlabel;"
  res <- dbSendQuery(con, q)
  dbClearResult(res)
  dbDisconnect(con)
}
# - create itemlabel table
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- "CREATE TABLE wdcm_itemlabel (
  
  -- item ID
  en_id varchar(255) NOT NULL,
  
  -- item label
  en_label varchar(255) NOT NULL
  );"
res <- dbSendQuery(con, q)
dbClearResult(res)
dbDisconnect(con)
# - fetch wdcm_item_aspect 
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- "SELECT * FROM wdcm_item_aspect;"
res <- dbSendQuery(con, q)
wdcm_item_aspect <- fetch(res, -1)
dbClearResult(res)
dbDisconnect(con)
# - find the most common language label for each item:
uniqueItems <- unique(wdcm_item_aspect$en_id)
wdcm_item_aspect <- filter(wdcm_item_aspect, grepl("L.", en_aspect, fixed = T))
uniqueItems <- setdiff(uniqueItems, unique(wdcm_item_aspect$en_id))
wdcm_item_aspect <- wdcm_item_aspect %>% 
  group_by(en_id) %>% 
  slice(which.max(en_count)[1])
wdcm_item_aspect$en_count <- NULL
wdcm_item_aspect$en_aspect <- str_sub(wdcm_item_aspect$en_aspect, 3, -1)
uniqueItems <- data.frame(en_id = uniqueItems,
                          en_aspect = "en",
                          stringsAsFactors = F)
wdcm_item_aspect <- rbindlist(l = list(wdcm_item_aspect, uniqueItems))
rm(uniqueItems); gc()
wdcm_item_aspect <- arrange(wdcm_item_aspect, en_aspect)
# - access wikidatawiki cluster, wb_terms table, and fetch item labels
lan <- unique(wdcm_item_aspect$en_aspect)
for (i in 1:length(lan)) {
  itemList <- gsub('Q', '', 
                   wdcm_item_aspect$en_id[which(wdcm_item_aspect$en_aspect %in% lan[i])],
                   fixed = T)
  if (length(itemList) >= 1000000) {
    # - cut into 5 batches:
    batchSize <- floor(length(itemList)/5)
    startBatchIx <- c(1:5) * batchSize - batchSize + 1
    stopBatchIx <- c(1:5) * batchSize
    stopBatchIx[5] <- length(itemList)
    for (b in 1:length(startBatchIx)) {
      # - select batch:
      itemBatch <- paste0(itemList[startBatchIx[b]:stopBatchIx[b]], collapse = ", ", sep = "")
      # access wb_terms from wikidatawiki
      con <- dbConnect(MySQL(), 
                       host = "wikidatawiki.labsdb", 
                       defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                       dbname = "wikidatawiki_p",
                       user = mySQLCreds$user,
                       password = mySQLCreds$password)
      q <- paste("SELECT term_entity_id, term_text FROM wb_terms ",
                 "WHERE ((term_entity_id IN (", itemBatch, ")) AND ",
                 "(term_language = '", lan[i], "') AND ",
                 "(term_entity_type = 'item') AND (term_type = 'label'));", 
                 sep = "");
      res <- dbSendQuery(con, q)
      batchLabels <- fetch(res, -1)
      dbClearResult(res)
      dbDisconnect(con)
      # - delete duplicates occuring in wb_terms
      wNDup <- which(!(duplicated(batchLabels$term_entity_id)))
      batchLabels <- batchLabels[wNDup, ]
      # - process and store:
      itemBatch <- strsplit(itemBatch, split = ", ", fixed = T)[[1]]
      w <- which(!(itemBatch %in% batchLabels$term_entity_id))
      if (length(w) > 0) {
        restItem <- data.frame(term_entity_id = itemBatch[w], 
                               term_text = paste("Q", itemBatch[w], sep=""), 
                               stringsAsFactors = F)
        batchLabels <- rbindlist(l = list(batchLabels, restItem))
      }
      batchLabels$term_entity_id <- paste("Q", batchLabels$term_entity_id, sep="")
      colnames(batchLabels) <- c('en_id', 'en_label')
      con <- dbConnect(MySQL(), 
                       host = "tools.labsdb", 
                       defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                       dbname = "u16664__wdcm_p",
                       user = mySQLCreds$user,
                       password = mySQLCreds$password)
      dbWriteTable(conn = con,
                   name = "wdcm_itemlabel",
                   value = batchLabels,
                   row.names = F,
                   append = T)
      dbDisconnect(con)
    }
  } else {
    # - select items:
    itemBatch <- paste0(itemList, collapse = ", ", sep = "")
    # access wb_terms from wikidatawiki
    con <- dbConnect(MySQL(), 
                     host = "wikidatawiki.labsdb", 
                     defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                     dbname = "wikidatawiki_p",
                     user = mySQLCreds$user,
                     password = mySQLCreds$password)
    q <- paste("SELECT term_entity_id, term_text FROM wb_terms ",
               "WHERE ((term_entity_id IN (", itemBatch, ")) AND ",
               "(term_language = '", lan[i], "') AND ",
               "(term_entity_type = 'item') AND (term_type = 'label'));", 
               sep = "");
    res <- dbSendQuery(con, q)
    batchLabels <- fetch(res, -1)
    dbClearResult(res)
    dbDisconnect(con)
    # - delete duplicates occuring in wb_terms
    wNDup <- which(!(duplicated(batchLabels$term_entity_id)))
    batchLabels <- batchLabels[wNDup, ]
    # - process and store:
    itemBatch <- strsplit(itemBatch, split = ", ", fixed = T)[[1]]
    w <- which(!(itemBatch %in% batchLabels$term_entity_id))
    if (length(w) > 0) {
      restItem <- data.frame(term_entity_id = itemBatch[w], 
                             term_text = paste("Q", itemBatch[w], sep=""), 
                             stringsAsFactors = F)
      batchLabels <- rbindlist(l = list(batchLabels, restItem))
    }
    batchLabels$term_entity_id <- paste("Q", batchLabels$term_entity_id, sep="")
    colnames(batchLabels) <- c('en_id', 'en_label')
    con <- dbConnect(MySQL(), 
                     host = "tools.labsdb", 
                     defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                     dbname = "u16664__wdcm_p",
                     user = mySQLCreds$user,
                     password = mySQLCreds$password)
    dbWriteTable(conn = con,
                 name = "wdcm_itemlabel",
                 value = batchLabels,
                 row.names = F,
                 append = T)
    dbDisconnect(con)
  }
  
}
# - clean-up
rm(wdcm_item_aspect); gc()


### ---------------------------------------------------------------------------
### --- TOPIC MODELS: items x projects in each semantic category
# - get categories:
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- "SELECT DISTINCT(en_category) FROM wdcm_category_project;"
res <- dbSendQuery(con, q)
categories <- fetch(res, -1)
dbClearResult(res)
dbDisconnect(con)
# - loop over categories:
for (i in 1:length(categories$en_category)) {
  
  # - get per item and per project counts in category i:
  con <- dbConnect(MySQL(), 
                   host = "tools.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "u16664__wdcm_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  q <- paste("SELECT en_id, en_project, SUM(en_count) AS en_freq FROM wdcm_datatable ",
             "WHERE en_category = '",
             categories$en_category[i],
             "' GROUP BY en_id, en_project;",
             sep = "")
  res <- dbSendQuery(con, q)
  itemCat <- fetch(res, -1)
  dbClearResult(res)
  dbDisconnect(con)
  
  # - wrangle for tf-idf application:
  itemCat <- spread(itemCat,
                    key = en_project,
                    value = en_freq,
                    fill = 0)
  rownames(itemCat) <- itemCat$en_id
  itemCat$en_id <- NULL
  
  # - apply tf-idf
  # - eliminate empty projects, if any:
  emptyProj <- which(colSums(itemCat) == 0)
  if (length(emptyProj) > 0) {
    itemCat[, -emptyProj]
  }
  numDocs <- dim(itemCat)[2]
  # - sort wdcm_itemProject by tf-idf and select vocabulary:
  sfInit(parallel = T, cpus = 8, type = "SOCK")
  sfExport('itemCat', 'numDocs')
  tfIdf <- sfApply(itemCat, 1, function(x) {
    sum(x*log(numDocs/sum(x > 0)))
  })
  sfStop()
  itemCat$tfIdf <- tfIdf
  rm(tfIdf); gc()
  itemCat <- itemCat[order(-itemCat$tfIdf), ]
  
  # - select top 100,000 tf-idf items - or how many are available 
  if (dim(itemCat)[1] >= 100000) {
    itemCat <- itemCat[1:100000, ]
  }
  itemCat$tfIdf <- NULL
  
  # - transpose itemCat:
  itemCat <- t(itemCat)
  
  # - get vocabulary item labels:
  con <- dbConnect(MySQL(), 
                   host = "tools.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "u16664__wdcm_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  q <- paste("SELECT en_id, en_label FROM wdcm_itemlabel ", 
             "WHERE en_id IN (", 
             paste("'", colnames(itemCat), "'", collapse = ", ", sep = ""),
             ");",
             sep = "")
  rs <- dbSendQuery(con, 'set character set "utf8"')
  res <- dbSendQuery(con, q)
  itemLabel <- fetch(res, -1)
  dbClearResult(res)
  dbDisconnect(con)
  # - match labels from itemLabel and itemCat
  sfInit(parallel = T, cpus = 8, type = "SOCK")
  sfExport('itemCat', 'itemLabel')
  matchItemLabel <- sfLapply(itemLabel$en_id, function(x) {
    which(colnames(itemCat) %in% x)
  })
  sfStop()
  matchItemLabel <- unlist(matchItemLabel)
  # - before match, check whether there are any duplicates in itemLabel$en_label
  # - NOTE: duplicated labels - not descriptions - are legitimate in Wikidata
  # - use make.unique() to create valid columnames
  itemLabel$en_label <- make.unique(itemLabel$en_label)
  # - now match itemCat and itemLabel$en_label
  colnames(itemCat)[matchItemLabel] <- itemLabel$en_label
  item_en_ids <- itemLabel$en_id
  rm(itemLabel); gc()
  
  # - topic modeling:
  itemCat <- as.simple_triplet_matrix(itemCat)
  # - topic modeling w. {maptpx}
  # - run on K = seq(2,20) semantic topics:
  numTopics <- 2:20
  topicModel <- maptpx::topics(itemCat,
                               K = numTopics,
                               shape = NULL,
                               initopics = NULL,
                               tol = 0.01,
                               bf = T, 
                               kill = 4,
                               ord = TRUE,
                               verb = 2)
  rm(itemCat); gc()
  
  ### --- topic model tables
  
  wdcm_itemtopic <- as.data.frame(topicModel$theta)
  colnames(wdcm_itemtopic) <- paste("topic", seq(1, dim(wdcm_itemtopic)[2]), sep = "")
  wdcm_itemtopic$en_id[matchItemLabel] <- item_en_ids
  wdcm_itemtopic$en_label <- rownames(wdcm_itemtopic)
  
  # - check whether paste('wdcm_itemtopic', categories$en_category[i], sep = "_") table exists:
  # - create itemTopicFileName
  itemTopicFileName <- paste('wdcm_itemtopic', categories$en_category[i], sep = "_")
  checkTable <- which(st$tables %in% itemTopicFileName)
  # - DROP wdcm_itemtopic if it exists
  if (length(checkTable) == 1) {
    con <- dbConnect(MySQL(), 
                     host = "tools.labsdb", 
                     defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                     dbname = "u16664__wdcm_p",
                     user = mySQLCreds$user,
                     password = mySQLCreds$password)
    ### --- index names:
    ixNames <- paste(itemTopicFileName, c('en_id', 'en_label'), sep = "_")
    q <- paste("ALTER TABLE ",
               itemTopicFileName, 
               " DROP INDEX ",
               ixNames[1],
               ", DROP INDEX ",
               ixNames[2],
               ";",
               sep = "");
    res <- dbSendQuery(con, q)
    dbClearResult(res)
    q <- paste("DROP TABLE ", itemTopicFileName, ";", sep = "")
    res <- dbSendQuery(con, q)
    dbClearResult(res)
    dbDisconnect(con)
  }
  # - create itemTopicFileName table:
  con <- dbConnect(MySQL(), 
                   host = "tools.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "u16664__wdcm_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  q <- paste(
    "CREATE TABLE ", itemTopicFileName," (",
    paste(colnames(wdcm_itemtopic[1:(dim(wdcm_itemtopic)[2]-2)]), 
          " DOUBLE PRECISION, ", 
          sep = "", 
          collapse = ""),
    "en_id varchar(255) NOT NULL, en_label varchar(255));",
    sep = ""
  )
  res <- dbSendQuery(con, q)
  dbClearResult(res)
  dbDisconnect(con)
  # - created indexes on itemTopicFileName:
  ### --- index names:
  ixNames <- paste(itemTopicFileName, c('en_id', 'en_label'), sep = "_")
  con <- dbConnect(MySQL(), 
                   host = "tools.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "u16664__wdcm_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  q <- paste("CREATE INDEX ", ixNames[1], " ON ", itemTopicFileName, " (en_id); ", sep = "")
  res <- dbSendQuery(con, q)
  dbClearResult(res)
  q <- paste("CREATE INDEX ", ixNames[2], " ON ", itemTopicFileName, " (en_label); ", sep = "")
  res <- dbSendQuery(con, q)
  dbClearResult(res)
  dbDisconnect(con)
  # - populate itemTopicFileName:
  con <- dbConnect(MySQL(), 
                   host = "tools.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "u16664__wdcm_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  dbWriteTable(conn = con,
               name = itemTopicFileName,
               value = wdcm_itemtopic,
               row.names = F,
               append = T)
  dbDisconnect(con)
  rm(wdcm_itemtopic); gc()
  
  ### --- wdcm_projecttopic
  projectTopicFileName <- paste('wdcm_projecttopic', categories$en_category[i], sep = "_")
  wdcm_projecttopic <- as.data.frame(topicModel$omega)
  colnames(wdcm_projecttopic) <- paste("topic", seq(1, dim(wdcm_projecttopic)[2]), sep = "")
  wdcm_projecttopic$project <- rownames(wdcm_projecttopic)
  # - check whether wdcm_projecttopic table exists:
  checkTable <- which(st$tables %in% projectTopicFileName)
  # - DROP wdcm_projecttopic if it exists
  if (length(checkTable) == 1) {
    con <- dbConnect(MySQL(), 
                     host = "tools.labsdb", 
                     defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                     dbname = "u16664__wdcm_p",
                     user = mySQLCreds$user,
                     password = mySQLCreds$password)
    ### --- index names:
    ixNames <- paste(projectTopicFileName, 'ptix_en_id', sep = "_")
    q <- paste("ALTER TABLE ",
               projectTopicFileName, 
               " DROP INDEX ",
               ixNames[1],
               ";",
               sep = "");
    res <- dbSendQuery(con, q)
    dbClearResult(res)
    q <- paste("DROP TABLE ", projectTopicFileName, ";", sep = "")
    res <- dbSendQuery(con, q)
    dbClearResult(res)
    dbDisconnect(con)
  }
  # - create wdcm_projecttopic:
  con <- dbConnect(MySQL(), 
                   host = "tools.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "u16664__wdcm_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  q <- paste(
    "CREATE TABLE ", projectTopicFileName, " (",
    paste(colnames(wdcm_projecttopic[1:(dim(wdcm_projecttopic)[2]-1)]), 
          " DOUBLE PRECISION, ", 
          sep = "", 
          collapse = ""),
    "project varchar(255) NOT NULL);",
    sep = ""
  )
  res <- dbSendQuery(con, q)
  dbClearResult(res)
  dbDisconnect(con)
  # - created indexes on wdcm_projecttopic:
  ### --- index names:
  ixNames <- paste(projectTopicFileName, 'ptix_en_id', sep = "_")
  con <- dbConnect(MySQL(), 
                   host = "tools.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "u16664__wdcm_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  q <- paste("CREATE INDEX ", ixNames, " ON ", projectTopicFileName, " (project); ", sep = "")
  res <- dbSendQuery(con, q)
  dbClearResult(res)
  dbDisconnect(con)
  # - populate wdcm_projecttopic:
  con <- dbConnect(MySQL(), 
                   host = "tools.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "u16664__wdcm_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  dbWriteTable(conn = con,
               name = projectTopicFileName,
               value = wdcm_projecttopic,
               row.names = F,
               append = T)
  dbDisconnect(con)
  rm(wdcm_projecttopic); gc()
  
}

### --- write out wdcm_itemproject_* and wdcm_projecttopic_* tables
### --- to /home/goransm/WMDE/WDCM/WDCM_RScripts/WDCM_Dashboard/data
### --- to be used locally from the Dashboard by fread()
# - get table names
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- "SHOW TABLES;"
res <- dbSendQuery(con, q)
tableNames <- fetch(res, -1)
dbClearResult(res)
dbDisconnect(con)
# - select tables
itemT <- which(grepl("wdcm_itemtopic_", tableNames$Tables_in_u16664__wdcm_p, fixed = T))
projT <- which(grepl("wdcm_projecttopic_", tableNames$Tables_in_u16664__wdcm_p, fixed = T))
tableNames <- tableNames$Tables_in_u16664__wdcm_p[c(itemT, projT)]
# - to /home/goransm/WMDE/WDCM/WDCM_RScripts/WDCM_Dashboard/data
setwd('/home/goransm/WMDE/WDCM/WDCM_RScripts/WDCM_Dashboard/data')
for (i in 1:length(tableNames)) {
  con <- dbConnect(MySQL(), 
                   host = "tools.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "u16664__wdcm_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  rs <- dbSendQuery(con, 'set character set "utf8"')
  q <- paste("SELECT * FROM ",
             tableNames[i], 
             ";", 
             sep = "")
  res <- dbSendQuery(con, q)
  dataSet <- fetch(res, -1)
  dbClearResult(res)
  dbDisconnect(con)
  write.csv(dataSet, file = paste(tableNames[i], ".csv", sep = ""))
  rm(dataSet); gc()
}
# - back to: setwd('/home/goransm/WMDE/WDCM/WDCM_RScripts')
setwd('/home/goransm/WMDE/WDCM/WDCM_RScripts')




###############################################################
### - Old Model STARTS here(!)
# - get counts per item
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- "SELECT * FROM wdcm_item;"
res <- dbSendQuery(con, q)
wdcm_item <- fetch(res, -1)
dbClearResult(res)
dbDisconnect(con)
# - select items w. frequency >= 10
items <- wdcm_item$en_id[wdcm_item$en_count >= 10]
rm(wdcm_item); gc()
# - get wdcm_item_project
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- paste("SELECT * FROM wdcm_item_project ", 
           "WHERE en_id IN (", 
           paste("'", items, "'", collapse = ", ", sep = ""),
           ");",
           sep = "")
res <- dbSendQuery(con, q)
wdcm_itemProject <- fetch(res, -1)
dbClearResult(res)
dbDisconnect(con)
rm(items); gc()
wdcm_itemProject <- spread(wdcm_itemProject, 
                           key = en_project,
                           value = en_count,
                           fill = 0)
rownames(wdcm_itemProject) <- wdcm_itemProject$en_id
wdcm_itemProject$en_id <- NULL
numDocs <- dim(wdcm_itemProject)[2]
# - eliminate empty projects, if any:
emptyProj <- which(colSums(wdcm_itemProject) == 0)
if (length(emptyProj) > 0) {
  wdcm_itemProject[, -emptyProj]
}

# - sort wdcm_itemProject by tf-idf and select vocabulary:
sfInit(parallel = T, cpus = 8, type = "SOCK")
sfExport('wdcm_itemProject', 'numDocs')
tfIdf <- sfApply(wdcm_itemProject, 1, function(x) {
  sum(x*log(numDocs/sum(x > 0)))
})
sfStop()
wdcm_itemProject$tfIdf <- tfIdf
rm(tfIdf); gc()
wdcm_itemProject <- wdcm_itemProject[order(-wdcm_itemProject$tfIdf), ]
# - use top 100,000 tf-idf scored items as vocabulary:
wdcm_itemProject <- wdcm_itemProject[1:100000, ]
wdcm_itemProject$tfIdf <- NULL

# - transpose wdcm_itemProject
wdcm_itemProject <- t(wdcm_itemProject)

# - get vocabulary item labels:
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- paste("SELECT en_id, en_label FROM wdcm_itemlabel ", 
           "WHERE en_id IN (", 
           paste("'", colnames(wdcm_itemProject), "'", collapse = ", ", sep = ""),
           ");",
           sep = "")
rs <- dbSendQuery(con, 'set character set "utf8"')
res <- dbSendQuery(con, q)
itemLabel <- fetch(res, -1)
dbClearResult(res)
dbDisconnect(con)
# - match labels from itemLabel and wdcm_itemProject
sfInit(parallel = T, cpus = 8, type = "SOCK")
sfExport('wdcm_itemProject', 'itemLabel')
matchItemLabel <- sfLapply(itemLabel$en_id, function(x) {
  which(colnames(wdcm_itemProject) %in% x)
})
sfStop()
matchItemLabel <- unlist(matchItemLabel)
# - before match, check whether there are any duplicates in itemLabel$en_label
# - NOTE: duplicated labels - not descriptions - are legitimate in Wikidata
# - use make.unique() to create valid columnames
itemLabel$en_label <- make.unique(itemLabel$en_label)
# - now match wdcm_itemProject and itemLabel$en_label
colnames(wdcm_itemProject)[matchItemLabel] <- itemLabel$en_label
item_en_ids <- itemLabel$en_id
rm(itemLabel); gc()
# - topic modeling w. {maptpx}
wdcm_itemProject <- as.simple_triplet_matrix(wdcm_itemProject)
# - topic modeling w. {maptpx}
# - run on K = seq(2,20) semantic topics:
numTopics <- 2:20
topicModel <- maptpx::topics(wdcm_itemProject,
                             K = numTopics,
                             shape = NULL,
                             initopics = NULL,
                             tol = 0.01,
                             bf = T, 
                             kill = 4,
                             ord = TRUE,
                             verb = 2)
rm(wdcm_itemProject); gc()

### --- topic model tables



### --- wdcm_itemtopic

wdcm_itemtopic <- as.data.frame(topicModel$theta)
colnames(wdcm_itemtopic) <- paste("topic", seq(1, dim(wdcm_itemtopic)[2]), sep = "")
wdcm_itemtopic$en_id[matchItemLabel] <- item_en_ids
wdcm_itemtopic$en_label <- rownames(wdcm_itemtopic)

# - check whether wdcm_itemtopic table exists:
checkTable <- which(st$tables %in% "wdcm_itemtopic")
# - DROP wdcm_itemtopic if it exists
if (length(checkTable) == 1) {
  con <- dbConnect(MySQL(), 
                   host = "tools.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "u16664__wdcm_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  q <- "ALTER TABLE wdcm_itemtopic DROP INDEX itix_en_id, DROP INDEX itix_en_label;"
  res <- dbSendQuery(con, q)
  dbClearResult(res)
  q <- "DROP TABLE wdcm_itemtopic;"
  res <- dbSendQuery(con, q)
  dbClearResult(res)
  dbDisconnect(con)
}
# - create wdcm_itemtopic:
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- paste(
  "CREATE TABLE wdcm_itemtopic(",
  paste(colnames(wdcm_itemtopic[1:(dim(wdcm_itemtopic)[2]-2)]), 
        " DOUBLE PRECISION, ", 
        sep = "", 
        collapse = ""),
  "en_id varchar(255) NOT NULL, en_label varchar(255));",
  sep = ""
)
res <- dbSendQuery(con, q)
dbClearResult(res)
dbDisconnect(con)
# - created indexes on wdcm_itemtopic:
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- "CREATE INDEX itix_en_id ON wdcm_itemtopic (en_id); "
res <- dbSendQuery(con, q)
dbClearResult(res)
q <- "CREATE INDEX itix_en_label ON wdcm_itemtopic (en_label);" 
res <- dbSendQuery(con, q)
dbClearResult(res)
dbDisconnect(con)
# - populate wdcm_itemtopic:
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
dbWriteTable(conn = con,
             name = "wdcm_itemtopic",
             value = wdcm_itemtopic,
             row.names = F,
             append = T)
dbDisconnect(con)
rm(wdcm_itemtopic); gc()


### --- wdcm_projecttopic

wdcm_projecttopic <- as.data.frame(topicModel$omega)
colnames(wdcm_projecttopic) <- paste("topic", seq(1, dim(wdcm_projecttopic)[2]), sep = "")
wdcm_projecttopic$project <- rownames(wdcm_projecttopic)
# - check whether wdcm_projecttopic table exists:
checkTable <- which(st$tables %in% "wdcm_projecttopic")
# - DROP wdcm_projecttopic if it exists
if (length(checkTable) == 1) {
  con <- dbConnect(MySQL(), 
                   host = "tools.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "u16664__wdcm_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  q <- "DROP TABLE wdcm_projecttopic;"
  res <- dbSendQuery(con, q)
  dbClearResult(res)
  dbDisconnect(con)
}
# - create wdcm_projecttopic:
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- paste(
  "CREATE TABLE wdcm_projecttopic(",
  paste(colnames(wdcm_projecttopic[1:(dim(wdcm_projecttopic)[2]-1)]), 
        " DOUBLE PRECISION, ", 
        sep = "", 
        collapse = ""),
  "project varchar(255) NOT NULL);",
  sep = ""
)
res <- dbSendQuery(con, q)
dbClearResult(res)
dbDisconnect(con)
# - created indexes on wdcm_projecttopic:
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- "CREATE INDEX ptix_en_id ON wdcm_projecttopic (project); "
res <- dbSendQuery(con, q)
dbClearResult(res)
dbDisconnect(con)
# - populate wdcm_projecttopic:
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
dbWriteTable(conn = con,
             name = "wdcm_projecttopic",
             value = wdcm_projecttopic,
             row.names = F,
             append = T)
dbDisconnect(con)
rm(wdcm_projecttopic); gc()

### --- wdcm_project_category_aspect

# - check whether wdcm_project_category_aspect table exists:
checkTable <- which(st$tables %in% "wdcm_project_category_aspect")
# - DROP wdcm_project_category_aspect if it exists
if (length(checkTable) == 1) {
  con <- dbConnect(MySQL(), 
                   host = "tools.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "u16664__wdcm_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  q <- "DROP TABLE wdcm_project_category_aspect;"
  res <- dbSendQuery(con, q)
  dbClearResult(res)
  dbDisconnect(con)
}
# - create wdcm_project_category_aspect:
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- "CREATE TABLE wdcm_project_category_aspect 
          SELECT wdcm_datatable.en_project, wdcm_datatable.en_category, wdcm_datatable.en_aspect, SUM(wdcm_datatable.en_count) as en_count 
          FROM wdcm_datatable 
          GROUP BY wdcm_datatable.en_project, wdcm_datatable.en_category, wdcm_datatable.en_aspect;"
res <- dbSendQuery(con, q)
dbClearResult(res)
dbDisconnect()
# - created indexes on wdcm_project_category_aspect:
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- "CREATE INDEX pcaix_ ON wdcm_project_category_aspect (en_project, en_category, en_aspect); "
res <- dbSendQuery(con, q)
dbClearResult(res)
dbDisconnect(con)

### --- Current Statistics
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- "SELECT COUNT(DISTINCT(en_id)) as numitem, 
          COUNT(DISTINCT(en_category)) as numcategory,
          COUNT(DISTINCT(en_project)) as numproject,
          COUNT(DISTINCT(en_aspect)) as numaspect
          FROM wdcm_datatable;"
res <- dbSendQuery(con, q)
currentStats <- fetch(res, -1)
dbClearResult(res)
dbDisconnect(con)
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- "SELECT * FROM wdcm_projecttopic LIMIT 1;"
res <- dbSendQuery(con, q)
topicsInfo <- fetch(res, -1)
dbClearResult(res)
dbDisconnect(con)
currentStats$numtopic <- sum(grepl("^topic", colnames(topicsInfo)))
setwd('/home/goransm/WMDE/WDCM/WDCM_RScripts/WDCM_Dashboard/aux')
write.csv(currentStats, file = 'currentStats.csv')


### --- wdcm_itemfrequency table

### --- Most frequently used Wikidata items
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
q <- "SELECT * FROM wdcm_item;"
res <- dbSendQuery(con, q)
itemCounts <- fetch(res, -1)
q <- "SELECT * FROM wdcm_itemlabel;"
res <- dbSendQuery(con, q)
itemLabels <- fetch(res, -1)
dbClearResult(res)
dbDisconnect(con)
# - join
itemCounts <- itemCounts %>% 
  left_join(itemLabels, by = 'en_id') %>% 
  arrange(desc(en_count))
rm(itemLabels); gc()
# write to: wdcm_itemfrequency
# - check whether wdcm_itemfrequency table exists:
checkTable <- which(st$tables %in% "wdcm_itemfrequency")
# - DROP wdcm_itemfrequency if it exists
if (length(checkTable) == 1) {
  con <- dbConnect(MySQL(), 
                   host = "tools.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "u16664__wdcm_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  q <- "DROP TABLE wdcm_itemfrequency;"
  res <- dbSendQuery(con, q)
  dbClearResult(res)
  dbDisconnect(con)
}
# - CREATE:
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
dbWriteTable(conn = con,
             name = "wdcm_itemfrequency",
             value = itemCounts,
             row.names = F,
             append = T)
dbDisconnect(con)
rm(itemCounts); gc()

### --- Most important items per Topic, N = 100
topicNames <- paste0("topic", 1:currentStats$numtopic)
topicData <- vector(mode = "list", length = length(topicNames))
for (i in 1:length(topicNames)) {
  # - fetch data for topic i:
  con <- dbConnect(MySQL(), 
                   host = "tools.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "u16664__wdcm_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  # - create query:
  q <- paste(
    "SELECT en_label, en_id, ", topicNames[i], " FROM wdcm_itemtopic ",
    "ORDER BY ", topicNames[i], " DESC ",
    "LIMIT 100;",
    sep = ""
  )
  # - fetch:
  res <- dbSendQuery(con, q)
  topicData[[i]] <- fetch(res, -1)
  colnames(topicData[[i]])[1] <- paste(topicNames[i], "_labs", sep = "") 
  colnames(topicData[[i]])[2] <- paste(topicNames[i], "_id", sep = "") 
  dbClearResult(res)
  dbDisconnect(con)
}
# -write topicData as wdcm_topic100items:
wdcm_topic100items <- do.call(cbind, topicData)
rm(topicData)
# - check whether wdcm_topic100items table exists:
checkTable <- which(st$tables %in% "wdcm_topic100items")
# - DROP wdcm_topic100items if it exists
if (length(checkTable) == 1) {
  con <- dbConnect(MySQL(), 
                   host = "tools.labsdb", 
                   defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                   dbname = "u16664__wdcm_p",
                   user = mySQLCreds$user,
                   password = mySQLCreds$password)
  q <- "DROP TABLE wdcm_topic100items;"
  res <- dbSendQuery(con, q)
  dbClearResult(res)
  dbDisconnect(con)
}
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)
dbWriteTable(conn = con,
             name = "wdcm_topic100items",
             value = wdcm_topic100items,
             row.names = F,
             append = T)
dbDisconnect(con)
rm(wdcm_topic100items); gc()


