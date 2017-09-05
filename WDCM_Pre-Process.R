
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
### --- Results are stored locally as .tsv files on production,
### --- with each file encompassing the data for one client project.
### --- The outputs are stored locally on stat1005.eqiad.wmnet in:
### --- /home/goransm/WDCM_DataOUT/WDCM_DataOUT_ClientWDUsage
### --- These output .tsv files migrate to Labs:
### --- wikidataconcepts.wmflabs.org Cloud VPS instance
### --- where they are then processed by the WDCM Process Module.
### ---------------------------------------------------------------------------
### --- RUN:
### --- nohup Rscript /home/goransm/RScripts/WDCM_R/WDCM_Pre-Process.R &
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
# - wrangling:
library(data.table)
library(tidyr)
library(stringr)
# - modeling
library(maptpx)
library(Rtsne)
library(proxy)
# - directories:
dataInputDir <- '/home/goransm/WDCM_DataIN/WDCM_DataIN_SearchItems/'
dataDir <- '/home/goransm/WDCM_DataOUT/WDCM_DataOUT_ClientWDUsage/'

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
setwd(dataInputDir)
idFiles <- list.files()
idFiles <- idFiles[grepl(".csv$", idFiles)]
categories <- unname(sapply(idFiles, function(x) {
  strsplit(x, split = "_")[[1]][1]
}))

for (i in 1:length(categories)) {
  filename <- paste("wdcm_item_", gsub(" ", "", categories[i], fixed = T), ".tsv", sep = "")
  hiveQLquery <- paste(
    'USE goransm; SELECT eu_entity_id, SUM(eu_count) AS eu_count FROM wdcm_maintable WHERE category=\\"',
    categories[i],
    '\\" GROUP BY eu_entity_id ORDER BY eu_count DESC LIMIT 10000000;',
    sep = "")

  system(command = paste('beeline -e "',
                         hiveQLquery,
                         '" > /home/goransm/WDCM_DataOUT/WDCM_DataOUT_ClientWDUsage/',
                         filename,
                         sep = ""),
         wait = TRUE)
}


### --- everything else -> dataDir
setwd(dataDir)

### --- produce wdcm_project_category.tsv from wdcm_maintable (hdfs, database: goransm)
hiveQLquery <- 'USE goransm;
                SET hive.mapred.mode=unstrict;
                SELECT eu_project, category, SUM(eu_count) AS eu_count
                  FROM wdcm_maintable
                  GROUP BY eu_project, category ORDER BY eu_count DESC LIMIT 10000000;'
system(command = paste('beeline -e "',
                       hiveQLquery,
                       '" > /home/goransm/WDCM_DataOUT/WDCM_DataOUT_ClientWDUsage/wdcm_project_category.tsv',
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
system(command = paste('beeline -e "',
                       hiveQLquery,
                       '" > /home/goransm/WDCM_DataOUT/WDCM_DataOUT_ClientWDUsage/wdcm_project.tsv',
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
system(command = paste('beeline -e "',
                       hiveQLquery,
                       '" > /home/goransm/WDCM_DataOUT/WDCM_DataOUT_ClientWDUsage/wdcm_category.tsv',
                       sep = ""),
       wait = TRUE)
# - save wdcm_category.tsv as .csv
wdcm_category <- as.data.frame(fread('wdcm_category.tsv'))
write.csv(wdcm_category, "wdcm_category.csv")

### --- produce wdcm_project_category_item100.tsv from wdcm_maintable (hdfs, database: goransm)
hiveQLquery <- 'USE goransm; SET hive.mapred.mode=unstrict;
                 SELECT * FROM (
                   SELECT eu_project, category, eu_entity_id, eu_count, ROW_NUMBER() OVER (PARTITION BY eu_project, category ORDER BY eu_count DESC) AS row
                   FROM wdcm_maintable) t
                   WHERE row <= 100;'
system(command = paste('beeline -e "',
                        hiveQLquery,
                        '" > /home/goransm/WDCM_DataOUT/WDCM_DataOUT_ClientWDUsage/wdcm_project_category_item100.tsv',
                        sep = ""),
        wait = TRUE)
# - add projecttype to wdcm_project_category_item100.tsv
wdcm_project_category_item100 <- as.data.frame(fread('wdcm_project_category_item100.tsv'))
wdcm_project_category_item100$projectype <- projectType(wdcm_project_category_item100$t.eu_project)
write.csv(wdcm_project_category_item100, "wdcm_project_category_item100.csv")

### --- produce wdcm_project_item100.tsv from wdcm_maintable (hdfs, database: goransm)
hiveQLquery <- 'USE goransm; SET hive.mapred.mode=unstrict;
                SELECT * FROM (
                  SELECT eu_project, eu_entity_id, eu_count, ROW_NUMBER() OVER (PARTITION BY eu_project ORDER BY eu_count DESC) AS row
                  FROM wdcm_maintable) t
                  WHERE row <= 100;'
system(command = paste('beeline -e "',
                       hiveQLquery,
                       '" > /home/goransm/WDCM_DataOUT/WDCM_DataOUT_ClientWDUsage/wdcm_project_item100.tsv',
                       sep = ""),
       wait = TRUE)
# - add projecttype to wdcm_project_item100.tsv
wdcm_project_item100 <- as.data.frame(fread('wdcm_project_item100.tsv'))
wdcm_project_item100$projectype <- projectType(wdcm_project_item100$t.eu_project)
write.csv(wdcm_project_item100, "wdcm_project_item100.csv")


### --- Semantic Modeling

### --- produce project-item matrices for semantic topic modeling
print("TDF MATRICES")
itemFiles <- list.files()
itemFiles <- itemFiles[grepl("^wdcm_item", itemFiles)]
for (i in 1:length(itemFiles)) {
  # - load categoryFile[i].tsv as data.frame
  # fix "Work of Art" -> "Work Of Art"
  categoryName <- gsub("of", "Of", itemFiles[i])
  categoryName <- strsplit(categoryName, ".", fixed = T)[[1]][1]
  categoryName <- strsplit(categoryName, "_", fixed = T)[[1]][3]
  categoryName <- gsub("([[:lower:]])([[:upper:]])", "\\1 \\2", categoryName)
  # fix "Work Of Art" -> "Work of Art"
  categoryName <- gsub("Of", "of", categoryName)
  print(categoryName)
  # - load items
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
  system(command = paste('beeline -e "',
                         hiveQLquery,
                         '" > /home/goransm/WDCM_DataOUT/WDCM_DataOUT_ClientWDUsage/',
                         fileName,
                         sep = ""),
         wait = TRUE)
}

### --- reshape project-item matrices for semantic topic modeling
print("RESHAPING TDF MATRICES")
itemFiles <- list.files()
itemFiles <- itemFiles[grepl("^tfMatrix_", itemFiles)]
itemFiles <- itemFiles[grepl(".tsv", itemFiles, fixed = T)]
for (i in 1:length(itemFiles)) {
  print(itemFiles[i])
  # - load categoryFile[i].tsv as data.frame
  categoryFile <- fread(itemFiles[i])
  categoryFile <- spread(categoryFile,
                         key = eu_entity_id,
                         value = eu_count,
                         fill = 0)
  rownames(categoryFile) <- categoryFile$eu_project
  categoryFile$eu_project <- NULL
  w <- which(colSums(categoryFile) == 0)
  if(length(w)>0) {
    categoryFile <- categoryFile[, -w]
  }
  w <- which(rowSums(categoryFile) == 0)
  if(length(w)>0) {
    categoryFile <- categoryFile[-w, ]
  }
  fileName <- paste(strsplit(itemFiles[i], split = ".", fixed = T)[[1]][1], ".csv", sep = "")
  write.csv(categoryFile, fileName)
}

### --- semantic topic models for each category
print("SEMANTIC TOPIC MODELS")
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
print("t-SNE 2D MAPS")
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
  rm(projectTopics); rm(projectDist); rm(nodes); rm(edges)
}

