#!/usr/bin/env Rscript

### ---------------------------------------------------------------------------
### --- wdcmModule_ML.R
### --- Author: Goran S. Milovanovic, Data Analyst, WMDE
### --- Developed under the contract between Goran Milovanovic PR Data Kolektiv
### --- and WMDE.
### --- Contact: goran.milovanovic_ext@wikimedia.de
### --- January 2019.
### ---------------------------------------------------------------------------
### --- DESCRIPTION:
### --- Machine Learning procedures for WDCM
### --- NOTE: the execution of this WDCM script is always dependent upon the
### --- previous WDCM_Sqoop_Clients.R run, as well
### --- as the previous execution of wdcmModule_CollectItems.R and 
### --- wdcmModule_ETL.py (Pyspark ETL)
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
### --- Script 3: wdcmModule_ML.R
### ---------------------------------------------------------------------------
### --- DESCRIPTION:
### --- wdcmModule_ML.R produces WD items TF matrices for LDA Topic Models and 
### --- applies t-SNE dimensionality reduction across several 
### --- model-based similarity matrices. 
### ---------------------------------------------------------------------------


# - to runtime Log:
print(paste("--- wdcmModule_ML.R UPDATE RUN STARTED ON:", 
            Sys.time(), sep = " "))
# - GENERAL TIMING:
generalT1 <- Sys.time()

### --- Setup

# - contact:
library(httr)
library(XML)
library(jsonlite)
# - wrangling:
library(dplyr)
library(stringr)
library(readr)
library(data.table)
library(tidyr)
# - modeling:
library(snowfall)
library(maptpx)
library(Rtsne)
### NOTE: replace proxy Hellinger Distrance
# library(proxy)
library(topicmodels)

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

### --- Read WDCM paramereters
# - fPath: where the scripts is run from?
fPath <- as.character(commandArgs(trailingOnly = FALSE)[4])
fPath <- gsub("--file=", "", fPath, fixed = T)
fPath <- unlist(strsplit(fPath, split = "/", fixed = T))
fPath <- paste(
  paste(fPath[1:length(fPath) - 1], collapse = "/"),
  "/",
  sep = "")
params <- xmlParse(paste0(fPath, "wdcmConfig.xml"))
params <- xmlToList(params)

### --- Directories
# - fPath: where the scripts is run from?
fPath <- params$general$fPath_R
# - form paths:
ontologyDir <- params$general$ontologyDir
logDir <- params$general$logDir
itemsDir <- params$general$itemsDir
structureDir <- params$general$structureDir
etlDir <- params$general$etlDir
# - production published-datasets:
dataDir <- params$general$publicDir

### --- ML params
lda_NItems <- as.numeric(params$ml$lda_NItems)

### ----------------------------------------------
### --- reshape project-item matrices for LDA
### ----------------------------------------------
setwd(etlDir)
# - to runtime Log:
print("STEP: Semantic Modeling Phase: RESHAPING TDF MATRICES")
selItems <- list.files()
selItems <- selItems[grepl("^wdcm_category_item_", selItems)]
itemFiles <- list.files()
itemFiles <- itemFiles[grepl("^tfMatrix_", itemFiles)]
for (i in 1:length(itemFiles)) {
  # - to runtime Log:
  print(paste("----------------------- Reshaping TDF matrix: category ", i, ".", sep = ""))
  # - load the most frequently used items
  selFile <- fread(selItems[i])
  nItems <- selFile$eu_entity_id[1:lda_NItems]
  rm(selFile)
  # - load i-th TFMatrix
  categoryFile <- fread(itemFiles[i])
  categoryFile$V1 <- NULL
  # -filter for nItems in categoryFile (the top lda_NItems frequently used items)
  categoryFile <- categoryFile %>%
    dplyr::select(eu_entity_id, eu_project, eu_count) %>% 
    dplyr::filter(eu_entity_id %in% nItems)
  categoryFile <- spread(categoryFile,
                         key = eu_entity_id,
                         value = eu_count,
                         fill = 0)
  rownames(categoryFile) <- categoryFile$eu_project
  categoryFile$eu_project <- NULL
  # - remove items with zero observations, if any
  w <- which(colSums(categoryFile) == 0)
  if (length(w) > 0) {
    categoryFile <- categoryFile[, -w]
  }
  # - remove projects with zero observations, if any
  w <- which(rowSums(categoryFile) == 0)
  if (length(w) > 0) {
    categoryFile <- categoryFile[-w, ]
  }
  # - save reshaped TF matrix
  write.csv(categoryFile, itemFiles[i])
}

### ----------------------------------------------
### --- LDA topic models for each category
### ----------------------------------------------
### --- to nohup.out
# - to runtime Log:
outDir <- params$general$mlDir
print("STEP: Semantic Modeling Phase: LDA estimation")
for (i in 1:length(itemFiles)) {
  
  categoryName <- strsplit(itemFiles[i], split = ".", fixed = T)[[1]][1]
  categoryName <- strsplit(categoryName, split = "_", fixed = T)[[1]][2]
  
  # - try to read the TF Matrix:
  itemCat <- tryCatch({
    read.csv(itemFiles[i],
             header = T,
             check.names = F,
             row.names = 1,
             stringsAsFactors = F)
  },
  error = function(condition) {NULL}
  )
  
  if (!is.null(itemCat)) {
    
    itemCat <- as.simple_triplet_matrix(itemCat)
    
    ## -- run on K = seq(2,20) semantic topics
    ####### ----------- PARALLEL w. {snowfall} STARTS
    # - start cluster and export data + package
    sfInit(parallel = T, cpus = 19)
    sfExport("itemCat")
    sfLibrary(maptpx)
    
    # - to runtime Log:
    print(paste("----------------------- LDA model: category ", i, ".", sep = ""))  
    topicModel <- list()
    numTopics <- seq(2, 20, by = 1)
    topicModels <- sfClusterApplyLB(numTopics,
                                    function(x) {
                                      maptpx::topics(counts = itemCat,
                                                     K = x, bf = T,
                                                     shape = NULL, initopics = NULL,
                                                     tol = .01, kill = 0,
                                                     ord = TRUE, verb = 0)
                                    }
    )
    
    # - stop cluster
    sfStop()
    
    # - clear:
    rm(itemCat); gc()
    # - determine model from Bayes Factor against Null:
    wModel <- which.max(sapply(topicModels, function(x) {x$BF}))[1]
    topicModel <- topicModels[[wModel]]
    rm(topicModels)
    
    # - to ml results dir:
    setwd(outDir)
    
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
    
    # - back to etl dir:
    setwd(etlDir)
    
  }
  
}

### ----------------------------------------------
### --- t-SNE 2D maps from wdcm2_projectttopic 
### --- files: projects similarity structure
### ----------------------------------------------
# - to runtime Log:
print("STEP: Semantic Modeling Phase: t-SNE 2D MAPS")
setwd(outDir)
projectFiles <- list.files()
projectFiles <- projectFiles[grepl("^wdcm2_projecttopic", projectFiles)]
tsne_theta <- as.numeric(params$ml$tSNE_Theta)
tsne_perplexity <- as.numeric(params$ml$tSNE_Perplexity)
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
  projectDist <- distHellinger(as.matrix(projectTopics))
  
  # - t-SNE 2D map
  tsneProject <- Rtsne(projectDist,
                       theta = tsne_theta,
                       is_distance = T,
                       tsne_perplexity = 10)
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
  projectDist <- distHellinger(as.matrix(projectTopics))
  rownames(projectDist) <- rownames(projectTopics)
  colnames(projectDist) <- rownames(projectTopics)
  
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

### --------------------------------------------------
### --- log ML:
### --------------------------------------------------
# - to runtime Log:
print("--- LOG: Collect_Items step completed.")
# - set log dir:
setwd(logDir)
# - write to WDCM main reporting file:
lF <- list.files()
if ('WDCM_MainReport.csv' %in% lF) {
  mainReport <- read.csv('WDCM_MainReport.csv',
                         header = T,
                         row.names = 1,
                         check.names = F,
                         stringsAsFactors = F)
  newReport <- data.frame(Step = 'ML',
                          Time = as.character(Sys.time()),
                          stringsAsFactors = F)
  mainReport <- rbind(mainReport, newReport)
  write.csv(mainReport, 'WDCM_MainReport.csv')
} else {
  newReport <- data.frame(Step = 'ML',
                          Time = as.character(Sys.time()),
                          stringsAsFactors = F)
  write.csv(newReport, 'WDCM_MainReport.csv')
}

# - GENERAL TIMING:
generalT2 <- Sys.time()
# - GENERAL TIMING REPORT:
print(paste0("--- wdcmModule_ML.R UPDATE DONE IN: ", 
             generalT2 - generalT1, "."))

