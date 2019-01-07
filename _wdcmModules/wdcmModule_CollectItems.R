#!/usr/bin/env Rscript

### ---------------------------------------------------------------------------
### --- wdcmModule_CollectItems.R
### --- Author: Goran S. Milovanovic, Data Analyst, WMDE
### --- Developed under the contract between Goran Milovanovic PR Data Kolektiv
### --- and WMDE.
### --- Contact: goran.milovanovic_ext@wikimedia.de
### --- January 2019.
### ---------------------------------------------------------------------------
### --- DESCRIPTION:
### --- Contact WDQS and fetch all item IDs from the WDCM WD Ontology
### --- NOTE: the execution of this WDCM script is always dependent upon the
### --- previous WDCM_Sqoop_Clients.R run.
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
### --- Script 1: wdcmModule_CollectItems.R
### ---------------------------------------------------------------------------
### --- DESCRIPTION:
### --- wdcmModule_CollectItems.R takes a list of items (categories)
### --- defined by a given WDCM WD Ontology (human input) and then
### --- contacts the Wikidata Query Service to fetch all relevant item IDs.
### ---------------------------------------------------------------------------
### --- INPUT:
### --- the WDCM_Collect_Items.R reads the WDCM Ontology file (csv)
### --- ACTIVE WDCM TAXONOMY: WDCM_Ontology_Berlin_05032017.csv
### ---------------------------------------------------------------------------


# - to runtime Log:
print(paste("--- wdcmModule_CollectItems.R UPDATE RUN STARTED ON:", 
            Sys.time(), sep = " "))
# - GENERAL TIMING:
generalT1 <- Sys.time()

### --- Setup

# - contact:
library(httr)
library(XML)
library(jsonlite)
# - wrangling:
library(stringr)
library(readr)
library(data.table)
library(tidyr)
# - modeling:
library(snowfall)
library(maptpx)
library(Rtsne)
library(topicmodels)

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

### --- Set proxy
Sys.setenv(
  http_proxy = params$general$http_proxy,
  https_proxy = params$general$https_proxy)

### --- Directories
# - fPath: where the scripts is run from?
fPath <- params$general$fPath_R
# - form paths:
ontologyDir <- params$general$ontologyDir
logDir <- params$general$logDir
itemsDir <- params$general$itemsDir
structureDir <- params$general$structureDir
# - production published-datasets:
dataDir <- params$general$publicDir

### --- Read WDCM_Ontology
# - to runtime Log:
print("--- Reading Ontology.")
setwd(ontologyDir)
wdcmOntology <- read.csv(paste0(params$general$ontologyDir, params$general$ontology),
                         header = T,
                         check.names = F,
                         stringsAsFactors = F)

# - WDQS Classes endPoint
# - NOTE: from https://www.mediawiki.org/wiki/Wikidata_Query_Service/Categories
# - an endpoint is provided which does not return any results except for the very 
# - gas:in class that was inputed to BFS.
# - Using the "default" WDQS SPARQL endpoint here (this delivers):
endPointURL <- params$general$wdqs_endpoint

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
  
  # - to runtime Log:
  print(paste0("--- SPARQL category: ", wdcmOntology$WikidataDescription[i]))
  
  searchItems <- str_trim(
    strsplit(wdcmOntology$CategoryItems[i],
             split = ",", fixed = T)[[1]],
    "both")
  
  itemsOut <- list()
  
  for (k in 1:length(searchItems)) {
    
    ### --- fetch all subclasses
    
    # - define targetClass
    targetClass <- searchItems[k]
    
    # - to runtime Log:
    print(paste0("--- SPARQL sub-category: ", targetClass))
    
    # - Construct Query:
    query <- paste0('SELECT ?item WHERE { 
                    SERVICE gas:service { 
                    gas:program gas:gasClass "com.bigdata.rdf.graph.analytics.BFS" . 
                    gas:program gas:in wd:', targetClass, ' .
                    gas:program gas:linkType wdt:P279 .
                    gas:program gas:out ?subClass .
                    gas:program gas:traversalDirection "Reverse" .
                    } . 
                    ?item wdt:P31 ?subClass 
  }')

    # - Run Query:
    repeat {
      res <- tryCatch({
        GET(url = paste0(endPointURL, URLencode(query)))
      },
      error = function(condition) {
        print("Something's wrong on WDQS: wait 10 secs, try again.")
        Sys.sleep(10)
        GET(url = paste0(endPointURL, URLencode(query)))
      },
      warning = function(condition) {
        print("Something's wrong on WDQS: wait 10 secs, try again.")
        Sys.sleep(10)
        GET(url = paste0(endPointURL, URLencode(query)))
      }
      )  
      if (res$status_code == 200) {
        print(paste0(targetClass, ": success."))
        break
      } else {
        print(paste0(targetClass, ": failed; retry."))
        Sys.sleep(10)
        }
    }
    
    # - Extract item IDs:
    if (res$status_code == 200) {
      
      # - tryCatch rawToChar
      # - NOTE: might fail for very long vectors
      rc <- tryCatch(
        {
          rawToChar(res$content)
        },
        error = function(condition) {
          return(FALSE)
        }
      )
      
      if (rc == FALSE) {
        print("rawToChar() conversion failed. Skipping.")
        next
      }
      
      # - is.ExceptionTimeout
      queryTimeout <- grepl("timeout", rc, ignore.case = TRUE)
      if (queryTimeout) {
        print("Query timeout (!)")
      }
      
      rc <- data.frame(item = unlist(str_extract_all(rc, "Q[[:digit:]]+")), 
                       stringsAsFactors = F)
    } else {
      print(paste0("Server response: ", res$status_code))
      qErrors <- append(qErrors, targetClass)
    }
    
    itemsOut[[k]] <- rc
    
    }
  
  # - store
  if (length(itemsOut) > 0) {
    
    # - itemsOut as data.table:
    itemsOut <- rbindlist(itemsOut)
    # - write complete structure for category
    filename <- paste0(wdcmOntology$Category[i], "_Structure.csv")
    setwd(structureDir)
    write.csv(itemsOut, filename)
    
    # - keep only unique items:
    w <- which(!(duplicated(itemsOut$item)))
    itemsOut <- itemsOut[w]
    
    # store as CSV
    filename <- paste0(wdcmOntology$Category[i],"_ItemIDs.csv")
    setwd(itemsDir)
    write_csv(itemsOut, filename)
    
    # clear:
    rm(itemsOut); gc()
  }
  
  }

### --- Fix WDCM_Ontology (Phab T174896#3762820)

# - to runtime Log:
print("--- Fix WDCM_Ontology (Phab T174896#3762820)")

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

### --- log Collect_Items:
# - to runtime Log:
print("--- LOG: Collect_Items step completed.")
# - set log dir:
setwd(logDir)
# - log uncompleted queries
write.csv(qErrors, "WDCM_CollectItems_SPARQL_Errors.csv")
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

# - GENERAL TIMING:
generalT2 <- Sys.time()
# - GENERAL TIMING REPORT:
print(paste0("--- wdcmModule_CollectItems.R UPDATE DONE IN: ", 
             generalT2 - generalT1, "."))
