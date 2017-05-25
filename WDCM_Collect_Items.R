
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
### --- contacts the SPARQL endpoint to fetch all relevant item IDs.
### ---------------------------------------------------------------------------
### --- INPUT: 
### --- the WDCM_Collect_Items.R reads the WDCM Ontology file (csv)
### --- from /home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_Ontology
### --- on wikidataconcepts.wmflabs.org
### ---------------------------------------------------------------------------
### --- OUTPUT: 
### --- Results are stored locally as .csv files on the wikidataconcepts Labs instance:
### --- wikidataconcepts.wmflabs.org
### --- in: /home/goransm/WMDE/WDCM/WDCM_DataOUT
### --- These output .csv files migrate to production (stat1003.eqiad.wmnet, currently):
### --- where they are then further processed by the WDCM Search Module (running: 
### --- WDCM_Search_Clients.R)
### ---------------------------------------------------------------------------

### --- Setup

library(httr)
library(stringr)
library(XML)
library(readr)
library(data.table)

### ---- Read WDCM_Ontology from: /home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_Ontology

wDir <- '/home/goransm/WMDE/WDCM/WDCM_DataIN/WDCM_Ontology'
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

# - set output dir:
outDir <- '/home/goransm/WMDE/WDCM/WDCM_DataOUT/WDCM_DataOUT_SearchItems'
setwd(outDir)

# - track uncompleted queries:
qErrors <- character()

for (i in 1:length(wdcmOntology$CategoryItems)) {
  
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
      'SELECT ?item WHERE { ?item wdt:P31/wdt:P279* wd:', 
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
      labels <- xpathSApply(rc, "//literal", xmlValue)
  
      # - as.data.frame:
      items <- data.frame(item = items, 
                          label = labels,
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
  
  # Wait 30 secs before processing and running the next query:
  Sys.sleep(30)

}

# - log uncompleted queries:
# - set log dir:
outDir <- '/home/goransm/WMDE/WDCM/WDCM_DataOUT/WDCM_DataOUT_SearchItems/qErrorsLOG'
setwd(outDir)
write.csv(qErrors, "qErrors.csv")

