
### ---------------------------------------------------------------------------
### --- WDCM Search Module, v. Beta 0.1
### --- Script: WDCM_Search_Clients.R, v. Beta 0.1
### --- Author: Goran S. Milovanovic, Data Analyst, WMDE
### --- Developed under the contract between Goran Milovanovic PR Data Kolektiv 
### --- and WMDE.
### ---------------------------------------------------------------------------
### --- DESCRIPTION:
### --- WDCM_Search_Clients.R takes a list of
### --- item IDs from Wikidata (the list is delivered by
### --- the WDCM_Search_Items.R script) and searches for their
### --- usage across the Wikimedia projects on m2 MariaDB replicas
### --- on production (currently: stat1003.eqiad.wmnet).
### ---------------------------------------------------------------------------
### --- INPUT: 
### --- the WDCM_Search_Clients.R reads the list of item IDs
### --- to search for from /home/goransm/WDCM_DataIN/WDCM_DataIN_SearchItems.
### --- This folder contains the .csv files that specify the item IDs
### --- to search for; the files are produced by WDCM_Collect_Items.R 
### --- running on wikidataconcepts.wmflabs.org Labs instance.
### ---------------------------------------------------------------------------
### --- OUTPUT: 
### --- Results are stored locally as .tsv files on production,
### --- with each file encompassing the data for one client project.
### --- The outputs are stored locally on stat1003.eqiad.wmnet in:
### --- /a/published-datasets/WDCM/Search_Items_DataOUT
### --- These output .tsv files migrate to Labs:
### --- wikidataconcepts.wmflabs.org Labs instance
### --- where they are then processed by the WDCM Process Module.
### ---------------------------------------------------------------------------
### --- RUN:
### --- nohup Rscript /home/goransm/RScripts/WDCM_R/WDCM_Search_Clients.R &
### ---------------------------------------------------------------------------

### --- Setup
rm(list = ls())

# - Specify client projects:
top20WikisLang <- c('en', 'sv', 'ce', 'de', 'nl',
                    'fr', 'ru', 'wa', 'it', 'es',
                    'pl', 'vi', 'ja', 'pt', 'zh',
                    'uk', 'ca', 'fa', 'no', 'sh')
clients <- paste0(top20WikisLang, "wiki", sep = "")

### --- Loop over clients, construct query, and execute on production:

# - read item files from:
setwd('/home/goransm/WDCM_DataIN/WDCM_DataIN_SearchItems')
idFiles <- list.files()
idFiles <- idFiles[grepl(".csv$", idFiles)]

# - master timer starts:
# t1 <- Sys.time()

# - loop over items:
for (k in 1:length(idFiles)) {
  
  ### --- read item IDs:
  # - N.B. Using ALL item aspects
  qIDs <- read.csv(idFiles[k],
                   header = T,
                   check.names = F,
                   stringsAsFactors = F)
  qIDs <- qIDs$item
  qIDs <- qIDs[grepl("^Q[[:digit:]]+", qIDs)]
  
  # - cut into 5 batches:
  batchSize <- round(length(qIDs)/5)
  startBatchIx <- c(1:5) * batchSize - batchSize + 1
  stopBatchIx <- c(1:5) * batchSize
  stopBatchIx[5] <- length(qIDs)
  
  # - loop over batches:
  
  for (b in 1:length(startBatchIx)) {
    
    # - select batch:
    qIDsbatch <- paste0("'", qIDs[startBatchIx[b]:stopBatchIx[b]], "'", collapse = ",", sep = "")
    batchName <- paste0("b", b, collapse = "")
    
    # - Search
    
    # - loop over databases:
    for (i in 1:length(clients)) {
      
      # - construct sql query:
      fileName <- paste0(clients[i], "_",
                         paste0(strsplit(idFiles[k], split = "_", fixed = T)[[1]][1], collapse = ""),
                         "_",
                         batchName,
                         ".tsv",
                         collapse = ""
      )
      fileName <- paste0(strsplit(fileName, split = " ", fixed = T)[[1]], collapse = "")
      fileName <- paste0('/a/published-datasets/WDCM/Search_Items_DataOUT/',
                         fileName,
                         collapse = "")
      q <- paste0(paste0("USE ", clients[i], "; ", collapse = ""),
        "SELECT eu_entity_id, eu_aspect, COUNT(*) as Count ",
        "FROM wbc_entity_usage WHERE eu_entity_id IN  (",
        qIDsbatch,
        ") GROUP BY eu_entity_id, eu_aspect;",
        collapse = "")
      
      # - set path to sql scripts:
      setwd('/home/goransm/WDCM_sql')
      # - store sql script:
      write(q, 'wdcm_searchClients.sql')
      
      # - execute sql script:
      mySqlArgs <- 
        '--defaults-file=/etc/mysql/conf.d/research-client.cnf -h analytics-store.eqiad.wmnet -A'
      mySqlInput <- paste0('< /home/goransm/WDCM_sql/wdcm_searchClients.sql > ', 
        fileName,
        collapse = "")
      # - command:
      mySqlCommand <- paste0("mysql ", mySqlArgs, " ", mySqlInput, collapse = "")
      system(command = mySqlCommand, wait = TRUE)
      
    }
    
    # - pause 5 secs
    Sys.sleep(5)
    
  }
  
  # - pause 30 secs
  Sys.sleep(30)
  
  # - back to item lists:
  setwd('/home/goransm/WDCM_DataIN/WDCM_DataIN_SearchItems')
  
}




