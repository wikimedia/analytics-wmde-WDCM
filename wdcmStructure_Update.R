### ---------------------------------------------------------------------------
### --- WDCM Structure Dashboard UPDATE, v. Beta 0.1
### --- Script: ui.R, v. Beta 0.1
### ---------------------------------------------------------------------------

### --- Setup
library(data.table)
library(jsonlite)
library(httr)
library(stringr)
library(dplyr)

### --- wd_Superclasses_wTRecurrently
# - fetch all superclasses (inverse P279 search)
# - up to the P279 constraint target: Q35120 (entity)
wd_Superclasses_Recurrently <- function(entity, 
                                        language = 'en', 
                                        cleanup = T,
                                        fetchSubClasses = T,
                                        fetchCounts = T,
                                        SPARQL_Endpoint = 'https://query.wikidata.org/bigdata/namespace/wdq/sparql?query=') {
  
  # - to store results
  results <- vector(length(entity), mode = "list")
  # - to store the function output
  output <- list()
  
  # - fetch entity labels
  entityLab <- character(length(entity))
  for (i in 1:length(entity)) {
    
    message(paste0("Fetching labels......", round(i/length(entity)*100, 2), "%"))
    
    # - construct SPARQL query
    query <- paste0('SELECT * WHERE { wd:', entity[i], ' rdfs:label ?label . FILTER (langMatches( lang(?label), "',
                    language, '" ))} LIMIT 1')
    # - run Query
    res <- GET(paste0(SPARQL_Endpoint, URLencode(query)))
    # - fromJSON
    res <- fromJSON(rawToChar(res$content))
    entityLab[i] <- res$results$bindings$label$value
  }
  
  # - fetch entity P279 and P31 superclasses
  fresults <- vector(mode = "list", length = length(entity))
  for (i in 1:length(entity)) {
    
    results <- vector(mode = "list", length = 4)
    message(paste0("Fetching super-classes......", round(i/length(entity)*100, 2), "%"))
    
    # - compose SPARQL query1
    query1 <- paste0(
      'SELECT ?item ?itemLabel ?Superclass ?SuperclassLabel ', 
      'WHERE { wd:', entity[i] ,' ((wdt:P279|wdt:P31)/((wdt:P279|wdt:P31)*))|(wdt:P279)|(wdt:P31) ?item . ?item wdt:P279 ?Superclass .
      SERVICE wikibase:label { bd:serviceParam wikibase:language "', language, '". }}'
    )
    # - run query 1
    res <- GET(paste0(SPARQL_Endpoint, URLencode(query1)))
    # - fromJSON
    results[[1]] <- fromJSON(rawToChar(res$content))
    if (length(results[[1]]$results$bindings) == 0) {
      results[[1]] <- NULL
    } else {
      results[[1]] <- data.frame(item = results[[1]]$results$bindings$item$value,
                                 itemLabel = results[[1]]$results$bindings$itemLabel$value,
                                 superClass = results[[1]]$results$bindings$Superclass$value,
                                 superClassLabel = results[[1]]$results$bindings$SuperclassLabel$value,
                                 stringsAsFactors = F)
      results[[1]]$relation <- rep('P279', dim(results[[1]])[1])
    }
    
    # - compose SPARQL query2
    query2 <- paste0(
      'SELECT ?item ?itemLabel ?Superclass ?SuperclassLabel ', 
      'WHERE { wd:', entity[i] ,' ((wdt:P279|wdt:P31)/((wdt:P279|wdt:P31)*))|(wdt:P279)|(wdt:P31) ?item . ?item wdt:P31 ?Superclass .
      SERVICE wikibase:label { bd:serviceParam wikibase:language "', language, '". }}'
    )
    # - run query 2
    res <- GET(paste0(SPARQL_Endpoint, URLencode(query2)))
    # - fromJSON
    results[[2]] <- fromJSON(rawToChar(res$content))
    if (length(results[[2]]$results$bindings) == 0) {
      results[[2]] <- NULL
    } else {
      results[[2]] <- data.frame(item = results[[2]]$results$bindings$item$value,
                                 itemLabel = results[[2]]$results$bindings$itemLabel$value,
                                 superClass = results[[2]]$results$bindings$Superclass$value,
                                 superClassLabel = results[[2]]$results$bindings$SuperclassLabel$value,
                                 stringsAsFactors = F)
      results[[2]]$relation <- rep('P31', dim(results[[2]])[1])
    }
    
    # - query to fetch immediate P31 superclasses:
    # - compose SPARQL query
    query3 <- paste0(
      'SELECT ?Superclass ?SuperclassLabel ', 
      'WHERE { wd:', entity[i] ,' wdt:P31 ?Superclass .
      SERVICE wikibase:label { bd:serviceParam wikibase:language "', language, '". }}'
    )
    # - run Query
    res <- GET(paste0(SPARQL_Endpoint, URLencode(query3)))
    # - fromJSON
    results[[3]] <- fromJSON(rawToChar(res$content))
    if (length(results[[3]]$results$bindings) == 0) {
      results[[3]] <- NULL
    } else {
      results[[3]] <- results[[3]]$results$bindings
      results[[3]] <- data.frame(item = paste0('http://www.wikidata.org/entity/', rep(entity[i], dim(results[[3]])[1])),
                                 itemLabel = rep(entityLab[i], dim(results[[3]])[1]),
                                 superClass = results[[3]]$Superclass$value,
                                 superClassLabel = results[[3]]$SuperclassLabel$value,
                                 relation = 'P31',
                                 stringsAsFactors = F)
    }
    
    # - query to fetch immediate P279 superclasses:
    # - compose SPARQL query
    query4 <- paste0(
      'SELECT ?Superclass ?SuperclassLabel ', 
      'WHERE { wd:', entity[i] ,' wdt:P279 ?Superclass .
      SERVICE wikibase:label { bd:serviceParam wikibase:language "', language, '". }}'
    )
    # - run Query
    res <- GET(paste0(SPARQL_Endpoint, URLencode(query4)))
    # - fromJSON
    results[[4]] <- fromJSON(rawToChar(res$content))
    if (length(results[[4]]$results$bindings) == 0) {
      results[[4]] <- NULL
    } else {
      results[[4]] <- results[[4]]$results$bindings
      results[[4]] <- data.frame(item = paste0('http://www.wikidata.org/entity/', rep(entity[i], dim(results[[4]])[1])),
                                 itemLabel = rep(entityLab[i], dim(results[[4]])[1]),
                                 superClass = results[[4]]$Superclass$value,
                                 superClassLabel = results[[4]]$SuperclassLabel$value,
                                 relation = 'P279',
                                 stringsAsFactors = F)
    }
    
    # - rbindlist results
    fresults[[i]] <- as.data.frame(rbindlist(results))
    fresults[[i]] <- fresults[[i]][!duplicated(fresults[[i]]), ]
    
    # - cleanup
    if (cleanup) {
      fresults[[i]]$item <- gsub('http://www.wikidata.org/entity/', '', fresults[[i]]$item)
      fresults[[i]]$superClass <- gsub('http://www.wikidata.org/entity/', '', fresults[[i]]$superClass)
    }
  }

  # - rbindlist()
  results <- rbindlist(fresults)
  results$itemLabel <- tolower(results$itemLabel)
  results$superClassLabel <- tolower(results$superClassLabel)
  results <- results[!duplicated(results), ]
  # - arrange()
  results <- arrange(results, item)
  output$structure <- results
  output$entity <- entity
  
  # - fetch all immediate subclasses of the classes under consideration
  if (fetchSubClasses) {
    
    classes <- c(unique(output$structure$item), unique(output$structure$superClass))
    imSubClass <- vector(length(classes), mode = "list")
    for (i in 1:length(classes)) {
      
      message(paste0("Fetching sub-classes......", round(i/length(classes)*100, 2), "%"))
      
      # - compose SPARQL query
      query <- paste0(
        'SELECT ?subClass ?subClassLabel ', 
        "WHERE { ?subClass wdt:P279 wd:" , classes[i], " . ",
        "SERVICE wikibase:label { bd:serviceParam wikibase:language '", language, "'. }}"
      )
      
      # - run Query
      res <- GET(paste0(SPARQL_Endpoint, URLencode(query)))
      
      # - fromJSON
      sClass <- fromJSON(rawToChar(res$content))$results$bindings
      
      # - data.frame:
      if (class(sClass) == "data.frame") {
        iLabel <- output$structure$itemLabel[which(output$structure$item %in% classes[i])[1]]
        imSubClass[[i]] <- data.frame(item = rep(classes[i], dim(sClass)[1]),
                                      itemLabel = rep(iLabel, dim(sClass)[1]),
                                      subClass = sClass$subClass$value,
                                      subClassLabel = sClass$subClassLabel$value,
                                      stringsAsFactors = F
                                      
                                      
        )
        
        # - cleanup
        if (cleanup) {
          imSubClass[[i]]$item <- gsub('http://www.wikidata.org/entity/', '', imSubClass[[i]]$item)
        }
        
      } else {
        imSubClass[[i]] <- NULL
      }
      
    }
    
    # - merge imSubClass to output
    # - rbindlist() imSubClass first
    imSubClass <- rbindlist(imSubClass)
    imSubClass$itemLabel <- tolower(imSubClass$itemLabel)
    imSubClass$subClassLabel <- tolower(imSubClass$subClassLabel)
    # - arrange()
    imSubClass <- arrange(imSubClass, item)
    imSubClass <- imSubClass[!duplicated(imSubClass), ]
    output$subClasses <- imSubClass
    rm(imSubClass)
    
  }
  
  # - fetch item counts for all classes under consideration
  if (fetchCounts) {
    
    classes <- c(unique(output$structure$item), unique(output$structure$superClass))
    classesCount <- vector(length(classes), mode = "list")
    for (i in 1:length(classes)) {
      
      message(paste0("Fetching counts......", round(i/length(classes)*100, 2), "%... ", classes[i]))
      
      # - compose SPARQL query to fetch COUNT(?subClass)
      query1 <- paste0(
        'SELECT (COUNT(?subClass) AS ?subClassCount) ',  
        "WHERE { ?subClass wdt:P279 wd:" , classes[i], " . }"
      )
      
      # - run Query
      res1 <- GET(paste0(SPARQL_Endpoint, URLencode(query1)))
      
      # - compose SPARQL query to fetch COUNT(?item)
      query2 <- paste0(
        "SELECT (COUNT(?item) AS ?itemCount)  WHERE {?item wdt:P31 wd:" , classes[i], " . }"
      )
      
      # - run Query 2
      res2 <- GET(paste0(SPARQL_Endpoint, URLencode(query2)))
      
      # - fromJSON
      counts1 <- fromJSON(rawToChar(res1$content))$results$bindings
      counts2 <- fromJSON(rawToChar(res2$content))$results$bindings
      
      # - data.frame:
      iLabel <- output$structure$itemLabel[which(output$structure$item %in% classes[i])[1]]
      classesCount[[i]] <- data.frame(item = classes[i], 
                                      itemLabel = iLabel,
                                      numSubClass = ifelse(class(counts1) == "data.frame", counts1$subClassCount$value, 0),
                                      numItems = ifelse(class(counts2) == "data.frame", counts2$itemCount$value, 0),
                                      stringsAsFactors = F
      )
      
    }
    
    # - merge w. output
    classesCount <- rbindlist(classesCount)
    output$counts <- classesCount
    rm(classesCount)
    
  }
  
  # - return
  return(output)
  
}

### --------------------- PLAY
setwd('/home/goransm/WMDE/WDCM/WDCM_StructureDashboard/_data/')
wdcmTax <- read.csv('WDCM_Ontology_Berlin_05032017.csv',
                    header = T,
                    row.names = 1,
                    check.names = F,
                    stringsAsFactors = F)
entity <- wdcmTax$CategoryItems 
entity <- unique(unlist(strsplit(entity, split = ", ", fixed = T)))

myWD <- wd_Superclasses_Recurrently(entity = entity, 
                                    language = 'en', 
                                    cleanup = T,
                                    fetchSubClasses = T,
                                    fetchCounts = T,
                                    SPARQL_Endpoint = 'https://query.wikidata.org/bigdata/namespace/wdq/sparql?query=')
write.csv(myWD$structure, "wdcmStructure_StructFrame.csv")
write.csv(myWD$subClasses, "wdcmStructure_SubClassFrame.csv")
write.csv(myWD$entity, "wdcmStructure_Entities.csv")
write.csv(myWD$counts, "wdcmStructure_Counts.csv")

### --- updateReport File
updateReport <- as.character(Sys.time())
upY <- substr(updateReport, 1, 4)
upM <- as.numeric(substr(updateReport, 6, 7))
upM <- month.name[upM]
upD <- substr(updateReport, 9, 10)
updateReport <- paste0(upY, " ", upM, " ", upD)
write(updateReport, "updateReport.txt")

### --------------------- cp to /srv/shiny-server/WDCM_Structure/_data
system('sudo cp /home/goransm/WMDE/WDCM/WDCM_StructureDashboard/_data/* /srv/shiny-server/WDCM_StructureDashboard/_data', 
       wait = F)


