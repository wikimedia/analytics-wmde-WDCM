### ---------------------------------------------------------------------------
### --- WDCM Semantics Dashboard, v. Beta 0.1
### --- Script: server.R, v. Beta 0.1
### ---------------------------------------------------------------------------

### --- Setup
rm(list = ls())
### --------------------------------
### --- general
library(shiny)
library(shinydashboard)
library(RMySQL)
library(data.table)
library(DT)
library(stringr)
library(tidyr)
library(dplyr)
library(reshape2)
### --- compute
library(parallelDist)
### --- visualization
library(RColorBrewer)
library(visNetwork)
library(networkD3)
library(ggplot2)
library(ggrepel)
library(scales)

### --- Server (Session) Scope
### --------------------------------

### --- Credentials
# setwd('/home/goransm/WMDE/WDCM/WDCM_RScripts/WDCM_Dashboard/aux')
setwd('/srv/shiny-server/aux')

mySQLCreds <- fread("mySQLCreds.csv", 
                    header = T,
                    drop = 1)

### -- Connect
con <- dbConnect(MySQL(), 
                 host = "tools.labsdb", 
                 defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                 dbname = "u16664__wdcm_p",
                 user = mySQLCreds$user,
                 password = mySQLCreds$password)

### --- list existing tables
q <- "SHOW TABLES;"
res <- dbSendQuery(con, q)
st <- fetch(res, -1)
dbClearResult(res)
colnames(st) <- "tables"

### --- SET CHARACTER SET utf8
q <- "SET CHARACTER SET utf8;"
res <- dbSendQuery(con, q)
dbClearResult(res)

### --- itemTopicTables
itemTopicTables <- st$tables[which(grepl("wdcm2_itemtopic_", st$tables, fixed = T))]

### --- fetch wdcm2_project
q <- "SELECT * FROM wdcm2_project;"
res <- dbSendQuery(con, q)
wdcmProject <- fetch(res, -1)
dbClearResult(res)
colnames(wdcmProject) <- c('Project', 'Usage', 'Project Type')

### --- fetch wdcm2_project_category
q <- "SELECT * FROM wdcm2_project_category;"
res <- dbSendQuery(con, q)
wdcmProjectCategory <- fetch(res, -1)
dbClearResult(res) 
colnames(wdcmProjectCategory) <- c('Project', 'Category', 'Usage', 'Project Type')

### --- fetch wdcm2_category
q <- "SELECT * FROM wdcm2_category;"
res <- dbSendQuery(con, q)
wdcmCategory <- fetch(res, -1)
dbClearResult(res) 
colnames(wdcmCategory) <- c('Category', 'Usage')

### --- fetch wdcm2_projects_2dmaps
q <- "SELECT * FROM wdcm2_projects_2dmaps;"
res <- dbSendQuery(con, q)
wdcm2_projects_2dmaps <- fetch(res, -1)
dbClearResult(res) 
colnames(wdcm2_projects_2dmaps) <- c('D1', 'D2', 'Project', 'Project Type', 'Category')

### --- Disconnect
dbDisconnect(con)

### --- Fetch local files
setwd('/home/goransm/WMDE/WDCM/WDCM_RScripts/WDCM_SemanticsDashboard/data/')

### --- fetch projecttopic tables
lF <- list.files()
lF <- lF[grepl("wdcm2_projecttopic_", lF, fixed = T)]
projectTopic <- vector(mode = "list", length = length(lF))
for (i in 1:length(lF)) {
  projectTopic[[i]] <- fread(lF[i], data.table = F)
}
names(projectTopic) <- sapply(lF, function(x) {
  strsplit(strsplit(x, split = ".", fixed = T)[[1]][1],
           split = "_",
           fixed = T)[[1]][3]
})

### --- fetch wdcm2_visNetworkNodes_project tables
lF <- list.files()
lF <- lF[grepl("wdcm2_visNetworkNodes_project", lF, fixed = T)]
visNetworkNodes <- vector(mode = "list", length = length(lF))
for (i in 1:length(lF)) {
  visNetworkNodes[[i]] <- fread(lF[i])
}
names(visNetworkNodes) <- sapply(lF, function(x) {
  strsplit(strsplit(x, split = ".", fixed = T)[[1]][1],
           split = "_",
           fixed = T)[[1]][4]
})

### --- fetch wdcm2_visNetworkEdges_project tables
lF <- list.files()
lF <- lF[grepl("wdcm2_visNetworkEdges_project", lF, fixed = T)]
visNetworkEdges <- vector(mode = "list", length = length(lF))
for (i in 1:length(lF)) {
  visNetworkEdges[[i]] <- fread(lF[i])
}
names(visNetworkEdges) <- sapply(lF, function(x) {
  strsplit(strsplit(x, split = ".", fixed = T)[[1]][1],
           split = "_",
           fixed = T)[[1]][4]
})

### - Determine Constants
# - determine Projects
projects <- wdcmProject$Project
# - determine present Project Types
projectTypes <- unique(wdcmProject$`Project Type`)
# - and assign Brewer colors
lengthProjectColor <- length(unique(wdcmProject$`Project Type`))
projectTypeColor <- brewer.pal(lengthProjectColor, "Set1")
names(projectTypeColor) <- unique(wdcmProject$`Project Type`)
# - determine Categories
categories <- wdcmCategory$Category
# - totalUsage
totalUsage <- sum(wdcmProject$Usage)
totalProjects <- length(wdcmProject$Project)
totalCategories <- length(wdcmCategory$Category)
totalProjectTypes <- length(unique(wdcmProject$`Project Type`))

### --- prepare search constants for Tabs/Crosstabs
search_projectTypes <- paste("_", projectTypes, sep = "")
unzip_projectTypes <- lapply(projectTypes, function(x) {
  wdcmProject$Project[which(wdcmProject$`Project Type` %in% x)]
})
names(unzip_projectTypes) <- search_projectTypes

### --- shinyServer
shinyServer(function(input, output, session) {
  
  ### ------------------------------------------
  ### --- TAB: tabPanel Semantic Models
  ### ------------------------------------------
  
  ### --- SELECT: update select 'selectCategory'
  updateSelectizeInput(session,
                       'selectCategory',
                       "Select Semantic Category:",
                       choices = categories,
                       selected = categories[round(runif(1, 1, length(categories)))],
                       server = TRUE)
  
  ### --- REACTIVE: category specific wdcm_itemtopic data.frame
  itemTopicsNum <- reactive({
    sC <- gsub(" ", "", input$selectCategory, fixed = T)
    sTable <- itemTopicTables[which(grepl(sC, itemTopicTables, fixed = T))]
    ### -- Connect
    con <- dbConnect(MySQL(), 
                     host = "tools.labsdb", 
                     defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                     dbname = "u16664__wdcm_p",
                     user = mySQLCreds$user,
                     password = mySQLCreds$password)
    ### --- check the particular table
    q <- paste("DESCRIBE ", sTable, ";", sep = "")
    res <- dbSendQuery(con, q)
    sIT <- fetch(res, -1)
    dbClearResult(res)
    ### --- Disconnect
    dbDisconnect(con)
    sum(grepl("topic", sIT$Field))
  })
  
  ### --- SELECT: updateSelectizeInput 'selectCatTopic'
  output$selectCatTopic <-
    renderUI({
      if ((is.null(input$selectCategory)) | (length(input$selectCategory) == 0)) {
        selectInput(inputId = "selectCategoryTopic",
                    label = "Select Semantic Topic:",
                    choices = NULL,
                    selected = NULL)
      } else {
        cH <- paste("Topic", 1:itemTopicsNum(), sep = " ")
        selectInput(inputId = "selectCategoryTopic",
                    label = "Select Semantic Topic:",
                    choices = cH,
                    selected = cH[1])
      }
    })
  
  ### --- REACTIVE current itemTopic table:
  itemTopic <- reactive({
      sC <- gsub(" ", "", input$selectCategory, fixed = T)
      sTable <- itemTopicTables[which(grepl(sC, itemTopicTables, fixed = T))]
      cTopic <- tolower(gsub(" ", "", input$selectCategoryTopic))
      if (!length(cTopic) == 0) {
        ### -- Connect
        con <- dbConnect(MySQL(),
                         host = "tools.labsdb",
                         defult.file = "/home/goransm/mySQL_Credentials/replica.my.cnf",
                         dbname = "u16664__wdcm_p",
                         user = mySQLCreds$user,
                         password = mySQLCreds$password)
        ### --- check the particular table
        q <- 'SET CHARACTER SET utf8;'
        res <- dbSendQuery(con, q)
        q <- paste("SELECT * FROM ", sTable, " ORDER BY ", cTopic, " DESC LIMIT 50;", sep = "")
        res <- dbSendQuery(con, q)
        iT <- fetch(res, -1)
        dbClearResult(res)
        ### --- Disconnect
        dbDisconnect(con)
        ### --- Output:
        return(iT) 
      } else {return(NULL)}
  })
  
  ### --- OUTPUT output$topItemsTopic
  output$topItemsTopic <- renderPlot({
    if (!is.null(itemTopic())) {
      cTopic <- tolower(gsub(" ", "", input$selectCategoryTopic))
      plotFrame <- itemTopic()
      plotFrame <- select(plotFrame, 
                          eu_label, eu_entity_id, cTopic)
      colnames(plotFrame) <- c('Label', 'Id', 'Probability')
      plotFrame$Label <- paste(1:dim(plotFrame)[1], ". ", plotFrame$Label, sep = "")
      plotFrame$Label <- factor(plotFrame$Label, 
                                levels = plotFrame$Label[order(plotFrame$Probability)])
      plotFrame$Sign <- paste("(", 1:dim(plotFrame)[1], ") ", plotFrame$Id, sep = "")
      ggplot(plotFrame, aes(x = Probability, 
                            y = Label, 
                            label = Sign)) +
        geom_line(size = .25, color = "#4c8cff", group = 1) + 
        geom_point(size = 1.5, color = "#4c8cff") + 
        geom_point(size = 1, color = "white") + 
        geom_label_repel(size = 3, segment.size = .25, show.legend = FALSE) +
        ylab("Items Labels") + xlab("Item Importance\n(Item Probability in Topic)") +
        theme_minimal() +
        theme(axis.text.x = element_text(angle = 90, size = 12, hjust = 1)) + 
        theme(axis.text.y = element_text(size = 12, hjust = 1)) +
        theme(axis.title.x = element_text(size = 12)) +
        theme(axis.title.y = element_text(size = 12)) %>% 
        withProgress(message = 'Generating plot',
                     min = 0,
                     max = 1,
                     value = 1, {incProgress(amount = 0)})
    } else {return(NULL)}
  }) 
  
  # - output$networkItemsTopic
  output$networkItemsTopic <- renderVisNetwork({
    
    if (!is.null(itemTopic())) {
      # - normalization: Luce's choice axiom
      itemNames <- itemTopic()$eu_label
      root <- select(itemTopic(), starts_with('topic'))
      root <- as.matrix(parDist(as.matrix(root), method = "euclidean"))
      rownames(root) <- itemNames
      colnames(root) <- itemNames
      indexMinDist <- sapply(rownames(root), function(x) {
        w <- which(rownames(root) %in% x)
        y <- sort(root[w, -w], decreasing = T)
        names(y)[length(y)]
      })
      id <- 1:length(colnames(root))
      label <- colnames(root)
      nodes <- data.frame(id = id,
                          label = label,
                          stringsAsFactors = F)
      conceptsStruct <- data.frame(from = names(indexMinDist),
                                   to = unname(indexMinDist),
                                   stringsAsFactors = F)
      conceptsStruct$from <- sapply(conceptsStruct$from, function(x) {
        nodes$id[which(nodes$label %in% x)]
      })
      conceptsStruct$to <- sapply(conceptsStruct$to, function(x) {
        nodes$id[which(nodes$label %in% x)]
      })
      conceptsStruct$arrows <- rep("to", length(conceptsStruct$to))
      visNetwork(nodes = nodes,
                 edges = conceptsStruct,
                 width = "100%",
                 height = "100%") %>%
        visEvents(type = "once",
                  startStabilizing = "function() {this.moveTo({scale:0.65})}") %>%
        visPhysics(stabilization = FALSE) %>%
        withProgress(message = 'Generating plot',
                     min = 0,
                     max = 1,
                     value = 1, {incProgress(amount = 0)})
    } else {return(NULL)}
  })
  
  ### --- REACTIVE current pTopic data.frame:
  pTopic <- reactive({
    w <- which(names(projectTopic) %in% gsub(" ", "", input$selectCategory, fixed = T))
    if (!length(w) == 0) {
      pTopic <- as.data.frame(projectTopic[[w]])
      cTopic <- which(colnames(pTopic) %in% tolower(gsub(" ", "", input$selectCategoryTopic)))
      if (!length(cTopic) == 0) {
        pTopic <- pTopic %>% 
          select(cTopic, project, projecttype) %>% 
          arrange(desc(pTopic[, cTopic]))
        pTopic <- pTopic[1:50, ]
        if (sum(is.na(pTopic$project)) > 0) {
          pTopic <- pTopic[-which(is.na(pTopic$project)), ]
        }
        colnames(pTopic) <- c('Probability', 'Project', 'Project Type')
        pTopic$Label <- paste(1:dim(pTopic)[1], ". ", pTopic$Project, sep = "")
        pTopic$Label <- factor(pTopic$Label, 
                               levels = pTopic$Label[order(pTopic$Probability)])
        return(pTopic)
      } else {return(NULL)}
    } else {return(NULL)}
  })
  
  ### --- OUTPUT output$topProjectsTopic
  output$topProjectsTopic <- renderPlot({
    if (!is.null(pTopic())) {
      ggplot(pTopic(), aes(x = Probability, 
                            y = Label, 
                            label = Label)) +
        geom_line(size = .25, color = "#4c8cff", group = 1) + 
        geom_point(size = 1.5, color = "#4c8cff") + 
        geom_point(size = 1, color = "white") + 
        geom_label_repel(size = 3, segment.size = .25, show.legend = FALSE) +
        ylab("Projects") + xlab("Topic Importance in Project\n(Topic Probability in Project)") +
        theme_minimal() +
        theme(axis.text.x = element_text(angle = 90, size = 12, hjust = 1)) + 
        theme(axis.text.y = element_text(size = 12, hjust = 1)) +
        theme(axis.title.x = element_text(size = 12)) +
        theme(axis.title.y = element_text(size = 12)) %>% 
        withProgress(message = 'Generating plot',
                     min = 0,
                     max = 1,
                     value = 1, {incProgress(amount = 0)})
    } else {return(NULL)}
  }) 
  
  ### ------------------------------------------
  ### --- TAB: tabPanel Projects
  ### ------------------------------------------
  
  ### --- SELECT: update select 'selectProject'
  updateSelectizeInput(session,
                       'selectProject',
                       choices = c(projects, paste("_", projectTypes, sep = "")),
                       selected = "_Wikipedia",
                       server = TRUE)
  
  ### --- REACTIVE: selectedProjects
  selectedProjects <- reactive({
    ### --- selected projects:
    if (!is.null(input$selectProject)) {
      wUnzip <- which(names(unzip_projectTypes) %in% input$selectProject)
      if (length(wUnzip > 0)) {
        selectedProjects <- unname(do.call(c, unzip_projectTypes[wUnzip]))
      }
      wSel <- which(projects %in% input$selectProject)
      if (length(wSel > 0)) {
        selectedProjects <- c(selectedProjects, projects[wSel])
      }
      selectedProjects <- unique(selectedProjects)
      return(selectedProjects)
    } else {return(NULL)}
  })
  
  ### --- OBSERVE: input$applySelection
  observeEvent(input$applySelection, {
  
    #### ---  Chart: projectTopicImportance
    output$projectTopicImportance <- renderPlot({
      # - Plot Frame for projectTopicImportance
      projList <- lapply(names(projectTopic), function(x) {
        cCategory <- which(names(projectTopic) %in% x)
        cProj <- projectTopic[[cCategory]]
        if (sum(which(cProj$project %in% isolate(selectedProjects()))) == 0) {
          return(NULL)
        } else {
          cProj <- cProj %>% 
            filter(project %in% isolate(selectedProjects())) %>% 
            select(starts_with("topic"), project) %>% 
            gather(key = Topic, 
                   value = Probability,
                   starts_with('topic'))
          catName <- gsub("([[:lower:]])([[:upper:]])", "\\1 \\2", names(projectTopic)[cCategory])
          # - FIX THIS:
          catName <- gsub("Workof Art", "Work of Art", catName, fixed = T) 
          cProj$Category <- catName
          cProj <- cProj %>% 
            select(Topic, Probability, Category) %>% 
            group_by(Category, Topic) %>% 
            summarise(Proportion = sum(Probability))
          cProj$Proportion <- round(cProj$Proportion/sum(cProj$Proportion)*100, 2)
          return(cProj)
        }
      })
      wEl <- sapply(projList, function(x) {
        !is.null(x)
        })
      projList <- as.data.frame(rbindlist(projList[wEl]))
      # - factor projList$Topic:
      projList$Topic <- str_to_title(gsub("([[:alpha:]]+)", "\\1 ", projList$Topic))
      projList$Topic <- factor(projList$Topic, levels = unique(projList$Topic))
      # - visualize w. ggplot2
      ggplot(projList,
             aes(x = Topic, 
                 y = Proportion, 
                 label = paste(Proportion, "%", sep = ""))
             ) +
        geom_bar(stat = "identity", width = .1, color = "#4c8cff", fill = "#4c8cff", group = 1) +
        geom_label(size = 4) +
        facet_wrap(~ Category, ncol = 3, scales = "free_y") +
        xlab('Topic') + ylab('Topic Engagement (%)') +
        scale_y_continuous(labels = comma) + 
        theme_minimal() +
        theme(axis.text.x = element_text(angle = 90, size = 12, hjust = 1)) +
        theme(axis.title.x = element_text(size = 12)) +
        theme(axis.title.y = element_text(size = 12)) +
        theme(strip.text = element_text(size = 13)) %>%
        withProgress(message = 'Generating plot',
                     min = 0,
                     max = 1,
                     value = 1, {incProgress(amount = 0)})
    })
  
  }, ignoreNULL = FALSE)
  
  
}) ### --- END shinyServer










