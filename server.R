### ---------------------------------------------------------------------------
### --- WDCM Dashboard Module, v. Beta 0.1
### --- Script: server.R, v. Beta 0.1
### ---------------------------------------------------------------------------

### --- Setup
rm(list = ls())
### --------------------------------
### --- general
library(shiny)
library(RMySQL)
library(data.table)
library(stringr)
library(tidyr)
library(dplyr)
library(reshape2)
### --- compute
library(parallelDist)
library(smacof)
### --- visualization
library(wordcloud)
library(RColorBrewer)
library(visNetwork)
library(rbokeh)
library(networkD3)
library(ggplot2)
library(ggvis)

### --- Server (Session) Scope
### --------------------------------

### --- Credentials
# setwd('/home/goransm/WMDE/WDCM/WDCM_RScripts/WDCM_Dashboard/aux')
setwd('/srv/shiny-server/WDCM_Dashboard/aux')

mySQLCreds <- fread("mySQLCreds.csv", 
                    header = T,
                    drop = 1)

currentStats <- fread("currentStats.csv",
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

### --- fetch wdcm_category_project
q <- "SELECT * FROM wdcm_category_project"
res <- dbSendQuery(con, q)
category_project <- fetch(res, -1)
colnames(category_project) <- str_replace(colnames(category_project), "en_", "")
dbClearResult(res) 
  

### --- fetch wdcm_category_aspect
q <- "SELECT * FROM wdcm_category_aspect"
res <- dbSendQuery(con, q)
category_aspect <- fetch(res, -1)
colnames(category_aspect) <- str_replace(colnames(category_aspect), "en_", "")
dbClearResult(res)

### --- fetch wdcm_category_aspect_l
q <- "SELECT * FROM wdcm_category_aspect_l"
res <- dbSendQuery(con, q)
category_aspect_l <- fetch(res, -1)
colnames(category_aspect_l) <- str_replace(colnames(category_aspect_l), "en_", "")
dbClearResult(res)

### --- fetch wdcm_project_aspect
q <- "SELECT * FROM wdcm_project_aspect"
res <- dbSendQuery(con, q)
project_aspect <- fetch(res, -1)
colnames(project_aspect) <- str_replace(colnames(project_aspect), "en_", "")
dbClearResult(res)

### --- fetch wdcm_project_aspect_l
q <- "SELECT * FROM wdcm_project_aspect_l"
res <- dbSendQuery(con, q)
project_aspect_l <- fetch(res, -1)
colnames(project_aspect_l) <- str_replace(colnames(project_aspect_l), "en_", "")
dbClearResult(res)

### --- fetch wdcm_projecttopic
q <- "SELECT * FROM wdcm_projecttopic;"
res <- dbSendQuery(con, q)
project_topic <- fetch(res, -1)
colnames(project_topic) <- c(paste("Topic", 1:(length(colnames(project_topic))-1), sep = ""), "Project")
dbClearResult(res)

### --- fetch wdcm_project_category_aspect
q <- "SELECT * FROM wdcm_project_category_aspect;"
res <- dbSendQuery(con, q)
project_category_aspect <- fetch(res, -1)
dbClearResult(res)

### --- fetch wdcm_itemfrequency; NOTE: 200 top items
q <- "SELECT * FROM wdcm_itemfrequency LIMIT 200;"
res <- dbSendQuery(con, q)
itemFrequency <- fetch(res, -1)
dbClearResult(res)

### --- fetch wdcm_itemfrequency; NOTE: 200 top items at all
q <- "SELECT * FROM wdcm_itemfrequency LIMIT 200;"
res <- dbSendQuery(con, q)
itemFrequency <- fetch(res, -1)
dbClearResult(res)

### --- fetch wdcm_topic100items; NOTE: 100 top items per topic
res <- dbSendQuery(con, 'set character set "utf8"')
dbClearResult(res)
q <- "SELECT * FROM wdcm_topic100items;"
res <- dbSendQuery(con, q)
topic100items <- fetch(res, -1)
dbClearResult(res)

### --- fetch wdcm_itemtopic; 

q <- "SELECT * FROM wdcm_itemtopic;"
res <- dbSendQuery(con, q)
itemTopic <- fetch(res, -1)
dbClearResult(res)

### --- Disconnect
dbDisconnect(con)

### --- fetch wdcm_itemtopic_* and wdcm_projecttopic_* tables
### --- stored locally:
setwd('/home/goransm/WMDE/WDCM/WDCM_RScripts/WDCM_Dashboard/data')
fileNames <- list.files()
itopicFiles <- fileNames[which(grepl("wdcm_itemtopic_", fileNames, fixed = T))]
ptopicFiles <- fileNames[which(grepl("wdcm_projecttopic_", fileNames, fixed = T))]
itemTopicList <- list()
projectTopicList <- list()
for (i in 1:length(itopicFiles)) {
  itemTopicList[[i]] <- as.data.frame(fread(itopicFiles[i], header = T))
  projectTopicList[[i]] <- as.data.frame(fread(ptopicFiles[i], header = T))
}
names(itemTopicList) <- itopicFiles
names(projectTopicList) <- ptopicFiles

### --- constants; NOTE: for project-dependent counts
allProjects <- unique(project_aspect$project)
allCategories <- unique(category_project$category)
allAspects_l <- unique(project_aspect_l$aspect)
allAspects_l <- gsub("^L$", "L_All", allAspects_l)
allLanAspects <- unique(project_aspect$aspect[which(grepl("^L", project_aspect$aspect))])

### --- constants2; NOTE: for category-project-aspect counts
allProjects2 <- unique(project_category_aspect$en_project)
allCategories2 <- unique(project_category_aspect$en_category)
allLanAspects2 <- unique(project_category_aspect$en_aspect[which(grepl("^L", project_category_aspect$en_aspect))])

### --- searchTable
projectAspect <- as.data.frame(acast(data = project_aspect,
                                     formula = project ~ aspect,
                                     value.var = 'count',
                                     fill = 0))
projectAspect$L_All <- rowSums(projectAspect)
projectAspect$Project <- rownames(projectAspect)
searchTable <- project_topic %>% 
  dplyr::left_join(projectAspect, by = 'Project')
rm(projectAspect)
categoryProject <- as.data.frame(acast(data = category_project,
                                       formula = project ~ category,
                                       value.var = 'count',
                                       fill = 0))
colnames(categoryProject) <- paste("Cat_", colnames(categoryProject), sep = "")
categoryProject$Project <- rownames(categoryProject)
searchTable <- searchTable %>% 
  dplyr::left_join(categoryProject, by = 'Project')
rm(categoryProject)
projectCount <- category_project %>% 
  dplyr::group_by(project) %>% 
  dplyr::summarise(projectCount = sum(count))
colnames(projectCount)[1] <- 'Project'
searchTable <- searchTable %>% 
  dplyr::left_join(projectCount, by = 'Project')
rm(projectCount)

### --- shinyServer
shinyServer(function(input, output, session) {
  
  
  ### ----------------------------------
  ### --- TAB: Semantic Model
  ### ----------------------------------
  
  ### --- SELECT: update select 'selectTopics'
  updateSelectizeInput(session,
                       'selectTopics',
                       choices = paste("topic", 1:currentStats$numtopic, sep = ""),
                       selected = 'topic1',
                       server = TRUE)
  
  ### --- SELECT: update select 'selectCategory'
  updateSelectizeInput(session,
                       'selectCategory',
                       "Select Item Category:",
                       choices = unique(category_project$category),
                       selected = unique(category_project$category)[1],
                       server = TRUE)
  
  ### --- SELECT: updateSelectizeInput 'selectCatTopic'
  output$selectCatTopic <-
    renderUI({
      if ((is.null(input$selectCategory))|(length(input$selectCategory)==0)) {
        updateSelectInput(inputId = "selectCatTopic",
                          label = "Select Category Topic:",
                          choices = NULL,
                          selected = NULL)
      } else {
        wFile <- which(grepl(as.character(input$selectCategory), names(itemTopicList), fixed = T))
        if ((is.null(wFile))|(length(wFile)!=1)) {
          selectInput(inputId = "selectCatTopic",
                      label = "Select Category Topic:",
                      choices = NULL,
                      selected = NULL)
        } else {
          topChoices <- colnames(itemTopicList[[wFile]])[which(grepl("topic", colnames(itemTopicList[[wFile]]), fixed = T))]
          if ((is.null(topChoices))|(length(topChoices)==0)) {
            selectInput(inputId = "selectCatTopic",
                        label = "Select Category Topic:",
                        choices = NULL,
                        selected = NULL)
          } else {
            selectInput(inputId = "selectCatTopic",
                        label = "Select Category Topic:",
                        choices = as.character(topChoices),
                        selected = as.character(topChoices[1]))
          }
        }
      }
    })
  
  ### --- valueBox: Items
  # output$numItemsBox
  output$numItemsBox <- renderValueBox({
    valueBox(
      value = as.character(currentStats$numitem),
      subtitle = "Items",
      icon = icon("bars", lib = "font-awesome"),
      color = "blue"
    )
  }) # END output$numItemsBox
  
  ### --- valueBox: Projects
  # output$numProjectsBox
  output$numProjectsBox <- renderValueBox({
    valueBox(
      value = currentStats$numproject,
      subtitle = "Projects",
      icon = icon("bars", lib = "font-awesome"),
      color = "blue"
    )
  }) # END output$numProjectsBox
  
  ### --- valueBox: Categories
  # output$numCategoriesBox
  output$numCategoriesBox <- renderValueBox({
    valueBox(
      value = currentStats$numcategory,
      subtitle = "Semantic Categories",
      icon = icon("bars", lib = "font-awesome"),
      color = "blue"
    )
  }) # END output$numCategoriesBox
  
  ### --- wordcloud: itemFrequency
  output$itemFrequencyWordCloud <- renderPlot({
    wordC <- wordcloud(words = itemFrequency$en_label[1:200],
                       freq = log(itemFrequency$en_count[1:200]),
                       min.freq = 1, 
                       max.words = 200,
                       rot.per = .1,
                       scale=c(1.25, .005),
                       random.order = TRUE,
                       colors = brewer.pal(n = 9, 
                                           name = 'GnBu')[7:9]
                       ) %>%
      withProgress(message = 'Generating plot',
                   min = 0,
                   max = 1,
                   value = 1, {incProgress(amount = 0)})
    wordC
  })
  
  ### --- barplot: itemFrequencyBarPlot
  output$itemFrequencyBarPlot <- renderPlot({
    plotFrame <- data.frame(words = itemFrequency$en_label[1:30],
                            freq = itemFrequency$en_count[1:30],
                            stringsAsFactors = F)
    plotFrame$words <- factor(plotFrame$words, levels = plotFrame$words[order(-plotFrame$freq)]) 
    ggplot(plotFrame, aes(x = words, y = freq)) +
      geom_bar(stat = "identity", width = .6, fill = "#4c8cff") + 
      xlab("Items") + ylab("Entity Usage") +
      theme_bw() +
      theme(axis.text.x = element_text(angle = 90, size = 9, hjust = 1)) +
      theme(axis.title.x = element_text(size = 12)) +
      theme(axis.title.y = element_text(size = 12)) %>% 
      withProgress(message = 'Generating plot',
                   min = 0,
                   max = 1,
                   value = 1, {incProgress(amount = 0)})
  })
  
  ### --- wordcloud: projectVolumeWordCloud
  output$projectVolumeWordCloud <- renderPlot({
    wcFrame <- category_project %>% 
      dplyr::group_by(project) %>% 
      dplyr::summarise(total = sum(count))
    wordC <- wordcloud(words = wcFrame$project,
                       freq = wcFrame$total,
                       min.freq = 1, 
                       max.words = 100,
                       rot.per = .1,
                       scale=c(5, 1.25),
                       random.order = TRUE,
                       colors = brewer.pal(n = 9, 
                                           name = 'GnBu')[7:9]
    ) %>%
      withProgress(message = 'Generating plot',
                   min = 0,
                   max = 1,
                   value = 1, {incProgress(amount = 0)})
    wordC
  })
  
  ### --- barplot: projectVolumeBarPlot
  output$projectVolumeBarPlot <- renderPlot({
    plotFrame <- category_project %>%
      dplyr::group_by(project) %>%
      dplyr::summarise(total = sum(count)) %>% 
      dplyr::arrange(desc(total))
    plotFrame <- plotFrame[1:20, ]
    plotFrame$project <- factor(plotFrame$project, levels = plotFrame$project[order(-plotFrame$total)]) 
    ggplot(plotFrame, aes(x = project, y = total)) +
      geom_bar(stat = "identity", width = .6, fill = "#4c8cff") + 
      xlab("Projects") + ylab("Entity Usage") +
      theme_bw() +
      theme(axis.text.x = element_text(angle = 90, size = 9, hjust = 1)) +
      theme(axis.title.x = element_text(size = 12)) +
      theme(axis.title.y = element_text(size = 12)) %>% 
      withProgress(message = 'Generating plot',
                   min = 0,
                   max = 1,
                   value = 1, {incProgress(amount = 0)})
  })
  
  ### --- wordcloud: categoryVolumeWordCloud
  output$categoryVolumeWordCloud <- renderPlot({
    wcFrame <- category_project %>% 
      dplyr::group_by(category) %>% 
      dplyr::summarise(total = sum(count))
    wordC <- wordcloud(words = wcFrame$category,
                       freq = wcFrame$total,
                       min.freq = 1, 
                       max.words = 100,
                       rot.per = .1,
                       scale=c(3, 1.25),
                       random.order = TRUE,
                       colors = brewer.pal(n = 9, 
                                           name = 'GnBu')[7:9]
    ) %>%
      withProgress(message = 'Generating plot',
                   min = 0,
                   max = 1,
                   value = 1, {incProgress(amount = 0)})
    wordC
  })
  
  ### --- barplot: categoryVolumeBarPlot
  output$categoryVolumeBarPlot <- renderPlot({
    plotFrame <- category_project %>% 
      dplyr::group_by(category) %>% 
      dplyr::summarise(total = sum(count)) %>%
      dplyr::arrange(desc(total))
    plotFrame$category <- factor(plotFrame$category, levels = plotFrame$category[order(-plotFrame$total)]) 
    ggplot(plotFrame, aes(x = category, y = total)) +
      geom_bar(stat = "identity", width = .6, fill = "#4c8cff") + 
      xlab("Categories") + ylab("Entity Usage") +
      theme_bw() +
      theme(axis.text.x = element_text(angle = 90, size = 9, hjust = 1)) +
      theme(axis.title.x = element_text(size = 12)) +
      theme(axis.title.y = element_text(size = 12)) %>% 
      withProgress(message = 'Generating plot',
                   min = 0,
                   max = 1,
                   value = 1, {incProgress(amount = 0)})
  })
  
  ### --- wordcloud: aspectVolumeWordCloud
  output$aspectVolumeWordCloud <- renderPlot({
    wcFrame <- category_aspect %>% 
      dplyr::group_by(aspect) %>% 
      dplyr::summarise(total = sum(count))
    wordC <- wordcloud(words = wcFrame$aspect,
                       freq = wcFrame$total,
                       min.freq = 1, 
                       max.words = 100,
                       rot.per = .1,
                       scale=c(5, 1.25),
                       random.order = TRUE,
                       colors = brewer.pal(n = 9, 
                                           name = 'GnBu')[7:9]
    ) %>%
      withProgress(message = 'Generating plot',
                   min = 0,
                   max = 1,
                   value = 1, {incProgress(amount = 0)})
    wordC
  })
  
  ### --- barplot: aspectsBarPlot
  output$aspectsBarPlot <- renderPlot({
    plotFrame <- category_aspect %>% 
      dplyr::group_by(aspect) %>% 
      dplyr::summarise(total = sum(count)) %>%
      dplyr::arrange(desc(total))
    plotFrame <- plotFrame[1:30, ]
    plotFrame$aspect <- factor(plotFrame$aspect, levels = plotFrame$aspect[order(-plotFrame$total)]) 
    ggplot(plotFrame, aes(x = aspect, y = total)) +
      geom_bar(stat = "identity", width = .6, fill = "#4c8cff") + 
      xlab("Aspects") + ylab("Entity Usage") +
      theme_bw() +
      theme(axis.text.x = element_text(angle = 90, size = 9, hjust = 1)) +
      theme(axis.title.x = element_text(size = 12)) +
      theme(axis.title.y = element_text(size = 12)) %>% 
      withProgress(message = 'Generating plot',
                   min = 0,
                   max = 1,
                   value = 1, {incProgress(amount = 0)})
  })
  
  ## --- wordcloud: catTopicItemWordCloud
  observeEvent(input$selectCatTopic, {

    if (!(grepl("topic", input$selectCatTopic))) {
      return(NULL)
    } else {

      output$catTopicItemWordCloud <- renderPlot({
        wFile <- which(grepl(input$selectCategory, names(itemTopicList), fixed = T))
        wT <- itemTopicList[[wFile]] %>%
          dplyr::select(contains(input$selectCatTopic), en_label)
        wT <- wT[order(-wT[, 1]), ][1:100, ]

        wordC <- wordcloud(words = wT$en_label,
                           freq = wT[, 1],
                           min.freq = 0,
                           max.words = 100,
                           rot.per = .25,
                           scale=c(3.5, .75),
                           random.order = TRUE,
                           colors = brewer.pal(n = 9,
                                               name = 'GnBu')[7:9]
        ) %>%
          withProgress(message = 'Generating plot',
                       min = 0,
                       max = 1,
                       value = 1, {incProgress(amount = 0)})
        wordC
      })
    }
  })
  
  ## --- barplot: topicItemsBarPlot
  observeEvent(input$selectCatTopic, {
    
    if (!(grepl("topic", input$selectCatTopic))) {
      return(NULL)
    } else {
      
      output$topicItemsBarPlot <- renderPlot({
        wFile <- which(grepl(input$selectCategory, names(itemTopicList), fixed = T))
        wT <- itemTopicList[[wFile]] %>%
          dplyr::select(contains(input$selectCatTopic), en_label)
        wT <- wT[order(-wT[, 1]), ][1:30, ]
        plotFrame <- data.frame(words = wT$en_label,
                                freq = wT[, 1], 
                                stringsAsFactors = F)
        plotFrame$words <- factor(plotFrame$words, levels = plotFrame$words[order(-plotFrame$freq)]) 
        ggplot(plotFrame, aes(x = words, y = freq)) +
          geom_bar(stat = "identity", width = .6, fill = "#4c8cff") + 
          xlab("Items") + ylab("Entity Probablity in Topic") +
          theme_bw() +
          theme(axis.text.x = element_text(angle = 90, size = 9, hjust = 1)) +
          theme(axis.title.x = element_text(size = 12)) +
          theme(axis.title.y = element_text(size = 12)) %>% 
          withProgress(message = 'Generating plot',
                       min = 0,
                       max = 1,
                       value = 1, {incProgress(amount = 0)})
        })
    }
  })
  
  ### --- output$catTopicProjectsCloud
  observeEvent(input$selectCatTopic, {
    
    if (!(grepl("topic", input$selectCatTopic))) {
      return(NULL)
    } else {
      
      output$catTopicProjectsCloud <- renderPlot({
        wFile <- which(grepl(input$selectCategory, names(projectTopicList), fixed = T))
        wT <- projectTopicList[[wFile]] %>%
          dplyr::select(contains(input$selectCatTopic), project)
        wT <- wT[order(-wT[, 1]), ]
        
        wordC <- wordcloud(words = wT$project,
                           freq = wT[, 1],
                           min.freq = 0,
                           max.words = 100,
                           rot.per = .25,
                           scale=c(3.5, .75),
                           random.order = TRUE,
                           colors = brewer.pal(n = 9,
                                               name = 'GnBu')[7:9]
        ) %>%
          withProgress(message = 'Generating plot',
                       min = 0,
                       max = 1,
                       value = 1, {incProgress(amount = 0)})
        wordC
      })
    }
  })
  
  # - output$catTopicProjectsBarPlot
  observeEvent(input$selectCatTopic, {
    
    if (!(grepl("topic", input$selectCatTopic))) {
      return(NULL)
    } else {
      
      output$catTopicProjectsBarPlot <- renderPlot({
        wFile <- which(grepl(input$selectCategory, names(projectTopicList), fixed = T))
        wT <- projectTopicList[[wFile]] %>%
          dplyr::select(contains(input$selectCatTopic), project)
        wT <- wT[order(-wT[, 1]), ]
        plotFrame <- data.frame(project = wT$project,
                                freq = wT[, 1], 
                                stringsAsFactors = F)
        plotFrame$project <- factor(plotFrame$project, levels = plotFrame$project[order(-plotFrame$freq)]) 
        ggplot(plotFrame, aes(x = project, y = freq)) +
          geom_bar(stat = "identity", width = .6, fill = "#4c8cff") + 
          xlab("Project") + ylab("Topic probability in project") +
          theme_bw() +
          theme(axis.text.x = element_text(angle = 90, size = 9, hjust = 1)) +
          theme(axis.title.x = element_text(size = 12)) +
          theme(axis.title.y = element_text(size = 12)) %>% 
          withProgress(message = 'Generating plot',
                       min = 0,
                       max = 1,
                       value = 1, {incProgress(amount = 0)})
      })
    }
  })
  
  ### --- output$topicStructure, output$catProjectStructure
  ### --- output$topicMap, output$catProjectMap
  ### --- output$topicHierarchy, output$catProjectHierarchy
  observeEvent(input$selectCatTopic, {
    
    if(!(grepl("topic", input$selectCatTopic))) {
      return(NULL)
    } else {
      
      # - output$topicStructure
      output$topicStructure <- renderVisNetwork({
        
        wFile <- which(grepl(input$selectCategory, names(itemTopicList), fixed = T))
        wT <- itemTopicList[[wFile]] %>%
          dplyr::select(contains('topic'), en_label)
        wT <- wT[order(-wT[which(colnames(wT) %in% input$selectCatTopic)]), ][1:100, ]
        itemNames <- wT$en_label
        root <- dplyr::select(wT, starts_with("topic"))
        # - normalization: Luce's choice axiom
        root <- apply(root, 2, function(x) {x/sum(x)})
        root <- as.matrix(parDist(root, method = "hellinger"))
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
                    startStabilizing = "function() {this.moveTo({scale:0.5})}") %>%
          visPhysics(stabilization = FALSE) %>%
          withProgress(message = 'Generating plot',
                       min = 0,
                       max = 1,
                       value = 1, {incProgress(amount = 0)})
        })
      
      # - output$topicMap
      output$topicMap <- renderRbokeh({
        wFile <- which(grepl(input$selectCategory, names(itemTopicList), fixed = T))
        wT <- itemTopicList[[wFile]] %>%
          dplyr::select(contains('topic'), en_label)
        wT <- wT[order(-wT[which(colnames(wT) %in% input$selectCatTopic)]), ][1:100, ]
        itemNames <- wT$en_label
        root <- dplyr::select(wT, starts_with("topic"))
        # - normalization: Luce's choice axiom
        root <- apply(root, 2, function(x) {x/sum(x)})
        root <- as.matrix(parDist(root, method = "hellinger"))
        rownames(root) <- itemNames
        colnames(root) <- itemNames
        # - multidimensional scaling w. {smacof}
        root <- smacofSym(root, ndim = 2, type = "ordinal")
        mapFrame <- as.data.frame(root$conf)
        mapFrame$Item <- rownames(mapFrame)
        outFig <- figure() %>%
          ly_points(D1, D2, data = mapFrame,
                    hover = list(Item)) %>%
          withProgress(message = 'Generating plot',
                       min = 0, 
                       max = 1,
                       value = 1, {incProgress(amount = 0)})
        outFig
      })
      
      # - output$topicHierarchy
      output$topicHierarchy <- renderRadialNetwork({
        wFile <- which(grepl(input$selectCategory, names(itemTopicList), fixed = T))
        wT <- itemTopicList[[wFile]] %>%
          dplyr::select(contains('topic'), en_label)
        wT <- wT[order(-wT[which(colnames(wT) %in% input$selectCatTopic)]), ][1:100, ]
        itemNames <- wT$en_label
        wLong <- which(nchar(itemNames)>40)
        itemNames[wLong] <- paste(str_sub(itemNames[wLong], 1, 30), " ...", sep = "")
        root <- dplyr::select(wT, starts_with("topic"))
        # - normalization: Luce's choice axiom
        root <- apply(root, 2, function(x) {x/sum(x)})
        root <- parDist(root, method = "hellinger")
        # - hclust:
        root <- hclust(root, method = "ward.D")
        root$labels <- itemNames
        root <- as.radialNetwork(root)
        outFig <- radialNetwork(root)
        outFig
      })
      
      # - output$catProjectStructure
      output$catProjectStructure <- renderVisNetwork({
        
        wFile <- which(grepl(input$selectCategory, names(projectTopicList), fixed = T))
        wT <- projectTopicList[[wFile]] %>%
          dplyr::select(contains('topic'), project)
        projNames <- wT$project
        root <- dplyr::select(wT, starts_with("topic"))
        maxProjTopics <- apply(root, 1, function(x) {
          which.max(x)[1]
        })
        titles <- paste("topic ", maxProjTopics, sep = "") 
        nodeColors <- brewer.pal(10, 'Accent')[1:length(unique(maxProjTopics))] 
        nodeColors <- nodeColors[maxProjTopics]
        # - normalization: Luce's choice axiom
        root <- apply(root, 2, function(x) {x/sum(x)})
        root <- as.matrix(parDist(root, method = "hellinger"))
        rownames(root) <- projNames
        colnames(root) <- projNames
        indexMinDist <- sapply(rownames(root), function(x) {
          w <- which(rownames(root) %in% x)
          y <- sort(root[w, -w], decreasing = T)
          names(y)[length(y)]
        })
        id <- 1:length(colnames(root))
        label <- colnames(root)
        nodes <- data.frame(id = id,
                            label = label,
                            title = titles,
                            color.background = nodeColors,
                            color.border = 'black',
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
                    startStabilizing = "function() {this.moveTo({scale:0.5})}") %>%
          visPhysics(stabilization = FALSE) %>%
          withProgress(message = 'Generating plot',
                       min = 0,
                       max = 1,
                       value = 1, {incProgress(amount = 0)})
      })
      
      # - output$catProjectMap
      output$catProjectMap <- renderRbokeh({
        wFile <- which(grepl(input$selectCategory, names(projectTopicList), fixed = T))
        wT <- projectTopicList[[wFile]] %>%
          dplyr::select(contains('topic'), project)
        projNames <- wT$project
        root <- dplyr::select(wT, starts_with("topic"))
        maxProjTopics <- apply(root, 1, function(x) {
          which.max(x)[1]
        })
        titles <- paste("topic ", maxProjTopics, sep = "") 
        # - normalization: Luce's choice axiom
        root <- apply(root, 2, function(x) {x/sum(x)})
        root <- as.matrix(parDist(root, method = "hellinger"))
        rownames(root) <- projNames
        colnames(root) <- projNames
        # - multidimensional scaling w. {smacof}
        root <- smacofSym(root, ndim = 2, type = "ordinal")
        mapFrame <- as.data.frame(root$conf)
        mapFrame$Project <- rownames(mapFrame)
        mapFrame$Topic <- titles
        outFig <- figure() %>%
          ly_points(D1, D2, data = mapFrame,
                    color = Topic, glyph = Topic,
                    hover = list(Project)) %>%
          withProgress(message = 'Generating plot',
                       min = 0, 
                       max = 1,
                       value = 1, {incProgress(amount = 0)})
        outFig
      })
      
      # - output$catProjectHierarchy
      output$catProjectHierarchy <- renderRadialNetwork({
        wFile <- which(grepl(input$selectCategory, names(projectTopicList), fixed = T))
        wT <- projectTopicList[[wFile]] %>%
          dplyr::select(contains('topic'), project)
        wT <- wT[order(-wT[which(colnames(wT) %in% input$selectCatTopic)]), ]
        itemNames <- wT$project
        wLong <- which(nchar(itemNames)>40)
        itemNames[wLong] <- paste(str_sub(itemNames[wLong], 1, 30), " ...", sep = "")
        root <- dplyr::select(wT, starts_with("topic"))
        # - normalization: Luce's choice axiom
        root <- apply(root, 2, function(x) {x/sum(x)})
        root <- parDist(root, method = "hellinger")
        # - hclust:
        root <- hclust(root, method = "ward.D")
        root$labels <- itemNames
        root <- as.radialNetwork(root)
        outFig <- radialNetwork(root)
        outFig
      })
      
      }
  })
  
  
  ### ----------------------------------
  ### --- TAB: Projects
  ### ----------------------------------
  
    
  ### --- SELECT: update selectize 'projects'
  updateSelectizeInput(session,
                       'projects',
                       choices = allProjects,
                       server = TRUE)
  
  ### --- SELECT: update selectize 'categories'
  updateSelectizeInput(session,
                       'categories',
                       choices = allCategories,
                       server = TRUE)
  
  ### --- SELECT: update selectize 'aspects'
  updateSelectizeInput(session,
                       'aspects',
                       choices = allAspects_l,
                       server = TRUE)
  
  ### --- SELECT: update selectize 'languages'
  updateSelectizeInput(session,
                       'languages',
                       choices = allLanAspects,
                       server = TRUE)
  
  ### --- SELECT: indexTable
  ### --- NOTE: for project-dependent counts
  indexes <- reactive({
    if (length(input$projects) > 0) {
      wProjects <- allProjects[which(allProjects %in% reactive({input$projects})())]
    } else {wProjects <- allProjects}
    if (length(input$categories) > 0) {
      wCategories <- paste("Cat_", allCategories[which(allCategories %in% input$categories)], sep = "")
    } else {wCategories <- paste("Cat_", allCategories, sep = "")}
    if (length(input$aspects) > 0) {
      wAspects <- allAspects_l[which(allAspects_l %in% input$aspects)]
    } else {wAspects <- allAspects_l}
    if (length(input$languages) > 0) {
      wLanguages <- allLanAspects[which(allLanAspects %in% input$languages)]
    } else {wLanguages <- allLanAspects}
    list(wProjects, wCategories, wAspects, wLanguages)
  })
  indexTable <- reactive({
    sT <- searchTable %>%
      filter(Project %in% indexes()[[1]])
    sT <- sT[, c('Project', unlist(indexes()[2:4]))]
    sT$projectCount <- searchTable$projectCount[which(searchTable$Project %in% indexes()[[1]])] %>% 
      withProgress(message = 'Generating data', 
                   min = 0, 
                   max = 1, 
                   value = 1, {incProgress(amount = 0)})
    sT
  })
  
  ### --- SELECT: indexTable2
  ### --- NOTE: for project-category-aspect counts
  indexes2 <- reactive({
    if (length(input$projects) > 0) {
      wProjects2 <- allProjects2[which(allProjects2 %in% reactive({input$projects})())]
    } else {wProjects2 <- allProjects2}
    if (length(input$categories) > 0) {
      wCategories2 <- allCategories2[which(allCategories2 %in% input$categories)]
    } else {wCategories2 <- allCategories2}
    if (length(input$languages) > 0) {
      wLanguages2 <- allLanAspects2[which(allLanAspects2 %in% input$languages)]
    } else {wLanguages2 <- allLanAspects2}
    list(wProjects2, wCategories2, wLanguages2)
  })
  indexTable2 <- reactive({
    sT <- project_category_aspect %>%
      filter(en_project %in% indexes2()[[1]], 
             en_category %in% indexes2()[[2]],
             en_aspect %in% indexes2()[[3]])
    colnames(sT) <- c('Project', 'Category', 'Aspect', 'count') %>% 
      withProgress(message = 'Generating data', 
                   min = 0, 
                   max = 1, 
                   value = 1, {incProgress(amount = 0)})
    sT
  })
  
  ### --- SELECT: indexTable3
  ### --- NOTE: for project-category-aspect counts
  indexTable3 <- reactive({
    sT <- project_category_aspect %>%
      filter(en_project %in% indexes2()[[1]], 
             en_category %in% indexes2()[[2]],
             en_aspect %in% c('O', 'S', 'T', 'X', indexes2()[[3]]))
    colnames(sT) <- c('Project', 'Category', 'Aspect', 'count') %>% 
      withProgress(message = 'Generating data', 
                   min = 0, 
                   max = 1, 
                   value = 1, {incProgress(amount = 0)})
    sT
  })

  ### ---------------------------------------------------------------------
  ### ---  Tabulations and Cross-Tabulations: ggvis charts, data downloads
  ### ---------------------------------------------------------------------
  
  ### ----------------------------------
  ### --- TAB PANEL: Tabulations
  ### ----------------------------------

  # - OBSERVE: input$applySelection
  observeEvent(input$applySelection, {
    
    #### ---  Chart: tabulations_projectsChart
    output$tabulations_projectsChart <- renderPlot({
      # - Chart Frame for output$tabulations_projectsChart
      tabulations_projectsChart_Frame <- isolate(indexTable()) %>%
        dplyr::select(Project, projectCount)
      colnames(tabulations_projectsChart_Frame) <- c('project', 'count')
      tabulations_projectsChart_Frame <- tabulations_projectsChart_Frame %>%
        dplyr::mutate(key = paste(round(count/1000000, 1), "M", sep = "")) %>% 
        dplyr::arrange(desc(count))
      if (dim(tabulations_projectsChart_Frame)[1] > 15) {
        sumOther <- sum(tabulations_projectsChart_Frame$count[16:dim(tabulations_projectsChart_Frame)[1]])
        tabulations_projectsChart_Frame <- tabulations_projectsChart_Frame[1:15, ]
        tabulations_projectsChart_Frame <- rbind(tabulations_projectsChart_Frame, data.frame(project = 'other',
                                                                                             count = sumOther,
                                                                                             key = paste(round(sumOther/1000000, 1), "M", sep = ""))
        )
        tabulations_projectsChart_Frame <- dplyr::arrange(tabulations_projectsChart_Frame, desc(count))
      }
      tabulations_projectsChart_Frame$project <- factor(tabulations_projectsChart_Frame$project, 
                            levels = tabulations_projectsChart_Frame$project[order(-tabulations_projectsChart_Frame$count)])
      # - Plot
      ggplot(tabulations_projectsChart_Frame,
             aes(x = project, y = count, label = key)) +
        geom_bar(stat = "identity", width = .6, fill = "#4c8cff") +
        geom_label(size = 3, vjust = -.1) +
        xlab('Projects') + ylab('Entity Usage') +
        ylim(0, max(tabulations_projectsChart_Frame$count) + .1*max(tabulations_projectsChart_Frame$count)) +
        theme_bw() +
        theme(axis.text.x = element_text(angle = 90, size = 12, hjust = 1)) +
        theme(axis.title.x = element_text(size = 12)) +
        theme(axis.title.y = element_text(size = 12)) +
        theme(plot.title = element_text(size = 15)) %>% 
        withProgress(message = 'Generating plot',
                     min = 0,
                     max = 1,
                     value = 1, {incProgress(amount = 0)})
    })
    # - Download Frame: tabulations_projectsChart
    tabulations_projectsDownload_Frame <- reactive({
      out <- indexTable() %>%
        dplyr::select(Project, projectCount)
      colnames(out) <- c('project', 'count')
      out <- out %>%
        dplyr::arrange(desc(count)) %>%
        as.data.frame()
      out
    })
    # - Download: tabulations_projectsChart
    output$tabulations_projectsDownload_Frame <- downloadHandler(
      filename = function() {
        'WDCM_Data.csv'},
      content = function(file) {
        write.csv(tabulations_projectsDownload_Frame(), 
                  file,
                  quote = FALSE,
                  row.names = FALSE)
      },
      contentType = "text/csv"
    )
    
    ### --- Chart: tabulations_categoriesChart<
    output$tabulations_categoriesChart <- renderPlot({
      
      # - Chart Frame for output$tabulations_categoriesChart
      categories <- isolate(indexTable()) %>%
        dplyr::select(starts_with('Cat_')) %>%
        colnames()
      count <- isolate(indexTable()) %>%
        dplyr::select(starts_with('Cat_')) %>%
        colSums()
      tabulations_categoriesChart_Frame <- data.frame(category = categories,
                                                      count = count)
      tabulations_categoriesChart_Frame$category <- gsub("Cat_", "", tabulations_categoriesChart_Frame$category, fixed = T)
      tabulations_categoriesChart_Frame <- tabulations_categoriesChart_Frame %>%
        dplyr::mutate(key = paste(round(count/1000000, 1), "M", sep = "")) %>%
        dplyr::arrange(desc(count))
        if (dim(tabulations_categoriesChart_Frame)[1] > 15) {
          sumOther <- sum(tabulations_categoriesChart_Frame$count[16:dim(tabulations_categoriesChart_Frame)[1]])
          tabulations_categoriesChart_Frame <- tabulations_categoriesChart_Frame[1:15, ]
          tabulations_categoriesChart_Frame <- 
            rbind(tabulations_categoriesChart_Frame, data.frame(project = 'other',
                                                                count = sumOther,
                                                                key = paste(round(sumOther/1000000, 1), "M", sep = ""))
            )
          tabulations_categoriesChart_Frame <- dplyr::arrange(tabulations_categoriesChart_Frame, desc(count))
        }
      tabulations_categoriesChart_Frame$category <- factor(tabulations_categoriesChart_Frame$category,
                                                           levels = tabulations_categoriesChart_Frame$category[order(-tabulations_categoriesChart_Frame$count)])
      
      # - Plot:
      ggplot(tabulations_categoriesChart_Frame,
             aes(x = category, y = count, label = key)) +
        geom_bar(stat = "identity", width = .6, fill = "#4c8cff") +
        geom_label(size = 3, vjust = -.1) +
        xlab('Categories') + ylab('Entity Usage') +
        ylim(0, max(tabulations_categoriesChart_Frame$count) + .1*max(tabulations_categoriesChart_Frame$count)) +
        theme_bw() +
        theme(axis.text.x = element_text(angle = 90, size = 12, hjust = 1)) +
        theme(axis.title.x = element_text(size = 12)) +
        theme(axis.title.y = element_text(size = 12)) +
        theme(plot.title = element_text(size = 15)) %>% 
        withProgress(message = 'Generating plot',
                     min = 0,
                     max = 1,
                     value = 1, {incProgress(amount = 0)})
    })
    # - Download Frame: tabulations_categoriesChart
    tabulations_categoriesDownload_Frame <- reactive({
      categories <- indexTable() %>%
        dplyr::select(starts_with('Cat_')) %>%
        colnames()
      count <- indexTable() %>%
        dplyr::select(starts_with('Cat_')) %>%
        colSums()
      out <- data.frame(category = categories,
                        count = count)
      out$category <- gsub("Cat_", "", out$category, fixed = T)
      out <- out %>%
        dplyr::arrange(desc(count))
      out
    })
    # - Download: tabulations_categoriesChart
    output$tabulations_categoriesDownload_Frame <- downloadHandler(
      filename = function() {
        'WDCM_Data.csv'},
      content = function(file) {
        write.csv(tabulations_categoriesDownload_Frame(),
                  file,
                  quote = FALSE,
                  row.names=FALSE)
      }
    )
    
    ### --- Chart: tabulations_aspectsChart
    output$tabulations_aspectsChart <- renderPlot({
      
      # - Chart Frame for output$tabulations_aspectsChart
      wAsp <- which(c('O', 'S', 'T', 'X', 'L_All') %in% colnames(isolate(indexTable())))
      wAsp <- c('O', 'S', 'T', 'X', 'L_All')[wAsp]
      aspects <- isolate(indexTable()) %>%
        dplyr::select(wAsp) %>%
        colnames()
      count <- isolate(indexTable()) %>%
        dplyr::select(wAsp) %>%
        colSums()
      tabulations_aspectsChart_Frame <- data.frame(aspect = aspects,
                                                   count = count)
      tabulations_aspectsChart_Frame <- tabulations_aspectsChart_Frame %>%
        dplyr::mutate(key = paste(round(count/1000000, 1), "M", sep = "")) %>%
        dplyr::arrange(desc(count))
      if (dim(tabulations_aspectsChart_Frame)[1] > 15) {
        sumOther <- sum(tabulations_aspectsChart_Frame$count[16:dim(tabulations_aspectsChart_Frame)[1]])
        tabulations_aspectsChart_Frame <- tabulations_aspectsChart_Frame[1:15, ]
        tabulations_aspectsChart_Frame <- 
          rbind(tabulations_aspectsChart_Frame, data.frame(aspect = 'other',
                                                           count = sumOther,
                                                           key = paste(round(sumOther/1000000, 1), "M", sep = ""))
          )
        tabulations_aspectsChart_Frame <- dplyr::arrange(tabulations_aspectsChart_Frame, desc(count))
      }
      tabulations_aspectsChart_Frame$aspect <- 
        factor(tabulations_aspectsChart_Frame$aspect,
               levels = tabulations_aspectsChart_Frame$aspect[order(-tabulations_aspectsChart_Frame$count)])
      
      # - Plot:
      ggplot(tabulations_aspectsChart_Frame,
             aes(x = aspect, y = count, label = key)) +
        geom_bar(stat = "identity", width = .6, fill = "#4c8cff") +
        geom_label(size = 3, vjust = -.1) +
        xlab('Aspects') + ylab('Entity Usage') +
        ylim(0, max(tabulations_aspectsChart_Frame$count) + .1*max(tabulations_aspectsChart_Frame$count)) +
        theme_bw() +
        theme(axis.text.x = element_text(angle = 0, size = 12, hjust = 1)) +
        theme(axis.title.x = element_text(size = 12)) +
        theme(axis.title.y = element_text(size = 12)) +
        theme(plot.title = element_text(size = 15)) %>% 
        withProgress(message = 'Generating plot',
                     min = 0,
                     max = 1,
                     value = 1, {incProgress(amount = 0)})
    })
    # - Download Frame: tabulations_aspectsChart
    tabulations_aspectsDownload_Frame <- reactive({
      wAsp <- which(c('O', 'S', 'T', 'X', 'L_All') %in% colnames(indexTable()))
      wAsp <- c('O', 'S', 'T', 'X', 'L_All')[wAsp]
      aspects <- indexTable() %>%
        dplyr::select(wAsp) %>%
        colnames()
      count <- indexTable() %>%
        dplyr::select(wAsp) %>%
        colSums()
      out <- data.frame(aspect = aspects,
                        count = count)
      out <- out %>%
        dplyr::arrange(desc(count))
      out
    })
    # - Download: tabulations_aspectsChart
    output$tabulations_aspectsDownload_Frame <- downloadHandler(
      filename = function() {
        'WDCM_Data.csv'},
      content = function(file) {
        write.csv(tabulations_aspectsDownload_Frame(),
                  file,
                  quote = FALSE,
                  row.names=FALSE)
      }
    )
    
    ### --- Chart: tabulations_LAspectsChart
    output$tabulations_LAspectsChart <- renderPlot({
      
      # - Chart Frame for output$tabulations_LAspectsChart
      aspects <- isolate(indexTable()) %>%
        dplyr::select(starts_with('L.')) %>%
        colnames()
      count <- isolate(indexTable()) %>%
        dplyr::select(starts_with('L.')) %>%
        colSums()
      tabulations_LAspectsChart_Frame <- data.frame(aspect = aspects,
                                                    count = count)
      tabulations_LAspectsChart_Frame <- tabulations_LAspectsChart_Frame %>%
        dplyr::mutate(key = paste(round(count/1000000, 1), "M", sep = "")) %>%
        dplyr::arrange(desc(count))
      if (dim(tabulations_LAspectsChart_Frame)[1] > 15) {
        sumOther <- sum(tabulations_LAspectsChart_Frame$count[16:dim(tabulations_LAspectsChart_Frame)[1]])
        tabulations_LAspectsChart_Frame <- tabulations_LAspectsChart_Frame[1:15, ]
        rbind(tabulations_LAspectsChart_Frame, data.frame(aspect = 'other',
                                                          count = sumOther,
                                                          key = paste(round(sumOther/1000000, 1), "M", sep = ""))
        )
        tabulations_LAspectsChart_Frame <- dplyr::arrange(tabulations_LAspectsChart_Frame, desc(count))
      }
      tabulations_LAspectsChart_Frame$aspect <- factor(tabulations_LAspectsChart_Frame$aspect,
                                                       levels = tabulations_LAspectsChart_Frame$aspect[order(-tabulations_LAspectsChart_Frame$count)])
      
      # - Plot:
      ggplot(tabulations_LAspectsChart_Frame,
             aes(x = aspect, y = count, label = key)) +
        geom_bar(stat = "identity", width = .6, fill = "#4c8cff") +
        geom_label(size = 3, vjust = -.1) +
        xlab('Aspects') + ylab('Entity Usage') +
        ylim(0, max(tabulations_LAspectsChart_Frame$count) + .1*max(tabulations_LAspectsChart_Frame$count)) +
        theme_bw() +
        theme(axis.text.x = element_text(angle = 90, size = 12, hjust = 1)) +
        theme(axis.title.x = element_text(size = 12)) +
        theme(axis.title.y = element_text(size = 12)) +
        theme(plot.title = element_text(size = 15)) %>% 
        withProgress(message = 'Generating plot',
                     min = 0,
                     max = 1,
                     value = 1, {incProgress(amount = 0)})
    })
    # - Download Frame: tabulations_aspectsChart
    tabulations_LAspectsDownload_Frame <- reactive({
      aspects <- indexTable() %>%
        dplyr::select(starts_with('L.')) %>%
        colnames()
      count <- indexTable() %>%
        dplyr::select(starts_with('L.')) %>%
        colSums()
      out <- data.frame(aspect = aspects,
                        count = count)
      out <- out %>%
        dplyr::arrange(desc(count))
      out
    })
    # - Download: tabulations_aspectsChart
    output$tabulations_LAspectsDownload_Frame <- downloadHandler(
      filename = function() {
        'WDCM_Data.csv'},
      content = function(file) {
        write.csv(tabulations_LAspectsDownload_Frame(),
                  file,
                  quote = FALSE,
                  row.names=FALSE)
      }
    )
    
    ### ----------------------------------
    ### --- TAB PANEL: Cross-Tabulations
    ### ----------------------------------
    
    ### --- Chart: crosstabulations_ProjectCategory_Chart
    # - Chart Frame for output$crosstabulations_ProjectCategory
    crosstabulations_ProjectCategory_Chart_Frame <- isolate(indexTable()) %>% 
      dplyr::select(Project, starts_with('Cat_')) %>% 
      gather(key = Category,
             value = count,
             starts_with('Cat_')
      )
    projSum <- crosstabulations_ProjectCategory_Chart_Frame %>% 
      dplyr::group_by(Project) %>% 
      dplyr::summarise(sum = sum(count)) %>% 
      dplyr::arrange(desc(sum))
    w6Proj <- projSum$Project[1:6]
    wOther <- which(!(unique(crosstabulations_ProjectCategory_Chart_Frame$Project) %in% w6Proj))
    crosstabulations_ProjectCategory_Chart_Frame$Project[crosstabulations_ProjectCategory_Chart_Frame$Project %in% unique(crosstabulations_ProjectCategory_Chart_Frame$Project)[wOther]] <- 'other'
    crosstabulations_ProjectCategory_Chart_Frame <- crosstabulations_ProjectCategory_Chart_Frame %>% 
      dplyr::group_by(Project, Category) %>% 
      dplyr::mutate(count = sum(count))
    crosstabulations_ProjectCategory_Chart_Frame$Category <- gsub("Cat_", "", crosstabulations_ProjectCategory_Chart_Frame$Category)
    ### --- Observe: Log_crosstabulations_ProjectCategory
    observeEvent(input$Log_crosstabulations_ProjectCategory, {
      if (input$Log_crosstabulations_ProjectCategory == FALSE) {
        output$crosstabulations_ProjectCategory_Chart <- renderPlot({
          ggplot(crosstabulations_ProjectCategory_Chart_Frame, 
                 aes(x = Category, y = count, group = Project, color = Project, label = count)) + 
            geom_line(size = .5) +
            geom_point(size = 2) +
            xlab('Categories') + ylab('Entity Usage') + 
            ylim(0, max(crosstabulations_ProjectCategory_Chart_Frame$count) + .1*max(crosstabulations_ProjectCategory_Chart_Frame$count)) + 
            theme_bw() + 
            theme(axis.text.x = element_text(angle = 90, size = 12, hjust = 1)) + 
            theme(axis.title.x = element_text(size = 12)) + 
            theme(axis.title.y = element_text(size = 12)) + 
            theme(plot.title = element_text(size = 15)) %>% 
            withProgress(message = 'Generating plot', 
                         min = 0, 
                         max = 1, 
                         value = 1, {incProgress(amount = 0)})
        })
        
      } else {
        output$crosstabulations_ProjectCategory_Chart <- renderPlot({
          ggplot(crosstabulations_ProjectCategory_Chart_Frame, 
                 aes(x = Category, y = log(count+1), group = Project, color = Project, label = log(count+1))) + 
            geom_line(size = .5) +
            geom_point(size = 2) +
            xlab('Categories') + ylab('Log(Entity Usage+1)') + 
            ylim(0, max(log(crosstabulations_ProjectCategory_Chart_Frame$count+1)) + .1*max(log(crosstabulations_ProjectCategory_Chart_Frame$count+1))) + 
            theme_bw() + 
            theme(axis.text.x = element_text(angle = 90, size = 12, hjust = 1)) + 
            theme(axis.title.x = element_text(size = 12)) + 
            theme(axis.title.y = element_text(size = 12)) + 
            theme(plot.title = element_text(size = 15)) %>% 
            withProgress(message = 'Generating plot', 
                         min = 0, 
                         max = 1, 
                         value = 1, {incProgress(amount = 0)})
          })
        }
      })
    # - Download Frame: crosstabulations_ProjectCategory_Chart_Frame
    crosstabulations_ProjectCategoryDownload_Frame <- reactive({
      out <- indexTable() %>%
        dplyr::select(Project, starts_with('Cat_')) %>%
        gather(key = Category,
               value = count,
               starts_with('Cat_')
        ) %>%
        dplyr::arrange(Project, Category)
      out$Category <- gsub("Cat_", "", out$Category)
      as.data.frame(out)
    })
    # - Download: crosstabulations_ProjectCategory_Chart_Frame
    output$crosstabulations_ProjectCategoryDownload_Frame <- downloadHandler(
      filename = function() {
        'WDCM_Data.csv'},
      content = function(file) {
        write.csv(crosstabulations_ProjectCategoryDownload_Frame(),
                  file,
                  quote = FALSE,
                  row.names=FALSE)
      }
    )
    
    ### --- Chart: output$crosstabulations_ProjectAspects
    # - Chart Frame for output$crosstabulations_ProjectAspects
    crosstabulations_ProjectAspects_Chart_Frame <- isolate(indexTable()) %>% 
      dplyr::select(Project, O, S, `T`, X, L_All) %>% 
      gather(key = Aspect, 
             value = count, 
             O, S, `T`, X, L_All
             )
    projSum <- crosstabulations_ProjectAspects_Chart_Frame %>% 
      dplyr::group_by(Project) %>% 
      dplyr::summarise(sum = sum(count)) %>% 
      dplyr::arrange(desc(sum))
    w6Proj <- projSum$Project[1:6] 
    wOther <- which(!(unique(crosstabulations_ProjectAspects_Chart_Frame$Project) %in% w6Proj)) 
    crosstabulations_ProjectAspects_Chart_Frame$Project[crosstabulations_ProjectAspects_Chart_Frame$Project %in% unique(crosstabulations_ProjectAspects_Chart_Frame$Project)[wOther]] <- 'other'
    crosstabulations_ProjectAspects_Chart_Frame <- crosstabulations_ProjectAspects_Chart_Frame %>% 
      dplyr::group_by(Project, Aspect) %>% 
      dplyr::mutate(count = sum(count))
    ### --- Observe: logcrosstabulations_ProjectAspect
    observeEvent(input$logcrosstabulations_ProjectAspect, {
      
      if (input$logcrosstabulations_ProjectAspect == FALSE) {
        output$crosstabulations_ProjectAspect_Chart <- renderPlot({
          ggplot(crosstabulations_ProjectAspects_Chart_Frame, 
                 aes(x = Aspect, y = count, group = Project, color = Project, label = count)) + 
            geom_line(size = .5) +
            geom_point(size = 2) +
            xlab('Aspects') + ylab('Entity Usage') + 
            ylim(0, max(crosstabulations_ProjectAspects_Chart_Frame$count) + .1*max(crosstabulations_ProjectAspects_Chart_Frame$count)) + 
            theme_bw() + 
            theme(axis.text.x = element_text(angle = 0, size = 12, hjust = 1)) + 
            theme(axis.title.x = element_text(size = 12)) + 
            theme(axis.title.y = element_text(size = 12)) + 
            theme(plot.title = element_text(size = 15)) %>% 
            withProgress(message = 'Generating plot', 
                         min = 0, 
                         max = 1, 
                         value = 1, {incProgress(amount = 0)})
        })
      } else {
        output$crosstabulations_ProjectAspect_Chart <- renderPlot({
          ggplot(crosstabulations_ProjectAspects_Chart_Frame, 
                 aes(x = Aspect, y = log(count+1), group = Project, color = Project, label = log(count+1))) + 
            geom_line(size = .5) +
            geom_point(size = 2) +
            xlab('Aspects') + ylab('Entity Usage') + 
            ylim(0, max(log(crosstabulations_ProjectAspects_Chart_Frame$count+1)) + .1*max(log(crosstabulations_ProjectAspects_Chart_Frame$count+1))) + 
            theme_bw() + 
            theme(axis.text.x = element_text(angle = 90, size = 12, hjust = 1)) + 
            theme(axis.title.x = element_text(size = 12)) + 
            theme(axis.title.y = element_text(size = 12)) + 
            theme(plot.title = element_text(size = 15)) %>% 
            withProgress(message = 'Generating plot', 
                         min = 0, 
                         max = 1, 
                         value = 1, {incProgress(amount = 0)})
        })
      }
    })
    # - Download Frame
    crosstabulations_ProjectAspectDownload_Frame <- reactive({
      out <- indexTable() %>%
        dplyr::select(Project, O, S, `T`, X, L_All) %>%
        gather(key = Aspect,
               value = count,
               O, S, `T`, X, L_All
        ) %>%
        dplyr::arrange(Project, Aspect)
      as.data.frame(out)
    })
    # - Download
    output$crosstabulations_ProjectAspectDownload_Frame <- downloadHandler(
      filename = function() {
        'WDCM_Data.csv'},
      content = function(file) {
        write.csv(crosstabulations_ProjectAspectDownload_Frame(),
                  file,
                  quote =  FALSE,
                  row.names=FALSE)
      }
    )

    ### --- Chart: crosstabulations_ProjectLAspect_Chart
    # - Chart Frame for output$crosstabulations_ProjectLAspects
    crosstabulations_ProjectLAspects_Chart_Frame <- isolate(indexTable()) %>% 
      dplyr::select(Project, starts_with("L.")) %>% 
      gather(key = Aspect, 
             value = count, 
             starts_with("L.")
      )
    projSum <- crosstabulations_ProjectLAspects_Chart_Frame %>% 
      dplyr::group_by(Project) %>% 
      dplyr::summarise(sum = sum(count)) %>% 
      dplyr::arrange(desc(sum))
    w6Proj <- projSum$Project[1:6]
    wOther <- which(!(unique(crosstabulations_ProjectLAspects_Chart_Frame$Project) %in% w6Proj))
    crosstabulations_ProjectLAspects_Chart_Frame$Project[crosstabulations_ProjectLAspects_Chart_Frame$Project %in% unique(crosstabulations_ProjectLAspects_Chart_Frame$Project)[wOther]] <- 'other'
    crosstabulations_ProjectLAspects_Chart_Frame <- crosstabulations_ProjectLAspects_Chart_Frame %>% 
      dplyr::group_by(Project, Aspect) %>% 
      dplyr::mutate(count = sum(count))
    # - observe: Log_crosstabulations_ProjectLAspect
    observeEvent(input$Log_crosstabulations_ProjectLAspect, {
      
      if(input$Log_crosstabulations_ProjectLAspect == FALSE) {
        
        output$crosstabulations_ProjectLAspects_Chart <- renderPlot({
          ggplot(crosstabulations_ProjectLAspects_Chart_Frame, 
                 aes(x = Aspect, y = count, group = Project, color = Project, label = count)) + 
            geom_line(size = .5) +
            geom_point(size = 2) +
            xlab('Aspects') + ylab('Entity Usage') + 
            ylim(0, max(crosstabulations_ProjectLAspects_Chart_Frame$count) + .1*max(crosstabulations_ProjectLAspects_Chart_Frame$count)) + 
            theme_bw() + 
            theme(axis.text.x = element_text(angle = 90, size = 12, hjust = 1)) + 
            theme(axis.title.x = element_text(size = 12)) + 
            theme(axis.title.y = element_text(size = 12)) + 
            theme(plot.title = element_text(size = 15)) %>% 
            withProgress(message = 'Generating plot', 
                         min = 0, 
                         max = 1, 
                         value = 1, {incProgress(amount = 0)})
        })
      } else {
        output$crosstabulations_ProjectLAspects_Chart <- renderPlot({
          ggplot(crosstabulations_ProjectLAspects_Chart_Frame, 
                 aes(x = Aspect, y = log(count+1), group = Project, color = Project, label = log(count+1))) + 
            geom_line(size = .5) +
            geom_point(size = 2) +
            xlab('Aspects') + ylab('Entity Usage') + 
            ylim(0, max(log(crosstabulations_ProjectLAspects_Chart_Frame$count+1)) + .1*max(log(crosstabulations_ProjectLAspects_Chart_Frame$count+1))) + 
            theme_bw() + 
            theme(axis.text.x = element_text(angle = 90, size = 12, hjust = 1)) + 
            theme(axis.title.x = element_text(size = 12)) + 
            theme(axis.title.y = element_text(size = 12)) + 
            theme(plot.title = element_text(size = 15)) %>% 
            withProgress(message = 'Generating plot', 
                         min = 0, 
                         max = 1, 
                         value = 1, {incProgress(amount = 0)})
        })
      }
    })
    # - Download Frame
    crosstabulations_ProjectLAspectDownload_Frame <- reactive({
      out <- indexTable() %>%
        dplyr::select(Project, starts_with("L.")) %>%
        gather(key = Aspect,
               value = count,
               starts_with("L.")
        ) %>%
        dplyr::arrange(Project, Aspect)
      as.data.frame(out)
    })
    # - Download
    output$crosstabulations_ProjectLAspectDownload_Frame <- downloadHandler(
      filename = function() {
        'WDCM_Data.csv'},
      content = function(file) {
        write.csv(crosstabulations_ProjectLAspectDownload_Frame(),
                  file,
                  quote = FALSE,
                  row.names=FALSE)
      }
    )

    ### --- Chart: output$crosstabulations_CategoryAspects
    ### --- Chart Frame for output$crosstabulations_CategoryAspects
    crosstabulations_CategoryAspects_Chart_Frame <- isolate(indexTable3())
    wL <- which(grepl("^L", crosstabulations_CategoryAspects_Chart_Frame$Aspect))
    crosstabulations_CategoryAspects_Chart_Frame$Aspect[wL] <- "L_All"
    crosstabulations_CategoryAspects_Chart_Frame <- crosstabulations_CategoryAspects_Chart_Frame %>%
      dplyr::group_by(Category, Aspect) %>%
      dplyr::summarise(count = sum(count)) %>%
      as.data.frame()
    crosstabulations_CategoryAspects_Chart_Frame$Aspect <- factor(crosstabulations_CategoryAspects_Chart_Frame$Aspect)
    crosstabulations_CategoryAspects_Chart_Frame$Category <- factor(crosstabulations_CategoryAspects_Chart_Frame$Category)
    ### --- observe: logcrosstabulations_CategoryAspects
    observeEvent(input$logcrosstabulations_CategoryAspects, {
      
      if (input$logcrosstabulations_CategoryAspects == FALSE) {
        
        output$crosstabulations_CategoryAspects_Chart <- renderPlot({
          ggplot(crosstabulations_CategoryAspects_Chart_Frame, 
                 aes(x = Aspect, y = count, group = Category, color = Category, label = count)) + 
            geom_line(size = .5) +
            geom_point(size = 2) +
            xlab('Aspects') + ylab('Entity Usage') + 
            ylim(0, max(crosstabulations_CategoryAspects_Chart_Frame$count) + .1*max(crosstabulations_CategoryAspects_Chart_Frame$count)) + 
            theme_bw() + 
            theme(axis.text.x = element_text(angle = 90, size = 12, hjust = 1)) + 
            theme(axis.title.x = element_text(size = 12)) + 
            theme(axis.title.y = element_text(size = 12)) + 
            theme(plot.title = element_text(size = 15)) %>% 
            withProgress(message = 'Generating plot', 
                         min = 0, 
                         max = 1, 
                         value = 1, {incProgress(amount = 0)})
        })
      } else {
        output$crosstabulations_CategoryAspects_Chart <- renderPlot({
          ggplot(crosstabulations_CategoryAspects_Chart_Frame, 
                 aes(x = Aspect, y = log(count+1), group = Category, color = Category, label = log(count+1))) + 
            geom_line(size = .5) +
            geom_point(size = 2) +
            xlab('Aspects') + ylab('Entity Usage') + 
            ylim(0, max(log(crosstabulations_CategoryAspects_Chart_Frame$count+1)) + .1*max(log(crosstabulations_CategoryAspects_Chart_Frame$count+1))) + 
            theme_bw() + 
            theme(axis.text.x = element_text(angle = 90, size = 12, hjust = 1)) + 
            theme(axis.title.x = element_text(size = 12)) + 
            theme(axis.title.y = element_text(size = 12)) + 
            theme(plot.title = element_text(size = 15)) %>% 
            withProgress(message = 'Generating plot', 
                         min = 0, 
                         max = 1, 
                         value = 1, {incProgress(amount = 0)})
        })
      }
    })
    # - Download Frame
    crosstabulations_CategorAspects_Download_Frame <- reactive({
      out <- indexTable3()
      wL <- which(grepl("^L", out$Aspect))
      out$Aspect[wL] <- "L_All"
      out <- out %>%
        dplyr::group_by(Category, Aspect) %>%
        dplyr::summarise(count = sum(count)) %>%
        dplyr::arrange(Category, Aspect) %>%
        as.data.frame()
      out
    })
    # - Download
    output$crosstabulations_CategoryAspects_Download_Frame <- downloadHandler(
      filename = function() {
        'WDCM_Data.csv'},
      content = function(file) {
        write.csv(crosstabulations_CategorAspects_Download_Frame(),
                  file,
                  quote = FALSE,
                  row.names=FALSE)
      }
    )
    

    ### --- Chart: output$crosstabulations_CategoryLAspects
    ### --- Chart Frame for output$crosstabulations_CategoryLAspects
    crosstabulations_CategoryLAspects_Chart_Frame <- isolate(indexTable2()) %>%
      dplyr::group_by(Category, Aspect) %>%
      dplyr::summarise(count = sum(count)) %>%
      as.data.frame()
    crosstabulations_CategoryLAspects_Chart_Frame$Aspect <- factor(crosstabulations_CategoryLAspects_Chart_Frame$Aspect)
    crosstabulations_CategoryLAspects_Chart_Frame$Category <- factor(crosstabulations_CategoryLAspects_Chart_Frame$Category)
    ### --- observe: logcrosstabulations_CategoryLAspects
    observeEvent(input$logcrosstabulations_CategoryLAspects, {
      
      if (input$logcrosstabulations_CategoryLAspects == FALSE) {
        
        output$crosstabulations_CategoryLAspects_Chart <- renderPlot({
          ggplot(crosstabulations_CategoryLAspects_Chart_Frame, 
                 aes(x = Aspect, y = count, group = Category, color = Category, label = count)) +
            geom_line(size = .5) +
            geom_point(size = 2) +
            xlab('Aspects') + ylab('Entity Usage') + 
            ylim(0, max(crosstabulations_CategoryLAspects_Chart_Frame$count) + .1*max(crosstabulations_CategoryLAspects_Chart_Frame$count)) + 
            theme_bw() + 
            theme(axis.text.x = element_text(angle = 90, size = 12, hjust = 1)) + 
            theme(axis.title.x = element_text(size = 12)) + 
            theme(axis.title.y = element_text(size = 12)) + 
            theme(plot.title = element_text(size = 15)) %>% 
            withProgress(message = 'Generating plot', 
                         min = 0, 
                         max = 1, 
                         value = 1, {incProgress(amount = 0)})
        })
      } else {
        output$crosstabulations_CategoryLAspects_Chart <- renderPlot({
          ggplot(crosstabulations_CategoryLAspects_Chart_Frame, 
                 aes(x = Aspect, y = log(count+1), group = Category, color = Category, label = log(count+1))) +
            geom_line(size = .5) +
            geom_point(size = 2) +
            xlab('Aspects') + ylab('Entity Usage') + 
            ylim(0, max(log(crosstabulations_CategoryLAspects_Chart_Frame$count+1)) + .1*max(log(crosstabulations_CategoryLAspects_Chart_Frame$count+1))) + 
            theme_bw() + 
            theme(axis.text.x = element_text(angle = 90, size = 12, hjust = 1)) + 
            theme(axis.title.x = element_text(size = 12)) + 
            theme(axis.title.y = element_text(size = 12)) + 
            theme(plot.title = element_text(size = 15)) %>% 
            withProgress(message = 'Generating plot', 
                         min = 0, 
                         max = 1, 
                         value = 1, {incProgress(amount = 0)})
        })
      }
    })
    # - Download Frame
    crosstabulations_CategoryLAspects_Download_Frame <- reactive({
      out <- indexTable2() %>%
        dplyr::select(Category, Aspect, count) %>%
        dplyr::group_by(Category, Aspect) %>%
        dplyr::mutate(count = sum(count)) %>%
        dplyr::arrange(Category, Aspect) %>%
        as.data.frame()
      out
    })
    # - Download
    output$crosstabulations_CategoryLAspects_Download_Frame <- downloadHandler(
      filename = function() {
        'WDCM_Data.csv'},
      content = function(file) {
        write.csv(crosstabulations_CategoryLAspects_Download_Frame(),
                  file,
                  quote = FALSE,
                  row.names=FALSE)
      }
    )
    
    ### ----------------------------------
    ### --- Project Semantics Chart
    ### ----------------------------------
    
    # - Chart: projectSemantics_Chart
    # - Chart Frame for: projectSemantics_Chart
    # - find what categories from projectTopicList list are selected:
    if (is.null(input$categories)) {
      selectedCategories <- allCategories
    } else {
      selectedCategories <- unique(input$categories) 
    }
    if (is.null(input$projects)) {
      selectedProjects <- allProjects
    } else {
      selectedProjects <- unique(input$projects) 
    }
    categoriesPresent <- unname(sapply(names(projectTopicList), function(x) {
      out <- strsplit(x, split = "_", fixed = T)[[1]][3]
      out <- strsplit(out, split = ".", fixed = T)[[1]][1]
    }))
    categoriesPresent <- sapply(categoriesPresent, function(x) {
      which(selectedCategories %in% x)
    })
    categoriesPresentIndex <- unname(which(categoriesPresent > 0))
    catNames <- names(categoriesPresent)[categoriesPresentIndex]
    # - wrangle selected projectTopicList components:
    catLength <- sapply(projectTopicList[categoriesPresentIndex], function(x){
      (dim(x)[2]-2)*dim(x)[1]
    })
    projectSemantics_Chart_Frame <- lapply(projectTopicList[categoriesPresentIndex], function(x) {
      x <- x %>%
        dplyr::select(project, starts_with('topic')) %>%
        tidyr::gather(key = Topic,
                      value = Probability,
                      starts_with('topic'))
      })
    projectSemantics_Chart_Frame <- rbindlist(projectSemantics_Chart_Frame)
    projectSemantics_Chart_Frame$Category <- rep(catNames, catLength)
    colnames(projectSemantics_Chart_Frame)[1] <- 'Project'
    projectSemantics_Chart_Frame$Percent <- round(projectSemantics_Chart_Frame$Probability*100, 1) %>% 
      withProgress(message = 'Generating data', 
                   min = 0, 
                   max = 1, 
                   value = 1, {incProgress(amount = 0)})
    # - Plot projectSemantics_Chart:
    output$projectSemantics_Chart <- renderPlot({
      plotFrame <- 
        projectSemantics_Chart_Frame[which(projectSemantics_Chart_Frame$Project %in% selectedProjects[1:20])]
      ggplot(plotFrame, 
             aes(x = Project, y = Percent, group = Topic, color = Topic)) +
        geom_path(size = .5) +
        geom_point(size = 2) +
        facet_wrap(~ Category, ncol = 3, scales = "free_y") +
        xlab('Projects') + ylab('Percent') + 
        ylim(0, 100) +
        theme_bw() + 
        theme(strip.background = element_blank()) +
        theme(axis.text.x = element_text(angle = 90, size = 12, hjust = 1)) + 
        theme(axis.title.x = element_text(size = 12)) + 
        theme(axis.title.y = element_text(size = 12)) + 
        theme(plot.title = element_text(size = 15)) %>% 
        withProgress(message = 'Generating plot', 
                     min = 0, 
                     max = 1, 
                     value = 1, {incProgress(amount = 0)})
      
    }, width = "auto", height = 800)
    # - Download frame
    projectSemantics_Chart_Download_Frame <- reactive({projectSemantics_Chart_Frame})
    # - Download
    output$projectSemantics_Chart_Download_Frame <- downloadHandler(
      filename = function() {
        'WDCM_Data.csv'},
      content = function(file) {
        write.csv(projectSemantics_Chart_Download_Frame(),
                  file,
                  quote = FALSE,
                  row.names=FALSE)
      }
    )
    
    
  }, ignoreNULL = FALSE)

  ### ----------------------------------
  ### --- END TAB: Projects
  ### ----------------------------------
  
  

})
### --- END shinyServer




