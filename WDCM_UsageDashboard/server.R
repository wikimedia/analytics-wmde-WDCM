### ---------------------------------------------------------------------------
### --- WDCM Usage Dashboard, v. Beta 0.1
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
setwd('/srv/shiny-server/WDCM_Dashboard/aux')

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

### --- fetch wdcm2_project_item100
q <- "SELECT * FROM wdcm2_project_item100;"
res <- dbSendQuery(con, q)
wdcmProjectItem100 <- fetch(res, -1)
dbClearResult(res) 
colnames(wdcmProjectItem100) <- c('Project', 'EntityID', 'Usage', 'Project Type', 'Label')

### --- fetch wdcm2_project_category_item100
q <- "SELECT * FROM wdcm2_project_category_item100;"
res <- dbSendQuery(con, q)
wdcmProjectCategoryItem100 <- fetch(res, -1)
dbClearResult(res) 
colnames(wdcmProjectCategoryItem100) <- c('Project', 'Category', 'EntityID', 'Usage', 'Project Type', 'Label')

### --- fetch wdcm2_category
q <- "SELECT * FROM wdcm2_category;"
res <- dbSendQuery(con, q)
wdcmCategory <- fetch(res, -1)
dbClearResult(res) 
colnames(wdcmCategory) <- c('Category', 'Usage')

### --- fetch wdcm2_category_item100
q <- "SELECT * FROM wdcm2_category_item100;"
res <- dbSendQuery(con, q)
wdcmCategoryItem100 <- fetch(res, -1)
dbClearResult(res) 
colnames(wdcmCategoryItem100) <- c('EntityID', 'Usage', 'Category', 'Label')

### --- Disconnect
dbDisconnect(con)

### --- Compute per `Project Type` tables
# - wdcmProjectType
wdcmProjectType <- wdcmProject %>% 
  group_by(`Project Type`) %>% 
  summarise(Usage = sum(Usage)) %>% 
  arrange(desc(Usage))
# - wdcmProjectTypeCategory
wdcmProjectTypeCategory <- wdcmProjectCategory %>% 
  group_by(`Project Type`, Category) %>% 
  summarise(Usage = sum(Usage)) %>% 
  arrange(desc(Usage))
# - wdcmProjectTypeItem100
wdcmProjectTypeItem100 <- wdcmProjectItem100 %>% 
  select(`Project Type`, EntityID, Label, Usage) %>% 
  group_by(`Project Type`, EntityID, Label) %>% 
  summarise(Usage = sum(Usage)) %>% 
  arrange(`Project Type`, desc(Usage))

### --- Compute project similarity structure
projectSimilarity <- wdcmProjectCategory %>% 
  dplyr::select(Project, Category, Usage) %>% 
  tidyr::spread(key = Category,
         value = Usage,
         fill = 0)
projectNames <- projectSimilarity$Project
projectSimilarity$Project <- NULL
# - normalize:
projectSimilarity <- t(apply(projectSimilarity, 1, function(x) {x/sum(x)}))
# projectSimilarity[projectSimilarity > 0] <- 1
projectSimilarity <- as.matrix(parDist(as.matrix(projectSimilarity), method = "kullback"))
rownames(projectSimilarity) <- projectNames
colnames(projectSimilarity) <- projectNames

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
totalProjectTypes <- length(wdcmProjectType$`Project Type`)

### --- prepare search constants for Tabs/Crosstabs
search_projectTypes <- paste("_", projectTypes, sep = "")
unzip_projectTypes <- lapply(projectTypes, function(x) {
  wdcmProject$Project[which(wdcmProject$`Project Type` %in% x)]
})
names(unzip_projectTypes) <- search_projectTypes

### --- shinyServer
shinyServer(function(input, output, session) {
  
  ### ----------------------------------
  ### --- BASIC FACTS
  ### ----------------------------------
  
  ### --- valueBox: totalUsage
  # output$totalUsageBox
  output$totalUsageBox <- renderValueBox({
    valueBox(
      value = as.character(totalUsage),
      subtitle = "Total Wikidata Item Usage",
      icon = icon("bars", lib = "font-awesome"),
      color = "blue"
    )
  }) # END output$totalUsageBox
  
  ### --- valueBox: totalProjects
  # output$totalProjectsBox
  output$totalProjectsBox <- renderValueBox({
    valueBox(
      value = as.character(totalProjects),
      subtitle = "Projects Tracked",
      icon = icon("bars", lib = "font-awesome"),
      color = "blue"
    )
  }) # END output$totalProjectsBox
  
  ### --- valueBox: totalCategories
  # output$totalCategoriesBox
  output$totalCategoriesBox <- renderValueBox({
    valueBox(
      value = as.character(totalCategories),
      subtitle = "Semantic Categories",
      icon = icon("bars", lib = "font-awesome"),
      color = "blue"
    )
  }) # END output$totalCategoriesBox
  
  ### --- valueBox: totalProjectTypes
  # output$totalProjectTypesBox
  output$totalProjectTypesBox <- renderValueBox({
    valueBox(
      value = as.character(totalProjectTypes),
      subtitle = "Project Types",
      icon = icon("bars", lib = "font-awesome"),
      color = "blue"
    )
  }) # END output$totalProjectTypesBox
  
  ### ----------------------------------
  ### --- CATEGORIES OVERVIEW
  ### ----------------------------------
  
  ### --- SELECT: update select 'categories'
  updateSelectizeInput(session,
                       'categories',
                       choices = categories,
                       selected = categories[round(runif(1, 1, length(categories)))],
                       server = TRUE)
  
  ### --- htmlOutput: categoryProjects_overview_Title
  output$categoryProjects_overview_Title <- renderText({
    paste("<b>", input$categories, " top 30 projects:</b>")
  })
  
  ### --- lineplot: categoryProjects_overview
  output$categoryProjects_overview <- renderPlot({
    if (!(input$categories == "")) {
      plotFrame <- wdcmProjectCategory %>% 
        select(Project, Category, Usage) %>% 
        filter(Category %in% input$categories) %>% 
        arrange(desc(Usage))
      otherSum <- sum(plotFrame$Usage[31:dim(plotFrame)[1]]) 
      other <- data.frame(Project = 'Other', 
                          Category = input$categories,
                          Usage = otherSum,
                          stringsAsFactors = F)
      plotFrame <- rbind(plotFrame[1:30, ], other)
      plotFrame$Percent <- paste(round(plotFrame$Usage/sum(plotFrame$Usage)*100, 2),
                                 "%", sep = "")
      plotFrame$Project <- factor(plotFrame$Project, 
                                levels = plotFrame$Project[order(plotFrame$Usage)])
      ggplot(plotFrame, aes(x = Usage, y = Project)) +
        geom_line(size = .35, color = "firebrick", group = 1) +
        geom_point(size = 2, color = "firebrick") +
        geom_point(size = 1.5, color = "white") + 
        geom_text_repel(aes(label = plotFrame$Percent), 
                        size = 3) +
        xlab("Item Usage") + ylab("Project") +
        scale_x_continuous(labels = comma) + 
        theme_minimal() +
        theme(axis.text.x = element_text(angle = 0, size = 9, hjust = 1)) +
        theme(axis.title.x = element_text(size = 12)) +
        theme(axis.title.y = element_text(size = 12)) %>% 
        withProgress(message = 'Generating plot',
                     min = 0,
                     max = 1,
                     value = 1, {incProgress(amount = 0)})
    } else {
      return(NULL)
    }
  })
  
  ### --- htmlOutput: categoryItems_overview_Title
  output$categoryItems_overview_Title <- renderText({
    paste("<b>", input$categories, " top 30 Wikidata items:</b>")
  })
  
  ### --- lineplot: categoryItems_overview
  output$categoryItems_overview <- renderPlot({
    if (!(input$categories == "")) {
      plotFrame <- wdcmCategoryItem100 %>% 
        filter(Category %in% input$categories) %>% 
        arrange(desc(Usage))
      plotFrame <- plotFrame[1:30, ] 
      plotFrame$Label <- factor(plotFrame$Label, 
                                  levels = plotFrame$Label[order(plotFrame$Usage)])
      ggplot(plotFrame, aes(x = Usage, y = Label)) +
        geom_line(size = .35, color = "firebrick", group = 1) +
        geom_point(size = 2, color = "firebrick") +
        geom_point(size = 1.5, color = "white") + 
        geom_text_repel(aes(label = plotFrame$EntityID), 
                        size = 3) +
        xlab("Item Usage") + ylab("Item") +
        scale_x_continuous(labels = comma) + 
        theme_minimal() +
        theme(axis.text.x = element_text(angle = 0, size = 9, hjust = 1)) +
        theme(axis.title.x = element_text(size = 12)) +
        theme(axis.title.y = element_text(size = 12)) %>% 
        withProgress(message = 'Generating plot',
                     min = 0,
                     max = 1,
                     value = 1, {incProgress(amount = 0)})
    } else {
      return(NULL)
    }
  })
  
  ### --- lineplot: basicFacts_CategoryLine
  output$basicFacts_CategoryLine <- renderPlot({
    plotFrame <- wdcmCategory
    plotFrame$Percent <- paste(round(plotFrame$Usage/sum(plotFrame$Usage)*100, 2),
                               "%", sep = "")
    plotFrame$Category <- factor(plotFrame$Category, 
                                 levels = plotFrame$Category[order(plotFrame$Usage)])
    ggplot(plotFrame, aes(x = Usage, y = Category)) +
      geom_line(size = .35, color = "firebrick", group = 1) +
      geom_point(size = 2, color = "firebrick") +
      geom_point(size = 1.5, color = "white") + 
      geom_text_repel(aes(label = plotFrame$Percent), 
                      size = 3) +
      xlab("Item Usage") + ylab("Category") +
      scale_x_continuous(labels = comma) + 
      theme_minimal() +
      theme(axis.text.x = element_text(angle = 0, size = 9, hjust = 1)) +
      theme(axis.title.x = element_text(size = 12)) +
      theme(axis.title.y = element_text(size = 12)) %>% 
      withProgress(message = 'Generating plot',
                   min = 0,
                   max = 1,
                   value = 1, {incProgress(amount = 0)})
  })
  
  ### --- barplot: basicFacts_ProjectTypeCategory
  output$basicFacts_ProjectTypeCategory <- renderPlot({
    ggplot(wdcmProjectTypeCategory, aes(y = log(Usage), x = Category, color = Category, fill = Category)) +
      geom_bar(stat = "identity", width = .15) + 
      facet_wrap(~wdcmProjectTypeCategory$`Project Type`, ncol = 3) +
      xlab("Category") + ylab("log(Item Usage)") +
      theme_minimal() +
      theme(axis.text.x = element_text(angle = 90, size = 9, hjust = 1)) +
      theme(axis.title.x = element_text(size = 12)) +
      theme(axis.title.y = element_text(size = 12)) + 
      theme(legend.position = "top") +
      theme(strip.background = element_blank()) + 
      theme(strip.text = element_text(face = "bold")) %>% 
      withProgress(message = 'Generating plot',
                   min = 0,
                   max = 1,
                   value = 1, {incProgress(amount = 0)})
  })
  
  ### ----------------------------------
  ### --- PROJECT OVERVIEW
  ### ----------------------------------
  
  ### --- SELECT: update select 'projects'
  updateSelectizeInput(session,
                       'projects',
                       choices = projects,
                       selected = projects[round(runif(1, 1, length(projects)))],
                       server = TRUE)
  
  ### --- barplot: projectOverview_Category
  output$projectOverview_Category <- renderPlot({
    plotFrame <- wdcmProjectCategory %>% 
      filter(Project %in% input$projects)
    ggplot(plotFrame, aes(y = Usage, x = Category, color = Category, fill = Category)) +
      geom_bar(stat = "identity", width = .15) + 
      xlab("Category") + ylab("Item Usage") +
      scale_y_continuous(labels = comma) + 
      theme_minimal() +
      theme(axis.text.x = element_text(angle = 90, size = 9, hjust = 1)) +
      theme(axis.title.x = element_text(size = 12)) +
      theme(axis.title.y = element_text(size = 12)) + 
      theme(legend.position = "top") + 
      theme(legend.title = element_blank()) +
      withProgress(message = 'Generating plot',
                   min = 0,
                   max = 1,
                   value = 1, {incProgress(amount = 0)})
  })
  
  ### --- htmlOutput: projectOverview_Report
  output$projectOverview_Report <- renderText({
    # - project:
    project <- input$projects
    # - total project rank:
    totalRank <- which(wdcmProject$Project %in% input$projects)
    # - total projects:
    totalProjects <- length(projects)
    # - usage volume:
    volume <- wdcmProject$Usage[totalRank]
    # - percentage of total Wikidata usage:
    percVolume <- paste(round(volume/sum(wdcmProject$Usage)*100, 2), "%", sep = "")
    # - rank in its `Project Type`
    projectType <- wdcmProject$`Project Type`[totalRank]
    rankType <- wdcmProject %>% 
      filter(`Project Type` %in% projectType) %>% 
      arrange(desc(Usage))
    rankProjectType <- which(rankType$Project %in% project)
    # - total projects of this type
    totalProjectType <- dim(rankType)[1]
    paste("<p align = \"right\"><br><br><br><br>Wikidata usage on <b>", project, "</b>:<br><br>", "<font size = 2><b>", project, "</b> ", " has a total Wikidata usage volume of <b>", 
          volume, "</b> items (<b>", percVolume, "</b> of total Wikidata usage across the 
          client projects).<br>In terms of Wikidata usage, it is ranked <b>", totalRank, "/", totalProjects, "</b> among all client projects, and <b>", 
          rankProjectType, "/", totalProjectType, ".</b> in 
          its Project Type (<b><i>", projectType, "</i></b>).</font></p>", sep = "") %>% 
      withProgress(message = 'Generating report',
                   min = 0,
                   max = 1,
                   value = 1, {incProgress(amount = 0)})
  })
  
  ### --- htmlOutput: projectOverview_relativeRank_Title
  output$projectOverview_relativeRank_Title <- renderText({
    paste("<b>", input$projects, " Wikidata usage rank:</b>")
  })
  
  ### --- barplot: projectOverview_relativeRank
  output$projectOverview_relativeRank <- renderPlot({
    if (!(input$projects == "")) {
      ix <- which(wdcmProject$Project %in% input$projects)
      ixRange <- seq(ix-10, ix+10, by = 1)
      ixRange <- ixRange[which(ixRange > 0 & ixRange <= length(wdcmProject$Project))]
      plotFrame <- wdcmProject[ixRange, ]
      plotFrame$Rank <- ixRange
      plotFrame$Color <- rep('cadetblue3', dim(plotFrame)[1])
      plotFrame$Fill <- rep('white', dim(plotFrame)[1])
      plotFrame$Fill[which(plotFrame$Project %in% input$projects)] <- 'cadetblue3'
      plotFrame$Project <- factor(plotFrame$Project, 
                                  levels = plotFrame$Project[order(-plotFrame$Usage)])
      ggplot(plotFrame, aes(y = Usage, x = Project, color = Color, fill = Fill, label = Rank)) +
        geom_bar(stat = "identity", width = .1, color = plotFrame$Color, fill = plotFrame$Fill) + 
        xlab("Project") + ylab("Item Usage") +
        scale_y_continuous(labels = comma) + 
        geom_label(fill = "cadetblue3",
                   colour = "white", 
                   fontface = "bold", 
                   position = position_dodge(width = 1),
                   size = 4) + 
        theme_minimal() +
        theme(legend.position = "none") +
        theme(axis.text.x = element_text(angle = 90, size = 9, hjust = 1)) +
        theme(axis.title.x = element_text(size = 12)) +
        theme(axis.title.y = element_text(size = 12)) %>% 
        withProgress(message = 'Generating plot',
                     min = 0,
                     max = 1,
                     value = 1, {incProgress(amount = 0)})
    } else {
      return(NULL)
    }
  })
  
  ### --- htmlOutput: projectOverview_semantics_Title
  output$projectOverview_semantics_Title <- renderText({
    paste("<b>", input$projects, " Semantic Neighbourhood:</b>")
  })
  
  ### --- visNetwork: projectOverview_semantics
  # - output$projectOverview_semantics
  output$projectOverview_semantics <- renderVisNetwork({
    
    if (!(input$projects == "")) {
      # - select project
      ix <- which(rownames(projectSimilarity) %in% input$projects)
      # - select semantic neighbourhood of 15 projects
      neighbourhood <- c(names(sort(projectSimilarity[ix, -ix], decreasing = F)[1:20]),
                         rownames(projectSimilarity)[ix])
      projectsMat <- projectSimilarity[neighbourhood, neighbourhood]
      # - find most proximal neighbours
      indexMinDist <- sapply(rownames(projectsMat), function(x) {
        w <- which(rownames(projectsMat) %in% x)
        y <- sort(projectsMat[w, -w], decreasing = T)
        names(y)[length(y)]
      })
      id <- 1:length(colnames(projectsMat))
      label <- colnames(projectsMat)
      ncolor <- rep("grey", length(colnames(projectsMat)))
      w <- which(colnames(projectsMat) %in% input$projects)
      ncolor[w] <- "cadetblue"
      nodes <- data.frame(id = id,
                          label = label,
                          color = ncolor,
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
    } else {
      return(NULL)
      }
  })
  
  ### --- htmlOutput: projectOverview_topItems_Title
  output$projectOverview_topItems_Title <- renderText({
    paste("<b>", input$projects, " top 30 Wikidata items:</b>")
  })
  
  ### --- lineplot: projectOverview_topItems
  output$projectOverview_topItems <- renderPlot({
    if (!(input$projects == "")) {
      plotFrame <- wdcmProjectItem100 %>% 
        filter(Project %in% input$projects) %>% 
        arrange(desc(Usage))
      w <- which(!duplicated(plotFrame$Label))
      plotFrame <- plotFrame[w, ]
      plotFrame <- plotFrame[1:30, ]
      plotFrame$Label <- factor(plotFrame$Label, 
                                  levels = plotFrame$Label[order(plotFrame$Usage)])
      ggplot(plotFrame, aes(x = Usage, y = Label)) +
        geom_line(size = .35, color = "darkblue", group = 1) +
        geom_point(size = 2, color = "darkblue") +
        geom_point(size = 1.5, color = "white") + 
        geom_text_repel(aes(label = plotFrame$EntityID), 
                        size = 3) +
        xlab("Item Usage") + ylab("Item") +
        scale_x_continuous(labels = comma) + 
        theme_minimal() +
        theme(axis.text.x = element_text(angle = 0, size = 9, hjust = 1)) +
        theme(axis.title.x = element_text(size = 12)) +
        theme(axis.title.y = element_text(size = 12)) %>% 
        withProgress(message = 'Generating plot',
                     min = 0,
                     max = 1,
                     value = 1, {incProgress(amount = 0)})
    } else {
      return(NULL)
    }
  })
  
  ### ----------------------------------
  ### --- TABS AND CROSSTABS
  ### ----------------------------------
  
  ### --- SELECT: update select 'selectProject'
  updateSelectizeInput(session,
                       'selectProject',
                       choices = c(projects, paste("_", projectTypes, sep="")),
                       selected = c("_Wikipedia", "_Wikinews", "_Wiktionary"),
                       server = TRUE)
  
  ### --- SELECT: update select 'selectCategories'
  updateSelectizeInput(session,
                       'selectCategories',
                       choices = categories,
                       selected = categories[round(runif(6, 1, length(categories)))],
                       server = TRUE)
  
  tabsDataset <- reactive({
    ### --- selected projects:
    selectedProjects <- character()
    wUnzip <- which(names(unzip_projectTypes) %in% input$selectProject)
    if (length(wUnzip > 0)) {
      selectedProjects <- unname(do.call(c, unzip_projectTypes[wUnzip]))
    }
    wSel <- which(projects %in% input$selectProject)
    if (length(wSel > 0)) {
      selectedProjects <- c(selectedProjects, projects[wSel])
    }
    selectedProjects <- unique(selectedProjects)
    output$testSelectedProjects <- renderText({
      paste(selectedProjects, collapse = ", ", sep = "")
    })
    ### --- selected categories:
    selectedCategories <- input$selectCategories
    ### --- output
    out <- wdcmProjectCategory %>% 
      filter(Project %in% selectedProjects & Category %in% selectedCategories)
    out
  })

  ### --- OBSERVE: input$applySelection
  observeEvent(input$applySelection, {

      #### ---  Chart: tabulations_projectsChart
      output$tabulations_projectsChart <- renderPlot({
        # - Chart Frame for output$tabulations_projectsChart
        plotFrame <- isolate(tabsDataset()) %>%
          group_by(Project) %>% 
          summarise(Usage = sum(Usage)) %>%
          arrange(desc(Usage))
        # - top 25 projects:
        if (dim(plotFrame)[1] >= 25) {
          plotFrame <- plotFrame[1:25, ]
        }
        plotFrame$Project <- factor(plotFrame$Project, 
                                    levels = plotFrame$Project[order(-plotFrame$Usage)])
        # - express labels as K, M:
        plotFrame$Label <- sapply(plotFrame$Usage, function(x) {
          if (x >= 1e+03 & x < 1e+06) {
            out <- paste(round(x/1e+03, 1), "K", sep = "")
          } else if (x > 1e+06) {
            out <- paste(round(x/1e+06, 1), "M", sep = "")
          } else {
            out <- as.character(x)
          }
          return(out)
        })
        # - Plot
        ggplot(plotFrame,
               aes(x = Project, y = Usage, label = Label)) +
          geom_bar(stat = "identity", width = .6, fill = "#4c8cff") +
          xlab('Projects') + ylab('Entity Usage') +
          ylim(0, max(plotFrame$Usage) + .1*max(plotFrame$Usage)) +
          scale_y_continuous(labels = comma) + 
          geom_label(size = 3, vjust = -.1) +
          theme_minimal() +
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
        plotFrame <- isolate(tabsDataset()) %>%
          group_by(Project) %>% 
          summarise(Usage = sum(Usage)) %>%
          arrange(desc(Usage))
        plotFrame
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
      
      #### ---  Chart: tabulations_categoriesChart
      output$tabulations_categoriesChart <- renderPlot({
        # - Chart Frame for output$tabulations_categoriesChart
        plotFrame <- isolate(tabsDataset()) %>%
          group_by(Category) %>% 
          summarise(Usage = sum(Usage)) %>%
          arrange(desc(Usage))
        # - top 25 categories:
        if (dim(plotFrame)[1] > 25) {
          plotFrame <- plotFrame[1:25, ]
        }
        plotFrame$Category <- factor(plotFrame$Category, 
                                    levels = plotFrame$Category[order(-plotFrame$Usage)])
        # - express labels as K, M:
        plotFrame$Label <- sapply(plotFrame$Usage, function(x) {
          if (x >= 1e+03 & x < 1e+06) {
            out <- paste(round(x/1e+03, 1), "K", sep = "")
          } else if (x > 1e+06) {
            out <- paste(round(x/1e+06, 1), "M", sep = "")
          } else {
            out <- as.character(x)
          }
          return(out)
        })
        # - Plot
        ggplot(plotFrame,
               aes(x = Category, y = Usage, label = Label)) +
          geom_bar(stat = "identity", width = .6, fill = "#4c8cff") +
          xlab('Category') + ylab('Entity Usage') +
          ylim(0, max(plotFrame$Usage) + .1*max(plotFrame$Usage)) +
          scale_y_continuous(labels = comma) + 
          geom_label(size = 3, vjust = -.1) +
          theme_minimal() +
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
        plotFrame <- isolate(tabsDataset()) %>%
          group_by(Category) %>% 
          summarise(Usage = sum(Usage)) %>%
          arrange(desc(Usage))
        plotFrame
      })
      # - Download: tabulations_categoriesChart
      output$tabulations_categoriesDownload_Frame <- downloadHandler(
        filename = function() {
          'WDCM_Data.csv'},
        content = function(file) {
          write.csv(tabulations_categoriesDownload_Frame(),
                    file,
                    quote = FALSE,
                    row.names = FALSE)
        },
        contentType = "text/csv"
      )
      
      #### ---  Chart: tabulations_projectTypesChart
      output$tabulations_projectTypesChart <- renderPlot({
        # - Chart Frame for output$tabulations_projectTypesChart
        plotFrame <- isolate(tabsDataset()) %>%
          group_by(`Project Type`) %>% 
          summarise(Usage = sum(Usage)) %>%
          arrange(desc(Usage))
        # - top 25 categories:
        if (dim(plotFrame)[1] > 25) {
          plotFrame <- plotFrame[1:25, ]
        }
        plotFrame$`Project Type` <- factor(plotFrame$`Project Type`, 
                                     levels = plotFrame$`Project Type`[order(-plotFrame$Usage)])
        # - express labels as K, M:
        plotFrame$Label <- sapply(plotFrame$Usage, function(x) {
          if (x >= 1e+03 & x < 1e+06) {
            out <- paste(round(x/1e+03, 1), "K", sep = "")
          } else if (x > 1e+06) {
            out <- paste(round(x/1e+06, 1), "M", sep = "")
          } else {
            out <- as.character(x)
          }
          return(out)
        })
        # - Plot
        ggplot(plotFrame,
               aes(x = `Project Type`, y = Usage, label = Label)) +
          geom_bar(stat = "identity", width = .6, fill = "#4c8cff") +
          xlab('Project Type') + ylab('Entity Usage') +
          ylim(0, max(plotFrame$Usage) + .1*max(plotFrame$Usage)) +
          scale_y_continuous(labels = comma) + 
          geom_label(size = 3, vjust = -.1) +
          theme_minimal() +
          theme(axis.text.x = element_text(angle = 90, size = 12, hjust = 1)) +
          theme(axis.title.x = element_text(size = 12)) +
          theme(axis.title.y = element_text(size = 12)) +
          theme(plot.title = element_text(size = 15)) %>%
          withProgress(message = 'Generating plot',
                       min = 0,
                       max = 1,
                       value = 1, {incProgress(amount = 0)})
      })
      # - Download Frame: tabulations_projectTypesChart
      tabulations_projectTypesChartDownload_Frame <- reactive({
        plotFrame <- isolate(tabsDataset()) %>%
          group_by(`Project Type`) %>% 
          summarise(Usage = sum(Usage)) %>%
          arrange(desc(Usage))
        plotFrame
      })
      # - Download: tabulations_projectTypesChart
      output$tabulations_projectTypesChart_Frame <- downloadHandler(
        filename = function() {
          'WDCM_Data.csv'},
        content = function(file) {
          write.csv(tabulations_projectTypesChartDownload_Frame(),
                    file,
                    quote = FALSE,
                    row.names = FALSE)
        },
        contentType = "text/csv"
      )
      
      #### ---  Chart: crosstabulations_projectsCategoriesChart
      output$crosstabulations_projectsCategoriesChart <- renderPlot({
        # - Chart Frame for output$crosstabulations_projectsCategoriessChart
        plotFrame <- isolate(tabsDataset()) %>%
          arrange(desc(Usage))
        projectOrder <- plotFrame %>%
          group_by(Project) %>% 
          summarise(Usage = sum(Usage)) %>%
          arrange(desc(Usage))
        selProj <- projectOrder$Project[1:25]
        plotFrame <- plotFrame %>% 
          filter(Project %in% selProj)
        # - express labels as K, M:
        plotFrame$Label <- sapply(plotFrame$Usage, function(x) {
          if (x >= 1e+03 & x < 1e+06) {
            out <- paste(round(x/1e+03, 1), "K", sep = "")
          } else if (x > 1e+06) {
            out <- paste(round(x/1e+06, 1), "M", sep = "")
          } else {
            out <- as.character(x)
          }
          return(out)
        })
        plotFrame$Project <- factor(plotFrame$Project,
                                    levels = selProj)
        # - Plot
        ggplot(plotFrame,
               aes(x = Project, y = Usage, label = Label)) +
          geom_line(size = .25, color = "#4c8cff", group = 1) +
          geom_point(size = 1.5, color = "#4c8cff") + 
          geom_point(size = 1, color = "white") + 
          geom_text_repel(data = plotFrame, 
                          aes(x = Project, y = Usage, label = Label), 
                          size = 3) +
          facet_wrap(~ Category, ncol = 3, scales = "free_y") +
          xlab('Project') + ylab('Entity Usage') +
          scale_y_continuous(labels = comma) + 
          theme_minimal() +
          theme(axis.text.x = element_text(angle = 90, size = 12, hjust = 1)) +
          theme(axis.title.x = element_text(size = 12)) +
          theme(axis.title.y = element_text(size = 12)) +
          theme(plot.title = element_text(size = 15)) %>%
          withProgress(message = 'Generating plot',
                       min = 0,
                       max = 1,
                       value = 1, {incProgress(amount = 0)})
      })
      # - Download Frame: crosstabulations_projectsCategoriesChart
      crosstabulations_projectsCategoriesChartDownload_Frame <- reactive({
        plotFrame <- isolate(tabsDataset()) %>%
          arrange(desc(Usage))
        plotFrame
      })
      # - Download: crosstabulations_projectsCategoriesFrame
      output$crosstabulations_projectsCategoriesFrame <- downloadHandler(
        filename = function() {
          'WDCM_Data.csv'},
        content = function(file) {
          write.csv(crosstabulations_projectsCategoriesChartDownload_Frame(),
                    file,
                    quote = FALSE,
                    row.names = FALSE)
        },
        contentType = "text/csv"
      )
      
      #### ---  Chart: crosstabulations_projectTypesCategoriesChart
      output$crosstabulations_projectTypesCategoriesChart <- renderPlot({
        # - Chart Frame for output$crosstabulations_projectTypesCategoriesChart
        plotFrame <- isolate(tabsDataset()) %>%
          group_by(`Project Type`, Category) %>% 
          summarise(Usage = sum(Usage)) %>% 
          arrange(desc(Usage))
        projectTypeOrder <- plotFrame %>% 
          group_by(`Project Type`) %>% 
          summarise(Usage = sum(Usage)) %>% 
          arrange(desc(Usage))
        plotFrame$`Project Type` <- factor(plotFrame$`Project Type`, 
                                    levels = projectTypeOrder$`Project Type`)
        # - express labels as K, M:
        plotFrame$Label <- sapply(plotFrame$Usage, function(x) {
          if (x >= 1e+03 & x < 1e+06) {
            out <- paste(round(x/1e+03, 1), "K", sep = "")
          } else if (x > 1e+06) {
            out <- paste(round(x/1e+06, 1), "M", sep = "")
          } else {
            out <- as.character(x)
          }
          return(out)
        })
        # - Plot
        ggplot(plotFrame,
               aes(x = `Project Type`, y = Usage, label = Label)) +
          geom_line(size = .25, color = "#4c8cff", group = 1) +
          geom_point(size = 1.5, color = "#4c8cff") + 
          geom_point(size = 1, color = "white") + 
          geom_text_repel(data = plotFrame, 
                          aes(x = `Project Type`, y = Usage, label = Label), 
                          size = 3) +
          facet_wrap(~ Category, ncol = 3, scales = "free_y") +
          xlab('Project Type') + ylab('Entity Usage') +
          ylim(0, max(plotFrame$Usage) + .5*max(plotFrame$Usage)) +
          scale_y_continuous(labels = comma) + 
          theme_minimal() +
          theme(axis.text.x = element_text(angle = 90, size = 12, hjust = 1)) +
          theme(axis.title.x = element_text(size = 12)) +
          theme(axis.title.y = element_text(size = 12)) +
          theme(plot.title = element_text(size = 15)) %>%
          withProgress(message = 'Generating plot',
                       min = 0,
                       max = 1,
                       value = 1, {incProgress(amount = 0)})
      })
      # - Download Frame: crosstabulations_projectTypeCategoriesChart
      crosstabulations_projectTypeCategoriesChartDownload_Frame <- reactive({
        plotFrame <- isolate(tabsDataset()) %>%
          group_by(`Project Type`, Category) %>% 
          summarise(Usage = sum(Usage)) %>% 
          arrange(desc(Usage))
        plotFrame
      })
      # - Download: crosstabulations_projectTypeCategoriesChartFrame
      output$crosstabulations_projectTypeCategoriesChartFrame <- downloadHandler(
        filename = function() {
          'WDCM_Data.csv'},
        content = function(file) {
          write.csv(crosstabulations_projectTypeCategoriesChartDownload_Frame(),
                    file,
                    quote = FALSE,
                    row.names = FALSE)
        },
        contentType = "text/csv"
      )
      
  }, ignoreNULL = FALSE)
  
    
  
  ### ----------------------------------
  ### --- TABLES
  ### ----------------------------------
  
  ### --- output$projectTable
  output$projectTable <- DT::renderDataTable({
    datatable(wdcmProject,
              options = list(
                pageLength = 20,
                width = '100%',
                columnDefs = list(list(className = 'dt-center', targets = "_all"))
              ),
              rownames = FALSE
    )
  }) %>% withProgress(message = 'Generating data',
                      min = 0,
                      max = 1,
                      value = 1, {incProgress(amount = 1)})
  
  ### --- output$CategoryTable
  output$CategoryTable <- DT::renderDataTable({
    datatable(wdcmCategory,
              options = list(
                pageLength = 20,
                width = '100%',
                columnDefs = list(list(className = 'dt-center', targets = "_all"))
              ),
              rownames = FALSE
    )
  }) %>% withProgress(message = 'Generating data',
                      min = 0,
                      max = 1,
                      value = 1, {incProgress(amount = 1)})
  
  ### --- output$projectCategoryDataTable
  output$projectCategoryDataTable <- DT::renderDataTable({
    datatable(wdcmProjectCategory,
              options = list(
                pageLength = 20,
                width = '100%',
                columnDefs = list(list(className = 'dt-center', targets = "_all"))
              ),
              rownames = FALSE
    )
  }) %>% withProgress(message = 'Generating data',
                      min = 0,
                      max = 1,
                      value = 1, {incProgress(amount = 1)})
  
  ### --- output$projectType
  output$projectType <- DT::renderDataTable({
    datatable(wdcmProjectType,
              options = list(
                pageLength = 20,
                width = '100%',
                columnDefs = list(list(className = 'dt-center', targets = "_all"))
              ),
              rownames = FALSE
    )
  }) %>% withProgress(message = 'Generating data',
                      min = 0,
                      max = 1,
                      value = 1, {incProgress(amount = 1)})
  
  ### --- output$projectTypeCategory
  output$projectTypeCategory <- DT::renderDataTable({
    datatable(wdcmProjectTypeCategory,
              options = list(
                pageLength = 20,
                width = '100%',
                columnDefs = list(list(className = 'dt-center', targets = "_all"))
              ),
              rownames = FALSE
    )
  }) %>% withProgress(message = 'Generating data',
                      min = 0,
                      max = 1,
                      value = 1, {incProgress(amount = 1)})
  
  
})
### --- END shinyServer




