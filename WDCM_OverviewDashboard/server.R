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
library(DT)
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
library(ggrepel)
library(scales)
library(ggvis)

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

### --- fetch wdcm2_project
q <- "SELECT * FROM wdcm2_project;"
res <- dbSendQuery(con, q)
wdcmProject <- fetch(res, -1)
dbClearResult(res) 

### --- determine how many project types are present
### --- and assign Brewer colors
lengthProjectColor <- length(unique(wdcmProject$projectype))
projectTypeColor <- brewer.pal(lengthProjectColor, "Set1")

### --- fetch wdcm2_project_category_2dmap
q <- "SELECT * FROM wdcm2_project_category_2dmap;"
res <- dbSendQuery(con, q)
wdcm2_project_category_2dmap <- fetch(res, -1)
dbClearResult(res)
colnames(wdcm2_project_category_2dmap)[3] <- "eu_project"
wdcm2_project_category_2dmap <- left_join(wdcm2_project_category_2dmap,
                                          wdcmProject,
                                          by = "eu_project")
labelSet <- unlist(lapply(unique(wdcm2_project_category_2dmap$projecttype), function(x){
  w <- which(wdcm2_project_category_2dmap$projecttype %in% x)
  lS <- arrange(wdcm2_project_category_2dmap[w, ], desc(eu_count))[1:5, ]
  lS$eu_project 
}))
labelSetSmall1 <- unlist(lapply(unique(wdcm2_project_category_2dmap$projecttype), function(x){
  w <- which(wdcm2_project_category_2dmap$projecttype %in% x)
  lS <- arrange(wdcm2_project_category_2dmap[w, ], desc(eu_count))[1, ]
  lS$eu_project 
}))
labelSetSmall3 <- unlist(lapply(unique(wdcm2_project_category_2dmap$projecttype), function(x){
  w <- which(wdcm2_project_category_2dmap$projecttype %in% x)
  lS <- arrange(wdcm2_project_category_2dmap[w, ], desc(eu_count))[1:3, ]
  lS$eu_project 
}))

wdcm2_project_category_2dmapReduceLabels <- wdcm2_project_category_2dmap 
wdcm2_project_category_2dmapReduceLabels$eu_project[which(!(wdcm2_project_category_2dmapReduceLabels$eu_project %in% labelSet))] <- ""
colnames(wdcm2_project_category_2dmap)[c(3, 5,6)] <- c('Project', 'Usage', 'Project Type')
wdcm2_project_category_2dmap$projectTypeColor <- sapply(wdcm2_project_category_2dmap$`Project Type`, function(x) {
  projectTypeColor[which(sort(unique(wdcm2_project_category_2dmap$`Project Type`)) %in% x)]
})

### --- fetch wdcm2_category
q <- "SELECT * FROM wdcm2_category;"
res <- dbSendQuery(con, q)
wdcmCategory <- fetch(res, -1)
dbClearResult(res) 

### ---fetch wdcm2_category_project_2dmap
q <- "SELECT * FROM wdcm2_category_project_2dmap;"
res <- dbSendQuery(con, q)
wdcm2_category_project_2dmap <- fetch(res, -1)
dbClearResult(res) 
wdcm2_category_project_2dmap <- left_join(wdcm2_category_project_2dmap,
                                          wdcmCategory,
                                          by = "category")
colnames(wdcm2_category_project_2dmap)[3:4] <- c('Category', 'Usage')

### --- fetch wdcm2_project_category
q <- "SELECT * FROM wdcm2_project_category;"
res <- dbSendQuery(con, q)
wdcmProjectCategory <- fetch(res, -1)
dbClearResult(res) 
colnames(wdcmProjectCategory) <- c('Project', 'Category', 'Usage', 'Project Type')

### --- Disconnect
dbDisconnect(con)

### --- shinyServer
shinyServer(function(input, output, session) {
  
  ### ----------------------------------
  ### --- TAB: Overview
  ### ----------------------------------
  
  ### --- output$overviewPlot
  output$overviewPlot <- renderPlot({
    ggplot(wdcm2_project_category_2dmapReduceLabels, aes(x = D1, y = D2,
                                                         color = projecttype,
                                                         label = eu_project)) +
      geom_point(aes(size = eu_count), shape=21) +
      scale_size(name = "Usage", 
                 breaks = waiver(), 
                 labels = comma,
                 limits = NULL, 
                 range = c(.5, 30), 
                 trans = "identity", 
                 guide = "legend") + 
      scale_colour_manual(values = projectTypeColor, name = "Project Type") +
      geom_text_repel(size = 5, fontface = 'bold', segment.size = .25, show.legend = FALSE) +
      theme_bw() +
      theme(axis.text.x = element_blank()) +
      theme(axis.text.y = element_blank()) +
      theme(axis.title.x = element_blank()) +
      theme(axis.title.y = element_blank()) +
      theme(axis.ticks.x = element_blank()) +
      theme(axis.ticks.y = element_blank()) +
      theme(panel.background = element_rect(color = "white", fill = "white")) +
      theme(panel.border = element_blank()) +
      theme(panel.grid = element_blank()) + 
      theme(legend.text = element_text(size = 14)) +
      theme(legend.title = element_text(size = 15))
  }) %>% withProgress(message = 'Generating plot',
                      min = 0,
                      max = 1,
                      value = 1, {incProgress(amount = 1)})
  
  ### --- output$overviewPlotDynamic
  output$overviewPlotDynamic <- renderRbokeh({
    outFig <- figure(width = 1400, height = 900, logo = NULL) %>%
      ly_points(D1, D2, 
                data = wdcm2_project_category_2dmap,
                size = log(Usage),
                color = 'Project Type', 
                hover = list(Project, Usage)) %>% 
      x_axis(visible = F) %>% 
      y_axis(visible = F) %>% 
      theme_grid(which = c("x", "y"), 
                 grid_line_color = "white") %>% 
      theme_plot(outline_line_alpha = 0) %>% 
      set_palette(discrete_color = pal_color(projectTypeColor))
    outFig
  }) %>% withProgress(message = 'Generating plot',
                      min = 0,
                      max = 1,
                      value = 1, {incProgress(amount = 1)})
  
  ### --- output$usageTendencyPlot
  output$usageTendencyPlot <- renderPlot({
    ggplot(wdcm2_category_project_2dmap, aes(x = D1, 
                                             y = D2, 
                                             label = Category)) +
      scale_color_discrete(guide=FALSE) +
      geom_point(aes(size = Usage), fill = "white", color = "darkblue", shape=21) +
      scale_size(name = "Usage", 
                 breaks = waiver(), 
                 labels = comma,
                 limits = NULL, 
                 range = c(2, 20), 
                 trans = "identity", 
                 guide = "legend") + 
      theme_bw() +
      geom_text(size = 4, show.legend = FALSE) +
      theme(axis.text.x = element_blank()) +
      theme(axis.text.y = element_blank()) +
      theme(axis.title.x = element_blank()) +
      theme(axis.title.y = element_blank()) +
      theme(axis.ticks.x = element_blank()) +
      theme(axis.ticks.y = element_blank()) +
      theme(panel.border = element_blank()) +
      theme(panel.grid = element_blank()) + 
      theme(legend.text = element_text(size = 10)) +
      theme(legend.title = element_text(size = 12))
  }) %>% withProgress(message = 'Generating plot',
                      min = 0,
                      max = 1,
                      value = 1, {incProgress(amount = 1)})
  
  ### --- output$projectRankFrequencyPlot
  output$projectRankFrequencyPlot <- renderPlot({
    rank <- order(wdcmProject$eu_count)
    frequency <- wdcmProject$eu_count[rank]
    project <- wdcmProject$eu_project[rank]
    dataSet <- data.frame(Rank = rank,
                          Frequency = frequency, 
                          Project = project,
                          stringsAsFactors = F)
    dataSet$Project[which(!(dataSet$Project %in% labelSetSmall1))] <- ""
    dataSet <- dataSet[order(-dataSet$Frequency), ]
    ggplot(dataSet, aes(x = Rank, 
                        y = Frequency,
                        label = Project)) +
      geom_path(size = .25, color = "darkblue") + 
      geom_point(size = 1, color = "darkblue") + 
      geom_point(size = .65, color = "white") + 
      scale_y_continuous(labels = comma) +
      xlab("Project Usage Rank") + ylab("Project Usage") + 
      geom_text_repel(size = 3, segment.size = .15, show.legend = FALSE) +
      theme_bw() +
      theme(axis.text.x = element_text(size = 9)) +
      theme(axis.text.y = element_text(size = 9)) +
      theme(axis.title.x = element_text(size = 12)) +
      theme(axis.title.y = element_text(size = 12)) +
      theme(axis.ticks.x = element_blank()) +
      theme(axis.ticks.y = element_blank()) +
      theme(panel.border = element_blank()) +
      theme(panel.grid = element_blank()) + 
      theme(legend.text = element_text(size = 10)) +
      theme(legend.title = element_text(size = 12))
  }) %>% withProgress(message = 'Generating plot',
                      min = 0,
                      max = 1,
                      value = 1, {incProgress(amount = 1)})
  
  ### --- output$projectLogRankLogFrequencyPlot
  output$projectLogRankLogFrequencyPlot <- renderPlot({
    rank <- order(wdcmProject$eu_count)
    frequency <- wdcmProject$eu_count[rank]
    project <- wdcmProject$eu_project[rank]
    dataSet <- data.frame(Rank = log(rank),
                          Frequency = log(frequency), 
                          Project = project,
                          stringsAsFactors = F)
    dataSet$Project[which(!(dataSet$Project %in% labelSetSmall3))] <- ""
    dataSet <- dataSet[order(-dataSet$Frequency), ]
    ggplot(dataSet, aes(x = Rank, 
                        y = Frequency,
                        label = Project)) +
      geom_path(size = .25, color = "red") + 
      geom_smooth(size = .25, method = "lm", color = "red") + 
      geom_point(size = 1, color = "red") + 
      geom_point(size = .65, color = "white") + 
      scale_y_continuous(labels = comma) +
      xlab("log(Project Usage Rank)") + ylab("log(Project Usage)") + 
      geom_text_repel(size = 3, segment.size = .15, show.legend = FALSE) +
      theme_bw() +
      theme(axis.text.x = element_text(size = 9)) +
      theme(axis.text.y = element_text(size = 9)) +
      theme(axis.title.x = element_text(size = 12)) +
      theme(axis.title.y = element_text(size = 12)) +
      theme(axis.ticks.x = element_blank()) +
      theme(axis.ticks.y = element_blank()) +
      theme(panel.border = element_blank()) +
      theme(panel.grid = element_blank()) + 
      theme(legend.text = element_text(size = 10)) +
      theme(legend.title = element_text(size = 12))
  }) %>% withProgress(message = 'Generating plot',
                      min = 0,
                      max = 1,
                      value = 1, {incProgress(amount = 1)})
  
  ### --- output$projectCategoryCross
  output$projectCategoryCross <- renderPlot({
    dataSet <- wdcmProjectCategory
    dataSet$Project <- NULL
    dataSet <- dataSet %>% 
      group_by(`Project Type`, Category) %>%
      summarise(Usage = sum(Usage)) %>% 
      as.data.frame()
    dataSet$`Project Type` <- factor(dataSet$`Project Type`, 
                                     levels = unique(dataSet$`Project Type`))
    dataSet$Category <- factor(dataSet$Category, 
                               levels = unique(dataSet$Category))
    ggplot(dataSet, aes(x = Category, 
                        y = Usage,
                        fill = `Project Type`,
                        color = `Project Type`)) +
      geom_bar(stat = "identity", width = .35) +
      scale_fill_manual(values = projectTypeColor, name = "Project Type") +
      scale_color_manual(values = projectTypeColor) +
      scale_y_continuous(labels = comma) +
      xlab("Category") + ylab("Usage") + 
      facet_grid(`Project Type` ~ ., scales = "free_y") +
      theme_bw() +
      theme(axis.text.x = element_text(angle = 90, size = 11, hjust = 1)) +
      theme(axis.text.y = element_text(size = 9)) +
      theme(axis.title.x = element_text(size = 12)) +
      theme(axis.title.y = element_text(size = 12)) +
      theme(axis.ticks.x = element_blank()) +
      theme(axis.ticks.y = element_blank()) +
      theme(panel.background = element_rect(fill = "snow2")) +
      theme(panel.border = element_blank()) +
      theme(panel.grid = element_blank()) + 
      theme(legend.position = "none") +
      theme(strip.background = element_blank()) +
      theme(strip.text = element_text(size = 11))
  }) %>% withProgress(message = 'Generating plot',
                      min = 0,
                      max = 1,
                      value = 1, {incProgress(amount = 1)})
  
  ### --- output$projectVolume
  output$projectVolume <- renderPlot({
    minQ <- input$volumeSlider[1]/100
    maxQ <- input$volumeSlider[2]/100
    wSel <- which(wdcmProject$eu_count <= quantile(wdcmProject$eu_count, maxQ) & 
                    wdcmProject$eu_count >= quantile(wdcmProject$eu_count, minQ))
    dataSet <- wdcmProject[wSel, ] %>% 
      arrange(desc(eu_count)) %>% as.data.frame()
    colnames(dataSet) <- c('Project', 'Usage', 'Project Type')
    if (dim(dataSet)[1] > 30) {
      dataSet <- dataSet[1:30, ]
    }
    dataSet$Project <- factor(dataSet$Project)
    dataSet <- dataSet[order(dataSet$Usage), ]
    ggplot(dataSet, aes(x = reorder(Project, Usage), 
                        y = Usage,
                        fill = `Project Type`)) +
      geom_bar(stat = "identity", width = .2) + 
      xlab("Project") + ylab("Usage") + 
      scale_fill_manual(values = projectTypeColor[which(sort(unique(wdcmProject$projectype)) %in% dataSet$`Project Type`)], 
                        name = "Project Type") +
      scale_y_continuous(labels = comma) +
      theme_bw() +
      theme(axis.text.x = element_text(angle = 90, size = 11, hjust = 1)) +
      theme(axis.text.y = element_text(size = 9)) +
      theme(axis.title.x = element_text(size = 12)) +
      theme(axis.title.y = element_text(size = 12)) +
      theme(axis.ticks.x = element_blank()) +
      theme(axis.ticks.y = element_blank()) +
      theme(panel.border = element_blank()) +
      theme(panel.grid = element_blank())
  }) %>% withProgress(message = 'Generating plot',
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
  
  ### --- output$projectDataTable
  output$projectDataTable <- DT::renderDataTable({
    dataSet <- wdcmProject
    colnames(dataSet) <- c('Project', 'Usage', 'Project Type')
    datatable(dataSet,
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




