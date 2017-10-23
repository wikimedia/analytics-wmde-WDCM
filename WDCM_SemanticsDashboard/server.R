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
  projectTopic[[i]] <- fread(lF[i])
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
  
  
}) ### --- END shinyServer










