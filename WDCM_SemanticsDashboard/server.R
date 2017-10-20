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