### ---------------------------------------------------------------------------
### --- WDCM Semantics Dashboard, v. Beta 0.1
### --- Script: ui.R, v. Beta 0.1
### ---------------------------------------------------------------------------

### --- Setup
rm(list = ls())
### --- general
library(shiny)
library(shinydashboard)
library(shinycssloaders)
### --- outputs
library(visNetwork)
library(rbokeh)
library(networkD3)
library(ggvis)
library(DT)

# - options
options(warn = -1)