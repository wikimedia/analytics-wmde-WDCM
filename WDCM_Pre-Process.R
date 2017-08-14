
### ---------------------------------------------------------------------------
### --- WDCM Process Module, v. Beta 0.1
### --- Script: WDCM_Pre-Process.R, v. Beta 0.1
### --- Author: Goran S. Milovanovic, Data Analyst, WMDE
### --- Developed under the contract between Goran Milovanovic PR Data Kolektiv 
### --- and WMDE.
### --- Contact: goran.milovanovic_ext@wikimedia.de
### ---------------------------------------------------------------------------
### --- DESCRIPTION:
### --- WDCM_Pre-Process.R works with the
### --- wdcm_maintable Hive table on hdfs, database: goransm
### --- to produce the .tsv files that migrate to
### --- the wikidataconcepts.wmflabs.org Cloud VPS instance
### --- from production (currently: stat1005.eqiad.wmnet).
### ---------------------------------------------------------------------------
### --- INPUT: 
### --- wdcm_maintable Hive table on hdfs, database: goransm
### ---------------------------------------------------------------------------
### --- OUTPUT: 
### --- Results are stored locally as .tsv files on production,
### --- with each file encompassing the data for one client project.
### --- The outputs are stored locally on stat1005.eqiad.wmnet in:
### --- /home/goransm/WDCM_DataOUT/WDCM_DataOUT_ClientWDUsage
### --- These output .tsv files migrate to Labs:
### --- wikidataconcepts.wmflabs.org Cloud VPS instance
### --- where they are then processed by the WDCM Process Module.
### ---------------------------------------------------------------------------
### --- RUN:
### --- nohup Rscript /home/goransm/RScripts/WDCM_R/WDCM_Pre-Process.R &
### ---------------------------------------------------------------------------

### ---------------------------------------------------------------------------
### --- GPL v2
### ---------------------------------------------------------------------------
### --- This file is part of Wikidata Concepts Monitor (WDCM)
### --- WDCM is free software: you can redistribute it and/or modify
### --- it under the terms of the GNU General Public License as published by
### --- the Free Software Foundation, either version 2 of the License, or
### --- (at your option) any later version.
### ---
### --- WDCM is distributed in the hope that it will be useful,
### --- but WITHOUT ANY WARRANTY; without even the implied warranty of
### --- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
### --- GNU General Public License for more details.
### ---
### --- You should have received a copy of the GNU General Public License
### --- along with WDCM. If not, see <http://www.gnu.org/licenses/>.
### ---------------------------------------------------------------------------

### --- Setup
rm(list = ls())
scriptDir <- '/home/goransm/RScripts/WDCM_R/'
dataInputDir <- '/home/goransm/WDCM_DataIN/WDCM_DataIN_SearchItems/'
setwd(scriptDir)

### --- produce wdcm_project_category.tsv from wdcm_maintable (hdfs, database: goransm)
hiveQLquery <- 'USE goransm; 
                SET hive.mapred.mode=unstrict; 
                SELECT eu_project, category, SUM(eu_count) AS eu_count 
                  FROM wdcm_maintable 
                  GROUP BY eu_project, category;'
system(command = paste('beeline -e "', 
                       hiveQLquery, 
                       '" > /home/goransm/WDCM_DataOUT/WDCM_DataOUT_ClientWDUsage/wdcm_project_category.tsv',
                       sep = ""),
       wait = TRUE)

### --- produce wdcm_project.tsv from wdcm_maintable (hdfs, database: goransm)
hiveQLquery <- 'USE goransm; 
                SET hive.mapred.mode=unstrict; 
                SELECT eu_project, SUM(eu_count) AS eu_count 
                  FROM wdcm_maintable 
                  GROUP BY eu_project;'
system(command = paste('beeline -e "', 
                       hiveQLquery, 
                       '" > /home/goransm/WDCM_DataOUT/WDCM_DataOUT_ClientWDUsage/wdcm_project.tsv',
                       sep = ""),
       wait = TRUE)

### --- produce wdcm_category.tsv from wdcm_maintable (hdfs, database: goransm)
hiveQLquery <- 'USE goransm; 
                SET hive.mapred.mode=unstrict; 
                SELECT category, SUM(eu_count) AS eu_count 
                  FROM wdcm_maintable 
                  GROUP BY category;'
system(command = paste('beeline -e "', 
                       hiveQLquery, 
                       '" > /home/goransm/WDCM_DataOUT/WDCM_DataOUT_ClientWDUsage/wdcm_category.tsv',
                       sep = ""),
       wait = TRUE)

### --- produce wdcm_item.tsv from wdcm_maintable (hdfs, database: goransm)
### --- NOTE: one .tsv file per category (~14M rows, causes Java gc overflow from beeline...)

# - read item categories:
setwd(dataInputDir)
idFiles <- list.files()
idFiles <- idFiles[grepl(".csv$", idFiles)]
categories <- unname(sapply(idFiles, function(x) {
  strsplit(x, split = "_")[[1]][1]
}))

for (i in 1:length(categories)) {
  filename <- paste("wdcm_item_", gsub(" ", "", categories[i], fixed = T), ".tsv", sep = "")
  hiveQLquery <- paste(
    'USE goransm; SELECT eu_entity_id, SUM(eu_count) AS eu_count FROM wdcm_maintable WHERE category=\\"', 
    categories[i], 
    '\\" GROUP BY eu_entity_id ORDER BY eu_count DESC LIMIT 10000000;', 
    sep = "")
                  
  system(command = paste('beeline -e "',
                         hiveQLquery,
                         '" > /home/goransm/WDCM_DataOUT/WDCM_DataOUT_ClientWDUsage/',
                         filename,
                         sep = ""),
         wait = TRUE)
}





