
### --- Auxiliary WDCM Analytics

### --- Part 1.
### --- WDCM_Sqoop_Clients.R output analytics
rm(list=ls())
setwd('/home/goransm/Desktop/___DataKolektiv/Projects/WikimediaDEU/_WMDE_Projects/WDCM_Dev/WDCM/_misc/')
sqoopReport <- read.csv('wdcmSqoopReport_2017-07-21 10:31:45.csv',
                        header = T,
                        check.names = F,
                        row.names = 1,
                        stringsAsFactors = F)
### --- total WDCM_Sqoop_Clients.R runtime:
totalRuntime <- difftime(sqoopReport$endTime[length(sqoopReport$endTime)],
                         sqoopReport$startTime[1],
                         units = "hours")
### --- WDCM_Sqoop_Clients.R runtime distribution over client projects
sqoopReport$timeDiff <- difftime(sqoopReport$endTime,
                                 sqoopReport$startTime,
                                 units = "mins")
hist(as.numeric(sqoopReport$timeDiff), 50,
     main = paste("Sqoop Client Projects\nRuntime Distribution; Total Runtime = ",
                  as.character(round(totalRuntime, 2)),
                  " hours",
                  sep = ""),
     col = "orange",
     xlab = "Sqoop runtime\n(minutes)",
     cex.main = .95)
### --- the most demanding client project(s) to Sqoop
sqoopReport$project[which.max(sqoopReport$timeDiff)]
sqoopReport$timeDiff[which.max(sqoopReport$timeDiff)]
### --- the least demanding client project(s) to Sqoop
sqoopReport$project[which.min(sqoopReport$timeDiff)]
sqoopReport$timeDiff[which.min(sqoopReport$timeDiff)]


