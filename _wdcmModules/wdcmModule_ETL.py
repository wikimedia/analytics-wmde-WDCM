
### ---------------------------------------------------------------------------
### --- wdcmModule_ETL.py
### --- Author: Goran S. Milovanovic, Data Analyst, WMDE
### --- Developed under the contract between Goran Milovanovic PR Data Kolektiv
### --- and WMDE.
### --- Contact: goran.milovanovic_ext@wikimedia.de
### --- January 2019.
### ---------------------------------------------------------------------------
### --- DESCRIPTION:
### --- Pyspark ETL procedures for the WDCM system
### --- NOTE: the execution of this WDCM script is always dependent upon the
### --- wdcm_project_category_item100
### --- previous WDCM_Sqoop_Clients.R.
### ---------------------------------------------------------------------------
### ---------------------------------------------------------------------------
### --- LICENSE:
### ---------------------------------------------------------------------------
### --- GPL v2
### --- This file is part of Wikidata Concepts Monitor (WDCM)
### ---
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
### ---------------------------------------------------------------------------
### --- Script 2: wdcmModule_ETL.py
### ---------------------------------------------------------------------------
### --- DESCRIPTION:
### --- wdcmModule_ETL.py performs ETL procedures
### --- from the goransm.wdcm_clients_wb_entity_usage
### --- table (lives in: Hadoop, WMF Data Lake).
### ---------------------------------------------------------------------------

### --- Modules
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col
import numpy as np
import pandas as pd
import os
import gc
import re
from sys import stdin
import sys
from itertools import compress
import datetime
import xml.etree.ElementTree as ET

### --- start Time
startTime = datetime.datetime.now()
# - to runtime log:
print("WDCM ETL Module start at: " + str(startTime))


### --- parse WDCM parameters
# - where is the script being run from:
parsFile = str(sys.path[0]) + "/wdcmConfig.xml"
# - parse wdcmConfig.xml
tree = ET.parse(parsFile)
root = tree.getroot()
k = [elem.tag for elem in root.iter()]
v = [x.text for x in root.iter()]
params = dict(zip(k, v))

### --- dir structure and params
tempDir = params['tempDir']
wdcmOutdir = params['etlDir']
logDir = params['logDir']
topNpopularWDIitems = params['topNpopularWDIitems']
topNpopularCategoryWDItems = params['topNpopularCategoryWDItems']
NItemsTFMatrix = params['NItemsTFMatrix']

### --- Init Spark

# - Spark Session
sc = SparkSession\
    .builder\
    .appName("Wikidata Concepts Monitor ETL")\
    .enableHiveSupport()\
    .getOrCreate()
# - prevent verbose logs

# - SQL Context
sqlContext = pyspark.SQLContext(sc)

### --- Produce WDCM_MainTableRaw

# - from wdcm_clients_wb_entity_usage
WDCM_MainTableRaw = sqlContext.sql('SELECT eu_entity_id, wiki_db AS eu_project, COUNT(*) AS eu_count FROM \
                                        (SELECT DISTINCT eu_entity_id, eu_page_id, wiki_db \
                                        FROM goransm.wdcm_clients_wb_entity_usage) \
                                        AS t WHERE eu_entity_id RLIKE "^Q" GROUP BY wiki_db, eu_entity_id')

# - cache WDCM_MainTableRaw
WDCM_MainTableRaw.cache()

# - create View from WDCM_MainTableRaw
WDCM_MainTableRaw.createTempView("wdcmmain")

### --- Produce WDCM Analytical Tables

# - produce: wdcm_topItems
# - description: top 100,000 most popular Wikidata items
wdcm_item = sqlContext.sql('SELECT eu_entity_id, SUM(eu_count) AS eu_count FROM wdcmmain GROUP BY eu_entity_id \
                                    ORDER BY eu_count DESC LIMIT ' + topNpopularWDIitems)
wdcm_item.coalesce(1).toPandas().to_csv(wdcmOutdir + "wdcm_topItems.csv", header=True)

# - produce: wdcm_project
# - description: total item usage as sum(eu_count) per project
wdcm_project = sqlContext.sql('SELECT eu_project, SUM(eu_count) AS eu_count FROM wdcmmain GROUP BY eu_project \
                                    ORDER BY eu_count DESC')
wdcm_project.coalesce(1).toPandas().to_csv(wdcmOutdir + "wdcm_project.csv", header=True)

# - produce: wdcm_project_item100
# - description: total item usage as sum(eu_count) per project
# - ETL
wdcm_project_item100 = WDCM_MainTableRaw.\
    select(col('eu_project'), col('eu_entity_id'), col('eu_count'))\
    .filter(col('eu_count') > 1)

# - coalesce, filter, to Pandas, save
wdcm_project_item100 = wdcm_project_item100.coalesce(1).toPandas()
wdcm_project_item100 = wdcm_project_item100.sort_values('eu_count', ascending=False).groupby('eu_project').head(100)
wdcm_project_item100.to_csv(wdcmOutdir + "wdcm_project_item100.csv", header=True)

# - wdcm analytical tables per category
itemFiles = os.listdir(path='/home/goransm/PyScripts/WDCM_CollectedItems')
for itemFile in itemFiles:

    # category name:
    catName = re.sub("-", " ", itemFile.split("_")[0])

    # read itemFile: list of items in category
    items = sqlContext.read.csv(itemFile, header=True)

    # left.join(newItem, wdcmmain)
    items = items.select("eu_entity_id", "category").join(WDCM_MainTableRaw,
                                                          ["eu_entity_id"],
                                                          how='left')

    # - produce: wdcm_item_category_
    # - description: total sum of the top 10,000 entities usage from a category, per entity, one file per category
    fileName = "wdcm_category_item_" + catName + '.csv'

    wdcm_item_category = items.groupBy("eu_entity_id").sum("eu_count").withColumnRenamed("sum(eu_count)", "eu_count").\
        toPandas().sort_values(by="eu_count", ascending=False).head(int(topNpopularCategoryWDItems))
    wdcm_item_category.to_csv(wdcmOutdir + fileName, header=True)

    # - Term-Frequency Matrix
    # - filename
    fileName = "tfMatrix_" + catName + '.csv'
    # - drop aggregate 'eu_count' from wdcm_item_category
    tf_category = wdcm_item_category.drop(['eu_count'], axis=1)
    # - keep only top 1000 frequently used items
    tf_category = tf_category.head(int(NItemsTFMatrix))
    # - convert to Spark dataframe
    tf_category = sqlContext.createDataFrame(tf_category)
    # - left join with WDCM_MainTableRaw to pick up projects and eu_counts
    tf_category = tf_category.join(WDCM_MainTableRaw,
                                   ["eu_entity_id"],
                                   how='left')
    #- save
    tf_category = tf_category.toPandas()
    tf_category.to_csv(wdcmOutdir + fileName, header=True)
    # - delete tf_category
    del tf_category

    # delete wdcm_item_category
    del wdcm_item_category

    # - produce: wdcm_category
    # - description: sum(eu_count) for the category, one file per category
    fileName = "wdcm_category_" + catName + '.csv'

    wdcm_category = items.groupBy("category").sum("eu_count").withColumnRenamed("sum(eu_count)", "eu_count").\
        toPandas()
    wdcm_category.to_csv(tempDir + fileName, header=True)

    # delete wdcm_category
    del wdcm_category

    # - produce: wdcm_project_category
    # - description: sum(eu_count) aggregated per project, one file per category
    fileName = "wdcm_project_category_" + catName + '.csv'

    wdcm_project_category = items.groupBy("category", "eu_project").sum("eu_count").\
        withColumnRenamed("sum(eu_count)", "eu_count").\
        toPandas().sort_values(by="eu_count", ascending=False)
    wdcm_project_category.to_csv(tempDir + fileName, header=True)

    # delete wdcm_project_category
    del wdcm_project_category

    # - produce: wdcm_project_category_item100
    # - description: top 100 WD items per project and per category, one file per category
    fileName = "wdcm_project_category_item100_" + catName + '.csv'

    wdcm_project_category_item100 = items\
        .select(col('eu_project'), col('category'), col('eu_entity_id'), col('eu_count')) \
        .groupBy(['eu_project', 'category', 'eu_entity_id']).sum('eu_count')\
        .withColumnRenamed('sum(eu_count)', 'eu_count')\
        .filter(col('eu_count') > 1)

    # - coalesce, filter, to Pandas, save
    wdcm_project_category_item100 = wdcm_project_category_item100.coalesce(1).toPandas()
    wdcm_project_category_item100 = wdcm_project_category_item100.sort_values('eu_count', ascending=False)\
        .groupby(['eu_project', 'category']).head(100)
    wdcm_project_category_item100.to_csv(tempDir + fileName, header=True)

    # - delete wdcm_project_category_item100
    del wdcm_project_category_item100

    # - unpersist items
    items.unpersist()

### --- end Time
endTime = datetime.datetime.now()

### --- report update time
print("Total update time: " + str(endTime - startTime))

### --- Merge per item category outputs

# - OUTPUT: wdcm_category.csv
lF = os.listdir(path=tempDir)
ix = [bool(re.search("^wdcm_category_", i)) for i in lF]
lF = list(compress(lF, ix))
# read files
df = pd.concat(map(pd.read_csv, map(lambda x: tempDir + x, lF)))
# - select, sort, and write
df[['category', 'eu_count']].sort_values('eu_count', ascending=False).to_csv(wdcmOutdir + "wdcm_category.csv")

# - OUTPUT: wdcm_project_category.csv
lF = os.listdir(path=tempDir)
ix = [bool(re.search("^wdcm_project_category", i)) for i in lF]
lF = list(compress(lF, ix))
ix = [bool(re.search("_item100_", i)) for i in lF]
ix = [not x for x in ix]
lF = list(compress(lF, ix))
# read files
df = pd.concat(map(pd.read_csv, map(lambda x: tempDir + x, lF)))
# - select, sort, and write
df = df.dropna(axis=0)
df[['eu_project', 'category', 'eu_count']].to_csv(wdcmOutdir + "wdcm_project_category.csv")

# - OUTPUT: wdcm_project_category_item100.csv
lF = os.listdir(path=tempDir)
ix = [bool(re.search("^wdcm_project_category_item100", i)) for i in lF]
lF = list(compress(lF, ix))
# read files
df = pd.concat(\
    map(\
        lambda y: pd.read_csv(y, \
                              dtype={'eu_project':str, 'category':str, 'eu_entity_id':str, \
                                     'eu_count':float}),\
        map(lambda x: tempDir + x, lF)))
# - select, sort, and write
df = df.dropna(axis=0)
df.to_csv(wdcmOutdir + "wdcm_project_category_item100.csv")

### --- end Time
endTime = datetime.datetime.now()
# - to runtime log:
print("WDCM ETL Module completed at: " + str(endTime))
sdiff = endTime - startTime
print("WDCM ETL Module total runtime: " + str(sdiff.total_seconds()/60) + " minutes.")

### --- log ETL:
logFile = pd.read_csv(logDir + 'WDCM_MainReport.csv')
data = {'Step': ['ML'], 'Time': [str(endTime)]}
newReport = pd.DataFrame.from_dict(data)
logFile.append(newReport)
logFile = logFile.drop(['Unnamed: 0'], axis=1)
logFile.to_csv(logDir + 'WDCM_MainReport.csv')
