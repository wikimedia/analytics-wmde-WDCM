
### ---------------------------------------------------------------------------
### --- wdcmModule_ETL.py
### --- Authors: Goran S. Milovanovic, Data Scientist, WMDE
### --- Developed under the contract between Goran Milovanovic PR Data Kolektiv
### --- and WMDE;
### --- Andrew Otto, Senior Systems Engineer — Operations/Analytics,
### --- Wikimedia Foundation.
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
from pyspark import SparkFiles
import numpy as np
import pandas as pd
import os
import subprocess
import gc
import re
from sys import stdin
import sys
from itertools import compress
import datetime
import xml.etree.ElementTree as ET
import requests
import json


### --- Constants and functions

# - Wikidata API prefix
API_prefix = 'https://www.wikidata.org/w/api.php?action=wbgetentities&'
# - batch_size for Wikidata API calls
API_batch_size = 50

# - fetchWDlabels function
def fetchWDlabels(API_prefix, ids, proxy):
    searchTerms = "|".join(ids)
    query = API_prefix + 'ids=' + searchTerms + '&' + \
            'props=labels&languages=en&sitefilter=wikidatawiki&languagefallback=true&format=json'
    res = requests.get(query, proxies = proxy)
    res = json.loads(res.text)
    items = list(res['entities'].keys())
    labels = list(map(lambda x: res['entities'][x].get('labels'), res['entities']))
    # - check for the existence of 'labels'
    presentLabel = [type(i) is dict for i in labels]
    items = list(compress(items, presentLabel))
    labels = list(compress(labels, presentLabel))
    # - check for the existence of 'en'
    labels = list(map(lambda x: x.get('en'), labels))
    presentLabel = [type(i) is dict for i in labels]
    items = list(compress(items, presentLabel))
    labels = list(compress(labels, presentLabel))
    # - check for the existence of 'value'
    labels = list(map(lambda x: x.get('value'), labels))
    presentLabel = [not i == "None" for i in labels]
    items = list(compress(items, presentLabel))
    labels = list(compress(labels, presentLabel))
    # - output
    return (items, labels)

# - fetchWDlabels_batch function
def fetchWDlabels_batch(API_prefix, entities, API_batch_size, proxy):
    # - labels list:
    labels = []
    # - found items list:
    foundItems = []
    # - cut into batches
    startIx = np.arange(0, len(entities), API_batch_size)
    stopIx = startIx + 49
    stopIx[len(stopIx)-1] = len(entities)
    # - fetch WD labels batch by batch
    for i in range(0, len(startIx)):
        ids = list(entities[startIx[i]:stopIx[i]])
        out = fetchWDlabels(API_prefix, ids, proxy)
        foundItems = foundItems + out[0]
        labels = labels + out[1]
    return(foundItems, labels)

# - list files to work with hdfs
def list_files(dir):
    if (dir.startswith('hdfs://')):
        # use hdfs -stat %n to just get file names in dir/*
        cmd = ['hdfs', 'dfs', '-stat', '%n', dir + "/*"]
        files = [str(f, 'utf-8') for f in subprocess.check_output(cmd).split(b"\n")]
    else:
        files = os.listdir(path=dir)
    return [os.path.join(dir, f) for f in files]

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
publicDir = params['publicDir']
topNpopularWDIitems = params['topNpopularWDIitems']
topNpopularCategoryWDItems = params['topNpopularCategoryWDItems']
itemsDir = params['itemsDir']
NItemsTFMatrix = params['NItemsTFMatrix']
http_proxy  = params['http_proxy']
https_proxy = params['https_proxy']
hdfsDir_WDCMCollectedItemsDir = params['hdfsCollectedItemsDir']
hdfsPATH_WDCMCollectedItems = params['hdfsPATH_WDCMCollectedItems']
proxyDict = {
              "http"  : http_proxy,
              "https" : https_proxy,
            }

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
wdcm_item.coalesce(1).toPandas().to_csv(wdcmOutdir + "wdcm_topItems.csv", header=True, index=False)

# - produce: wdcm_project
# - description: total item usage as sum(eu_count) per project
wdcm_project = sqlContext.sql('SELECT eu_project, SUM(eu_count) AS eu_count FROM wdcmmain GROUP BY eu_project \
                                    ORDER BY eu_count DESC')
wdcm_project.coalesce(1).toPandas().to_csv(wdcmOutdir + "wdcm_project.csv", header=True, index=False)

# - produce: wdcm_project_item100
# - description: total item usage as sum(eu_count) per project
# - ETL
wdcm_project_item100 = WDCM_MainTableRaw.\
    select(col('eu_project'), col('eu_entity_id'), col('eu_count'))\
    .filter(col('eu_count') > 1)

# - coalesce, filter, to Pandas, save
wdcm_project_item100 = wdcm_project_item100.coalesce(1).toPandas()
wdcm_project_item100 = wdcm_project_item100.sort_values('eu_count', ascending=False).groupby('eu_project').head(100)
# - fetch item labels for wdcm_project_item100
labs = fetchWDlabels_batch(API_prefix = API_prefix,
                           entities = list(set(list(wdcm_project_item100['eu_entity_id']))),
                           API_batch_size = API_batch_size,
                           proxy = proxyDict)
# - join labels to wdcm_project_item100
labsDict = {'eu_entity_id': pd.Series(labs[0]), 'eu_label': pd.Series(labs[1])}
labsDF = pd.DataFrame(labsDict)
wdcm_project_item100 = pd.merge(wdcm_project_item100, labsDF, on='eu_entity_id', how='left')
# - sort wdcm_project_item100
wdcm_project_item100 .sort_values(['eu_project', 'eu_entity_id'], ascending=[True, True])
# - save wdcm_project_item100 as .csv w. labels
wdcm_project_item100.to_csv(wdcmOutdir + "wdcm_project_item100.csv", header=True, index=False)

# - wdcm analytical tables per category
itemFiles = list_files(hdfsPATH_WDCMCollectedItems)
itemFiles = itemFiles[0:-1]
for itemFile in itemFiles:

    # category name:
    catName = itemFile.split("/")[len(itemFile.split("/"))-1]
    catName = catName.split(".")[0]
    catName = catName.split("_")[0]
    catName = re.sub("-", " ", catName)

    # read itemFile: list of items in category
    items = sqlContext.read.csv(itemFile, header=True)
    # fix items format:
    if (len(items.columns) == 3):
        items = items.drop("_c0")
        items = items.withColumnRenamed("item.item", "item")
        items = items.withColumnRenamed("item.category", "category")

    # left.join(newItem, wdcmmain)
    items = items.withColumnRenamed("item", "eu_entity_id").join(WDCM_MainTableRaw,
                                                          ["eu_entity_id"],
                                                          how='left')

    # - produce: wdcm_item_category_
    # - description: total usage of the top topNpopularCategoryWDItems entities usage from a category,
    # - per entity, one file per category
    fileName = "wdcm_category_item_" + catName + '.csv'

    wdcm_item_category = items.select(col('eu_entity_id'), col('eu_count')).groupBy("eu_entity_id").sum("eu_count").\
        withColumnRenamed("sum(eu_count)", "eu_count").\
        toPandas().sort_values(by="eu_count", ascending=False).head(int(topNpopularCategoryWDItems))
    # - fetch item labels for wdcm_project_item100
    labs = fetchWDlabels_batch(API_prefix=API_prefix,
                               entities=list(set(list(wdcm_item_category['eu_entity_id']))),
                               API_batch_size=API_batch_size,
                               proxy=proxyDict)
    # - join labels to wdcm_item_category
    labsDict = {'eu_entity_id': pd.Series(labs[0]), 'eu_label': pd.Series(labs[1])}
    labsDF = pd.DataFrame(labsDict)
    wdcm_item_category = pd.merge(wdcm_item_category, labsDF, on='eu_entity_id', how='left')
    # - sort wdcm_item_category
    wdcm_item_category.sort_values('eu_count', ascending=False)
    # - save wdcm_project_item100 as .csv w. labels
    wdcm_item_category.to_csv(wdcmOutdir + fileName, header=True, index=False)

    # - Term-Frequency Matrix
    # - filename
    fileName = "tfMatrix_" + catName + '.csv'
    # - drop aggregate 'eu_count' from wdcm_item_category
    tf_category = wdcm_item_category.drop('eu_count', axis=1)
    # - remove labels to create Spark dataframe
    tf_category = tf_category.drop('eu_label', axis=1)
    # - keep only top NItemsTFMatrix frequently used items
    tf_category = tf_category.head(int(NItemsTFMatrix))
    # - convert to Spark dataframe
    tf_category = sqlContext.createDataFrame(tf_category)
    # - left join with WDCM_MainTableRaw to pick up projects and eu_counts
    tf_category = tf_category.join(WDCM_MainTableRaw,
                                   ["eu_entity_id"],
                                   how='left')
    #- save
    tf_category = tf_category.toPandas()
    # - enter labels
    tf_category = pd.merge(tf_category, labsDF, on='eu_entity_id', how='left')
    # - to.csv
    tf_category.to_csv(wdcmOutdir + fileName, header=True, index=False)

    # - delete tf_category
    del tf_category
    # delete wdcm_item_category
    del wdcm_item_category

    # - produce: wdcm_category_sum_*
    # - description: sum(eu_count) for the category, one file per category
    fileName = "wdcm_category_sum_" + catName + '.csv'

    wdcm_category = items.groupBy("category").sum("eu_count").withColumnRenamed("sum(eu_count)", "eu_count").\
        toPandas()
    wdcm_category.to_csv(tempDir + fileName, header=True, index=False)

    # delete wdcm_category
    del wdcm_category

    # - produce: wdcm_project_category
    # - description: sum(eu_count) aggregated per project, one file per category
    fileName = "wdcm_project_category_" + catName + '.csv'

    wdcm_project_category = items.groupBy("category", "eu_project").sum("eu_count").\
        withColumnRenamed("sum(eu_count)", "eu_count").\
        toPandas().sort_values(by="eu_count", ascending=False)
    wdcm_project_category.to_csv(tempDir + fileName, header=True, index=False)

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
    # - fetch item labels for wdcm_project_category_item100
    labs = fetchWDlabels_batch(API_prefix=API_prefix,
                               entities=list(set(list(wdcm_project_category_item100['eu_entity_id']))),
                               API_batch_size=API_batch_size,
                               proxy=proxyDict)
    # - join labels to wdcm_project_category_item100
    labsDict = {'eu_entity_id': pd.Series(labs[0]), 'eu_label': pd.Series(labs[1])}
    labsDF = pd.DataFrame(labsDict)
    wdcm_project_category_item100 = pd.merge(wdcm_project_category_item100, labsDF, on='eu_entity_id', how='left')
    # - sort wdcm_project_category_item100
    wdcm_project_category_item100.sort_values('eu_count', ascending=False)
    # - save wdcm_project_category_item100 as .csv w. labels
    wdcm_project_category_item100.to_csv(tempDir + fileName, header=True, index=False)

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
ix = [bool(re.search("^wdcm_category_sum_", i)) for i in lF]
lF = list(compress(lF, ix))
# read files
df = pd.concat(map(pd.read_csv, map(lambda x: tempDir + x, lF)))
# - select, sort, and write
df[['category', 'eu_count']].sort_values('eu_count', ascending=False)\
    .to_csv(wdcmOutdir + "wdcm_category.csv", index=False)

# - OUTPUT: wdcm_category_item.csv
lF = os.listdir(path = publicDir + 'etl/')
ix = [bool(re.search("^wdcm_category_item_", i)) for i in lF]
lF = list(compress(lF, ix))
catNames = list(map(lambda x: x.split('_')[len(x.split('_'))-1], lF))
catNames = list(map(lambda x: x.split('.')[0], catNames))
catNames = np.repeat(catNames, 100)
# read files
df = pd.concat(map(lambda p: pd.read_csv(p, nrows=100), map(lambda x: publicDir + 'etl/' + x, lF)))
df['category'] = catNames
# - select, sort, and write
df.to_csv(wdcmOutdir + "wdcm_category_item.csv", index=False)

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
df[['eu_project', 'category', 'eu_count']].to_csv(wdcmOutdir + "wdcm_project_category.csv", header=True, index=False)

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
df.to_csv(wdcmOutdir + "wdcm_project_category_item100.csv", header=True, index=False)

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
