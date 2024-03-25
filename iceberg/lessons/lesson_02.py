#!/usr/bin/env python
# coding: utf-8

# # Apache Iceberg and pySpark
# 

# ## CREATE table

# In[ ]:





# In[1]:


import pyspark
import pyspark
from pyspark.sql import SparkSession
import os


# In[2]:


## DEFINE SENSITIVE VARIABLES
NESSIE_URI = "http://nessie:19120/api/v1"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"


# In[3]:


conf = (
    pyspark.SparkConf()
        .setAppName('app_name')
        #packages
        .set('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.1,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.3_2.12:0.67.0,software.amazon.awssdk:bundle:2.17.178,software.amazon.awssdk:url-connection-client:2.17.178')
        #SQL Extensions
        .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions')
        #Configuring Catalog
        .set('spark.sql.catalog.nessie', 'org.apache.iceberg.spark.SparkCatalog')
        .set('spark.sql.catalog.nessie.uri', NESSIE_URI)
        .set('spark.sql.catalog.nessie.ref', 'main')
        .set('spark.sql.catalog.nessie.authentication.type', 'NONE')
        .set('spark.sql.catalog.nessie.catalog-impl', 'org.apache.iceberg.nessie.NessieCatalog')
        .set('spark.sql.catalog.nessie.warehouse', 's3a://warehouse')
        .set('spark.sql.catalog.nessie.s3.endpoint', 'http://minio:9000')
        .set('spark.sql.catalog.nessie.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
        #MINIO CREDENTIALS
        .set('spark.hadoop.fs.s3a.access.key', MINIO_ACCESS_KEY)
        .set('spark.hadoop.fs.s3a.secret.key', MINIO_SECRET_KEY)
)


# In[4]:


## Start Spark Session
spark = SparkSession.builder.config(conf=conf).getOrCreate()
print("Spark Running")


# In[8]:


## LOAD A CSV INTO AN SQL VIEW
csv_df = spark.read.format("csv").option("header", "true").load("sampledata/Worker_Coops.csv")
csv_df.createOrReplaceTempView("df_worker_coops")


# In[12]:


csv_df.printSchema()


# In[9]:


csv_df.show( 2 )


# In[18]:


query = """
SELECT `Business Name (DBA)` as dba, 
        city, 
        state,
        zip,
        `Community Board` as community_board
        
FROM df_worker_coops
LIMIT 5
"""
spark.sql( query ).show()


# In[ ]:





# In[19]:


## CREATE AN ICEBERG TABLE FROM THE SQL VIEW
query = '''
CREATE TABLE IF NOT EXISTS nessie.worker_coops 
USING iceberg 
AS 

SELECT * 
FROM df_worker_coops
'''

spark.sql( query ).show()


# In[20]:


## QUERY THE ICEBERG TABLE
spark.sql("SELECT * FROM nessie.worker_coops limit 10").show()


# ### SELECT our table from Iceberg Data Lakehouse in Nessie Catalog

# In[27]:


## CREATE AN ICEBERG TABLE FROM THE SQL VIEW
query = '''
SELECT `Business Name (DBA)` as dba, 
        city, 
        state,
        zip,
        `Community Board` as community_board

FROM   nessie.worker_coops 
'''

spark.sql( query ).show()


# In[22]:


## QUERY THE COUNT OF ENTRIES
spark.sql("SELECT Count(*) as Total FROM nessie.worker_coops").show()


# In[29]:


query = '''
SELECT zip, Count(*) as Total 
FROM nessie.worker_coops
GROUP BY zip
ORDER BY 2
'''

spark.sql( query ).show()


# In[23]:


## CREATE A BRANCH WITH NESSIE
spark.sql("CREATE BRANCH IF NOT EXISTS lesson2 IN nessie")


# In[24]:


## SWTICH TO THE NEW BRANCH
spark.sql("USE REFERENCE lesson2 IN nessie")


# In[30]:


## DELETE ALL RECORDS WHERE countryOfOriginCode = 'FR'
spark.sql("DELETE FROM nessie.worker_coops WHERE zip = '10024' ")


# In[31]:


## QUERY THE COUNT OF ENTRIES
spark.sql("SELECT Count(*) as Total FROM nessie.worker_coops").show()


# In[32]:


## SWITCH BACK TO MAIN BRANCH
spark.sql("USE REFERENCE main IN nessie")


# In[33]:


## QUERY THE COUNT OF ENTRIES
spark.sql("SELECT Count(*) as Total FROM nessie.worker_coops").show()


# In[34]:


## MERGE THE CHANGES
spark.sql("MERGE BRANCH lesson2 INTO main IN nessie")


# In[35]:


## QUERY THE COUNT OF ENTRIES
spark.sql("SELECT Count(*) as Total FROM nessie.worker_coops").show()


# In[ ]:





# In[ ]:




