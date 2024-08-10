# Databricks notebook source
dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.unmount("/mnt/cloudpandith")

# COMMAND ----------

# MAGIC %md
# MAGIC Mounting ADLS Gen2 Stroage

# COMMAND ----------

 configs = {"fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": "2c68472f-db74-48dc-a191-bcb12c8ed01d",
            "fs.azure.account.oauth2.client.secret": "PgF8Q~iFgpy.FqRjuPVm8uz~WWFQtWtrg~3Yndrt",
            "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/97cab7c0-265d-483d-8770-f3d4c7c96e5e/oauth2/token"}

 # Optionally, you can add <directory-name> to the source URI of your mount point.
 dbutils.fs.mount(
   source = "abfss://mnt@dataqualitytrg.dfs.core.windows.net/",
   mount_point = "/mnt/global",
   extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/global/gloabl/")

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.widgets.text("config_filepath","")
dbutils.widgets.text("processed_date","")

# COMMAND ----------

config_filepath=dbutils.widgets.get("config_filepath")
processed_date=dbutils.widgets.get("processed_date")

# COMMAND ----------

print(config_filepath)
print(processed_date)

# COMMAND ----------

tablename=config_filepath.split("/")[-1].split(".")[0]

# COMMAND ----------


tablename

# COMMAND ----------

df = spark.read.format("csv").load("/mnt/gloabl/india/landing/orders/2024/08/10/orders.csv")

# COMMAND ----------

df=spark.read.format("json").option("multiLine",True).load(config_filepath)
parameters=df.rdd.map(lambda x:x.asDict()).first()
print(parameters)


# COMMAND ----------

import json
source_filelocation=parameters['sourcefile']
target_filelocation=parameters['targetfile']
pending_filelocation=parameters['pendingfile']
audit_filelocation=parameters['auditfile']
duplicatecheck=parameters['duplicate_check']
selectcols=parameters['required_cols']
nullcheck=parameters['null_check']
dateformatcheck=parameters['dateformatchecks']
castcols=parameters['cols_datatype'][0].asDict()
negativecheck=parameters['no_negative_value']

# COMMAND ----------

source_filelocation

# COMMAND ----------

def append_date(url,pdate):
  return url+pdate

# COMMAND ----------

source_path=append_date(source_filelocation,processed_date)
target_path=append_date(target_filelocation,processed_date)
pending_path=append_date(pending_filelocation,processed_date)
audit_path=append_date(audit_filelocation,processed_date)

# COMMAND ----------

#Reading source files
df_csv=spark.read.format("csv").option("header",True).load(source_path)
df_csv.show()
sourcecount=df_csv.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Duplicate Check

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col,row_number
cols=df_csv.columns
windowspec=Window.partitionBy(*cols).orderBy(cols[0])
df_csv1=df_csv.select('*',row_number().over(windowspec).alias("rn"))
df_csv1.show()

# COMMAND ----------

pending_records_list=[]

# COMMAND ----------

#Find duplicates
from pyspark.sql.functions import lit
df_duplicates=df_csv1.filter(col("rn")>1).drop("rn").withColumn('reject_reason',lit('Duplicate record'))
df_duplicates.show()
pending_records_list.append(df_duplicates)

# COMMAND ----------

df_unique=df_csv1.filter(col("rn")==1).drop("rn")
df_unique.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Eleminating Nulls

# COMMAND ----------

#null check on columns
if len(nullcheck)!=0:
  for column in nullcheck:
    df_null=df_unique.filter(col(column).isNull()).withColumn('reject_reason',lit('Null Record'))
    pending_records_list.append(df_null)

# COMMAND ----------

# MAGIC %md
# MAGIC Negative Value Check

# COMMAND ----------

#negativecheck check on columns
if len(negativecheck)!=0:
  for column in negativecheck:
    df_negative=df_unique.filter(col(column)<0).withColumn('reject_reason',lit('Negative Value'))
    pending_records_list.append(df_negative)

# COMMAND ----------

#negativecheck check on columns
from pyspark.sql.functions import date_format
if len(dateformatcheck)!=0:
  for column in dateformatcheck:
    df_datefmt=df_unique.select("*",date_format(column,'yyyy-MM-dd').alias("orderdate1"))\
              .filter(col("orderdate1").isNull()).drop('orderdate1')\
              .withColumn('reject_reason',lit('Incorrect Dateformat'))
    pending_records_list.append(df_datefmt)



  

# COMMAND ----------

pending_records_list

# COMMAND ----------

from functools import reduce
from pyspark.sql import DataFrame
df_reject=reduce(DataFrame.union,pending_records_list)
reject_count=df_reject.count()

# COMMAND ----------

df_reject.show()

# COMMAND ----------

df_final=df_csv.exceptAll(df_reject.drop('reject_reason'))

# COMMAND ----------

if len(castcols)>0:
  for k,v in castcols.items():
    df_final=df_final.withColumn(k,col(k).cast(v))

# COMMAND ----------

df_final.show()

# COMMAND ----------

writtencount=df_final.count()

# COMMAND ----------

df_final=df_final.select(selectcols)

# COMMAND ----------

target_path

# COMMAND ----------

df_final.write.mode("overwrite").format("parquet").save(target_path)

# COMMAND ----------

pending_path

# COMMAND ----------

df_reject.write.mode("overwrite").format("csv").option("header",True).save(pending_path)

# COMMAND ----------

print(tablename)
print(sourcecount)
print(reject_count)
print(writtencount)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
df_audit=spark.createDataFrame([(tablename,sourcecount,reject_count,writtencount)],['tablename','sourcecount','rejectcount','writtencount'])\
                                .withColumn('loadtimestamp',current_timestamp())
df_audit.show()

# COMMAND ----------

audit_path


# COMMAND ----------

df_audit.write.mode("overwrite").format("csv").option("header",True).save(audit_path)

# COMMAND ----------


