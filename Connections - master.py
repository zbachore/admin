# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks - Mounting & Integrations
# MAGIC ##### Developer: `Zewdu Belachew`
# MAGIC ##### Developed Date: `September 4, 2020`
# MAGIC ##### Updated Date: `December 11, 2020`

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Mount Azure Data Lake Storage Gen2 filesystem using Azure Key Vault and Databricks Scope

# COMMAND ----------

# Collect the necessary values and store them in variables

storageAccountName = dbutils.secrets.get(scope="adminScope", key="data-lake-storage-name")
storageAccountKey = dbutils.secrets.get(scope="adminScope", key="storageAccountKey")
containerName = dbutils.secrets.get(scope="adminScope", key="data-lake-container-name")
applicationId = dbutils.secrets.get(scope="adminScope", key="applicationId")
secret = dbutils.secrets.get(scope="adminScope", key="databricks-secret")
tenantId = dbutils.secrets.get(scope="adminScope", key="tenantId")

source = "abfss://" + containerName + "@" + storageAccountName + ".dfs.core.windows.net/" + "dlake"
mountPoint = "/mnt/dlake"


# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": applicationId,
           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="adminScope",key="databricks-secret"),
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/" + tenantId + "/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://" + containerName + "@zbelachewdatalakegen2.dfs.core.windows.net/",
  mount_point = "/mnt/dlake",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mount Azure Data Lake Gen2 by Directly using the Keys

# COMMAND ----------

# Use this only to mount ADLS from Databricks Community Edition:

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "800bd065-c552-430d-aee2-16c32f8d3139",
"fs.azure.account.oauth2.client.secret": "PxZ06X~ENAkq.c7jZ-I5Jzlk~aNln6_5HS",
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/b46d5e38-aef3-415e-b011-140c9b0dcd86/oauth2/token"}

dbutils.fs.mount(
  source = "abfss://dlake@zbelachewdatalakegen2.dfs.core.windows.net/",
  mount_point = "/mnt/dlake",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/dlake")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Mount Azure Data Lake Gen2 using Credential Passthrough

# COMMAND ----------

# This only works for premium cluster
# The identity should not have already been used to mount using service principal
configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class":   spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

dbutils.fs.mount(
  source = "abfss://training@columbiadatalakegen2.dfs.core.windows.net/",
  mount_point = "/mnt/training",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading data directly from data lake using account key:

# COMMAND ----------

storage_account_name = "zbelachewdatalakegen2 "
storage_account_access_key = "yYHoK5+ObBrP3zyRY41NVvkak4fp2YFMH0TYfevqCTs1p3Ps+R0Tx4oSOZh87DRhcyMRfPjMqP3pK6CSBFceyA=="

file_location = "abfs://zbelachewdatalakegen2 @zbelachewblobstore.blob.core.windows.net/process.csv"
file_type = "csv"

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

# Read data into dataframe:
df = spark.read.format(file_type).option("header", "true").load(file_location)
display(df)

# COMMAND ----------

#Uncomment this below line to unmount when needed
# dbutils.fs.unmount("/mnt/blobfiles")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mount Blob Storage

# COMMAND ----------

# Mount Blob Storage Account
dbutils.fs.mount(
source = "wasbs://databricks@zst.blob.core.windows.net",
mount_point = "/mnt/blobfiles",
extra_configs = {"fs.azure.account.key.zst.blob.core.windows.net":dbutils.secrets.get(scope = "zstscope", key = "zstkey")})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mount Data Lake Storage

# COMMAND ----------

# To use the mount point in another running cluster, you must run dbutils.fs.refreshMounts() 
dbutils.fs.refreshMounts() dbutils.fs.refreshMounts() 

# COMMAND ----------


configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": "800bd065-c552-430d-aee2-16c32f8d3139",
       "fs.azure.account.oauth2.client.secret": "9.nE3c5wFF_XsUC1MT-zj.7uz8gVf64C4e",
       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/b46d5e38-aef3-415e-b011-140c9b0dcd86/oauth2/token",
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}

dbutils.fs.mount(
source = "abfss://training@zbelachewdatalakegen2.dfs.core.windows.net/", # optionally you can add directory name
mount_point = "/mnt/dlake",
extra_configs = configs)

# COMMAND ----------

dbutils.fs.unmount("/mnt/dlake")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Connect to Azure SQL Server Database - Scala

# COMMAND ----------

# MAGIC %scala
# MAGIC // This uses secrets saved in Azure KeyVault and accessed through Secret Scope
# MAGIC val jdbcUsername = dbutils.secrets.get(scope = "adminScope", key = "sqlusername")
# MAGIC val jdbcPassword = dbutils.secrets.get(scope = "adminScope", key = "sqlpassword")
# MAGIC // You can also hard code the username and password.
# MAGIC
# MAGIC Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
# MAGIC
# MAGIC val jdbcHostname = "zbelachewsqlserver.database.windows.net"
# MAGIC val jdbcPort = 1433
# MAGIC val jdbcDatabase = "AdventureWorksLT"
# MAGIC
# MAGIC // Create the JDBC URL without passing in the user and password parameters.
# MAGIC val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"
# MAGIC
# MAGIC // Create a Properties() object to hold the parameters.
# MAGIC import java.util.Properties
# MAGIC val connectionProperties = new Properties()
# MAGIC
# MAGIC connectionProperties.put("user", s"${jdbcUsername}")
# MAGIC connectionProperties.put("password", s"${jdbcPassword}")
# MAGIC
# MAGIC val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# MAGIC connectionProperties.setProperty("Driver", driverClass)
# MAGIC
# MAGIC val CustomerDF = spark.read.jdbc(jdbcUrl, "SalesLT.Customer", connectionProperties)
# MAGIC // CustomerDF.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Connect to Azure SQL Database - Python

# COMMAND ----------

# jdbcUsername = dbutils.secrets.get(scope = "adminScope", key = "sqlusername")
# jdbcPassword = dbutils.secrets.get(scope = "adminScope", key = "sqlpassword")
#  You can also hard code the username and password.

jdbcUsername = "zbelachew"
jdbcPassword = "B@chorei969i969"

jdbcHostname = "zbelachewsqlserver.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "AdventureWorksLT"

# Create the JDBC URL without passing in the user and password parameters.
jdbcUrl = "jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"

jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
connectionProperties = {
  "user" : jdbcUsername,
  "password" : jdbcPassword,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# You can query the table directly from SQL Server
Customer = """
(SELECT CustomerID, FirstName, MiddleName, LastName, EmailAddress FROM SalesLT.Customer) Customer
"""
customerDF = spark.read.jdbc(url=jdbcUrl, table=Customer, properties=connectionProperties)
customerDF.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Connect to Cosmos DB - Python

# COMMAND ----------

# Read Configuration
CosmosDBPrimaryKey = dbutils.secrets.get(scope="adminScope", key="CosmosDBPrimaryKey")
readConfig = {
    "Endpoint": "https://zbelachewcosmosdb.documents.azure.com:443/",
    "Masterkey": CosmosDBPrimaryKey,
    "Database": "RetailDemo",
    "Collection": "WebsiteData",
    "query_custom": "SELECT c.CartID, c.Price, c.UserName, c.Country, c.Year FROM c WHERE c.Country = 'Ethiopia'"
}

# Connect via azure-cosmosdb-spark to create Spark DataFrame
flights = spark.read.format(
    "com.microsoft.azure.cosmosdb.spark").options(**readConfig).load()
display(flights)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Writing to Cosmos DB

# COMMAND ----------

# Write configuration
writeConfig = {
    "Endpoint": "https://zbelachewcosmosdb.documents.azure.com:443/",
    "Masterkey": CosmosDBPrimaryKey,
    "Database": "RetailDemo",
    "Collection": "WebsiteData",
    "Upsert": "true"
}

# update username to zbelachew and write the result back to Cosmos DB
from pyspark.sql.functions import lit
newFlightsDF = flights.withColumn("UserName", lit("zbelachew"))

newFlightsDF.write.mode("overwrite").format("com.microsoft.azure.cosmosdb.spark").options(
    **writeConfig).save()

# COMMAND ----------

df = (spark
     .read
     .option("header", "true")
     .option("inferSchema", "true")
     .json("/mnt/training/WideWorldImporters/PopulationData/States/AK/AK_2021-01-05T122102.json"))

# COMMAND ----------


