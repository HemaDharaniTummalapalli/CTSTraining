# Databricks notebook source
# MAGIC %md
# MAGIC checking the list of scopes present.
# MAGIC

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

# MAGIC %md
# MAGIC check the list of secrets present in scope.

# COMMAND ----------

dbutils.secrets.list(scope = 'projectfinalscope')

# COMMAND ----------

# MAGIC %md
# MAGIC ESTABLISH THE CONNECTION BETWEEN SCOPE AND KEY.

# COMMAND ----------

dbutils.secrets.get(scope = 'projectfinalscope', key ='projectfinalsas')

# COMMAND ----------

# MAGIC %md
# MAGIC MOUNTING DATABRICKS WITH DATALAKE

# COMMAND ----------

storageAccountName = "adlsfinal03"
sasToken = dbutils.secrets.get(scope = 'projectfinalscope', key ='projectfinalsas')
blobContainerName = "refined"
mountPoint = "/mnt/data/"
if any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  dbutils.fs.unmount(mountPoint)
try:
  dbutils.fs.mount(
    source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
    mount_point = mountPoint,
    #extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
    extra_configs = {'fs.azure.sas.' + blobContainerName + '.' + storageAccountName + '.blob.core.windows.net': sasToken}
  )
  print("mount succeeded!")
except Exception as e:
  print("mount exception", e)

# COMMAND ----------

# MAGIC %md
# MAGIC CHECKING THE PATH IN DBFS

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/mnt/'))

# COMMAND ----------

# MAGIC %md
# MAGIC CHECKING PATH OF FILE INSIDE THE FOLDER

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/mnt/data'))

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/mnt/data/dharani'))

# COMMAND ----------

# MAGIC %md
# MAGIC CHECKING FILE INSIDE THE SUB-FOLDER

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/mnt/data/dharani/cohort3data'))

# COMMAND ----------

# MAGIC %md
# MAGIC READ THE FILES TO DBFS

# COMMAND ----------

product=spark.read.csv('dbfs:/mnt/data/dharani/cohort3data/Product.csv',header=True)
display(product)

# COMMAND ----------

salesorddet=spark.read.csv('dbfs:/mnt/data/dharani/cohort3data/SalesOrderDetail.csv',header=True)
display(salesorddet)

# COMMAND ----------

# MAGIC %md
# MAGIC ADDING HEADER TO THE FILE WHICH DOESN'T HAVE HEADER FOR IT. 

# COMMAND ----------

filepath="dbfs:/mnt/data/dharani/SalesOrderHeader.csv"
df_saleorder=spark.read.option("sep","\t").csv(filepath)
#display(df_saleorder)
header=["SalesOrderID","RevisionNumber","OrderDate","DueDate","ShipDate","Status","OnlineOrderFlag","SalesOrderNumber","PurchaseOrderNumber","AccountNumber","CustomerID","ShipToAddressID","OnlineOrdercopen","PurchaseOrderNumberid","SalesOrderNumberID","AccountNumberID","BillToAddressID","ShipMethod","CreditCardApprovalCode","SubTotal","TaxAmt","Freight","TotalDue","Comment","rowguid","ModifiedDate"]
for i in range (len(header)):
    df_saleorder=df_saleorder.withColumnRenamed('_c'+str(i),header[i])
 
display(df_saleorder)

# COMMAND ----------

# MAGIC %md
# MAGIC JOIN THE PRODUCT & SALES TABLE.

# COMMAND ----------

join_df=product.join(salesorddet,on="ProductID",how="inner")
display(join_df)

# COMMAND ----------

# MAGIC %md
# MAGIC REMOVING THE NULL VALUES IN SIZE AND COLOR COLUMN

# COMMAND ----------

transformed_df=join_df.filter((join_df["color"]!="NULL")&(join_df["Size"]!="NULL"))
display(transformed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC REPLACING THE VALUES OF SIZE COLUMN 

# COMMAND ----------

from pyspark.sql.functions import regexp_replace,when
transformed_df = transformed_df.withColumn("size",
    when(transformed_df["size"] == "S", "30")
    .when(transformed_df["size"] == "M", "32")
    .when(transformed_df["size"] == "L", "34")
    .when(transformed_df["size"] == "XL", "36")
    .when(transformed_df["size"] == "XXL", "38")
    .otherwise(transformed_df["size"]))
display(transformed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC CALCULATE THE DIFFERENCE BETWEEN STANDARD COST AND LIST PRICE COLUMNS

# COMMAND ----------

from pyspark.sql.functions import col
df=transformed_df.withColumn("cost_diff",col("StandardCost")-col("ListPrice"))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC CHANGE THE NAME COLUMN BY REMOVING THE SIZE IN THE VALUE OF NAME COLUMN

# COMMAND ----------

from pyspark.sql.functions import split, expr, col
split_name = split(transformed_df["Name"], "[,-]")
df= df.withColumn("name_without_size_color", split_name[0])
display(df.select('name_without_size_color',"Name"))

# COMMAND ----------

# MAGIC %md
# MAGIC SELECTING THE NECESSARY COLUMNS

# COMMAND ----------

dff=df.select('ProductID','Name','Color','size','SalesOrderID','OrderQty','UnitPrice','StandardCost','cost_diff')
dff.printSchema()

# COMMAND ----------

display(dff)

# COMMAND ----------

dff.createOrReplaceTempView('finaloutput')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from finaloutput

# COMMAND ----------

spark.sql("drop table if exists final")

# COMMAND ----------

# MAGIC %md
# MAGIC CREATING THE MANAGED TABLE

# COMMAND ----------

dff.write.saveAsTable('final')

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW create Table final

# COMMAND ----------

# MAGIC %md
# MAGIC ESTABLISH THE AUTENCATION TO STORAGE ACCOUNT IN DATALAKE 

# COMMAND ----------

spark.conf.set("fs.azure.account.key.adlsfinal03.dfs.core.windows.net", "PcyPh8QobU3LsI+cAHrQ9Aizh6msiJLoSHqe5fJO7exiTUo4Ju2636wA0sKK3/uVGmW3cLK7Gx+k+AStn5aqaw==")

# COMMAND ----------

spark.sql("drop table if exists EXTTABLE")

# COMMAND ----------

# MAGIC %md
# MAGIC CREATE EXTERNAL TABLE

# COMMAND ----------

spark.sql('''
CREATE TABLE IF NOT EXISTS EXTTABLE (
  ProductID STRING,   Name STRING,   Color STRING,   size STRING,   SalesOrderID STRING,   OrderQty STRING,   UnitPrice STRING,   StandardCost STRING,   cost_diff DOUBLE
)
USING DELTA
LOCATION 'abfss://processed@adlsfinal03.dfs.core.windows.net/new'
''')

# COMMAND ----------

# MAGIC %md
# MAGIC INSERT VALUES INTO EXTERNAL TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into EXTTABLE
# MAGIC select * from final
