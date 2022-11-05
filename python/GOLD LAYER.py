# Databricks notebook source
##DECLARAMOS LAS LIBRERIAS
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,FloatType, DoubleType, BooleanType,TimestampType
import pyspark.sql.functions as f
from pyspark.sql.functions import udf, col

# COMMAND ----------

##MONTAMOS EL SISTEMA DE ARCHIVOS

# Unmount directory if previously mounted. Replace lab2 with your container
MOUNTPOINT = "/mnt/rodrigoc/dbfs"
if MOUNTPOINT in [mnt.mountPoint for mnt in dbutils.fs.mounts()]:
    dbutils.fs.unmount(MOUNTPOINT)

# Add the Storage Account, Container, and reference the secret to pass the SAS Token
STORAGE_ACCOUNT = dbutils.secrets.get(scope="scope_rcc", key="storageaccount")
CONTAINER = dbutils.secrets.get(scope="scope_rcc", key="container")
SASTOKEN = dbutils.secrets.get(scope="scope_rcc", key="storagedatalake")

# Do not change these values
SOURCE = "wasbs://{container}@{storage_acct}.blob.core.windows.net/".format(container=CONTAINER, storage_acct=STORAGE_ACCOUNT)
URI = "fs.azure.sas.{container}.{storage_acct}.blob.core.windows.net".format(container=CONTAINER, storage_acct=STORAGE_ACCOUNT)

try:
    dbutils.fs.mount(
    source=SOURCE,
    mount_point=MOUNTPOINT,
    extra_configs={URI:SASTOKEN})
except Exception as e:
    if "Directory already mounted" in str(e):
        pass # Ignore error if already mounted.
    else:
        raise e

# COMMAND ----------

##DEFINIMOS LAS RUTAS DE LAS CAPAS DEL DELTA LAKE
gold_path = '/mnt/rodrigoc/dbfs/delta/gold'

##DEFINIMOS LAS CREDENCIALES DE BASE DE DATOS 
username = dbutils.secrets.get(scope="scope_rcc", key="databaseuser")
password = dbutils.secrets.get(scope="scope_rcc", key="databasepasswords")
server_name = dbutils.secrets.get(scope="scope_rcc", key="databaseserver")
database_name = dbutils.secrets.get(scope="scope_rcc", key="databasename")
url = server_name + ";" + "databaseName=" + database_name + ";"

# COMMAND ----------

# MAGIC %md ##PREGUNTA 1

# COMMAND ----------

##DEFINIMOS LA RUTA silver
silver_orders='/mnt/rodrigoc/dbfs/delta/silver/orders'
silver_customers='/mnt/rodrigoc/dbfs/delta/silver/customer'

##LEEMOS LA FUENTE NECESARIAS
ordersDF=spark.read.format("delta").load(f"{silver_orders}")
customersDF=spark.read.format("delta").load(f"{silver_customers}")

##APLICAMOS LAS REGLAS DE AGRUPAMIENTO
dfPregunta1=ordersDF.alias("O").join(
    customersDF.alias("C"),
    f.col("O.order_customer_id")==f.col("C.customer_id")
).select(
    f.concat_ws(' ',f.col("C.customer_fname"),f.col("C.customer_lname")).alias("full_name"),
    f.col("O.order_id")
).groupBy("full_name").\
count().alias("q_order").\
toDF(*("FULL_NAME", "Q_ORDER")).\
orderBy(f.col("Q_ORDER").desc())


##ESCRIBIMOS EL DELTA  EN LA CAPA GOLD
dfPregunta1.write.mode("overwrite").format("delta").save(f"{gold_path}/pregunta_1")

##DEFINIMOS EL NOMBRE DE LA TABLA OUTPUT
tabla="gold.pregunta_1"

##INSERTAMOS LA DATA EN SQL DATABASE
try:
    dfPregunta1.write \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .mode("overwrite") \
        .option("url", url) \
        .option("dbtable", tabla) \
        .option("user", username) \
        .option("password", password) \
        .save()
except ValueError as error :
    print("Connector write failed", error)


# COMMAND ----------

# MAGIC %md ##PREGUNTA 2

# COMMAND ----------

##DEFINIMOS LA RUTA silver
silver_orders='/mnt/rodrigoc/dbfs/delta/silver/orders'
silver_order_items='/mnt/rodrigoc/dbfs/delta/silver/order_items'
silver_products='/mnt/rodrigoc/dbfs/delta/silver/products'
silver_categories='/mnt/rodrigoc/dbfs/delta/silver/categories'

##LEEMOS LA FUENTE NECESARIAS
ordersDF=spark.read.format("delta").load(f"{silver_orders}")
order_itemsDF=spark.read.format("delta").load(f"{silver_order_items}")
productsDF=spark.read.format("delta").load(f"{silver_products}")
categoriesDF=spark.read.format("delta").load(f"{silver_categories}")

##PASAMOS LOS DATAFRAMES A VISTAS TEMPORALES PARA APLICAR SQL
ordersDF.createOrReplaceTempView("orders")
order_itemsDF.createOrReplaceTempView("order_items")
productsDF.createOrReplaceTempView("products")
categoriesDF.createOrReplaceTempView("categories")

##APLICAMOS LAS REGLAS DE AGRUPAMIENTO
dfPregunta2= spark.sql("""
    select 	
        date_format(b.order_date,'yyyyMM') as PERIOD ,
        upper(d.category_name) as CATEGORY_NAME,
        sum(a.order_item_quantity) as Q_ITEM
    from order_items a 
    inner join orders b on a.order_item_order_id=b.order_id
    inner join products c on a.order_item_product_id=c.product_id
    inner join categories d on c.product_category_id=d.category_id
    group by 
    upper(d.category_name) ,
    date_format(b.order_date,'yyyyMM')
    order by 
    date_format(b.order_date,'yyyyMM') asc
""")


##ESCRIBIMOS EL DELTA  EN LA CAPA GOLD
dfPregunta2.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{gold_path}/pregunta_2")

##DEFINIMOS EL NOMBRE DE LA TABLA OUTPUT
tabla="gold.pregunta_2"

##INSERTAMOS LA DATA EN SQL DATABASE
try:
    dfPregunta2.write \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .mode("overwrite") \
        .option("url", url) \
        .option("dbtable", tabla) \
        .option("user", username) \
        .option("password", password) \
        .save()
except ValueError as error :
    print("Connector write failed", error)


# COMMAND ----------

# MAGIC %md ##PREGUNTA 3

# COMMAND ----------

##DEFINIMOS LA RUTA silver
silver_orders='/mnt/rodrigoc/dbfs/delta/silver/orders'
silver_order_items='/mnt/rodrigoc/dbfs/delta/silver/order_items'
silver_products='/mnt/rodrigoc/dbfs/delta/silver/products'
silver_categories='/mnt/rodrigoc/dbfs/delta/silver/categories'

##LEEMOS LA FUENTE NECESARIAS
ordersDF=spark.read.format("delta").load(f"{silver_orders}")
order_itemsDF=spark.read.format("delta").load(f"{silver_order_items}")
productsDF=spark.read.format("delta").load(f"{silver_products}")
categoriesDF=spark.read.format("delta").load(f"{silver_categories}")

##PASAMOS LOS DATAFRAMES A VISTAS TEMPORALES PARA APLICAR SQL
ordersDF.createOrReplaceTempView("orders")
order_itemsDF.createOrReplaceTempView("order_items")
productsDF.createOrReplaceTempView("products")
categoriesDF.createOrReplaceTempView("categories")

##HALLO LOS MONTOS POR ORDER POR DIA 
dfVentaOrden= spark.sql("""
        select 
        b.order_date,
        date_format(b.order_date,'yyyy') as YEAR,
        date_format(b.order_date,'MM') as MONTH,
        date_format(b.order_date,'dd') as DAY,
        b.order_id AS ORDER_ID,
        round(sum(a.order_item_subtotal),2) as SUBTOTAL
    from order_items a 
    inner join orders b on a.order_item_order_id=b.order_id
    inner join products c on a.order_item_product_id=c.product_id
    inner join categories d on c.product_category_id=d.category_id
    --where b.order_id=4
    group by 
    b.order_date,
    date_format(b.order_date,'yyyy') ,
    date_format(b.order_date,'MM') ,
    date_format(b.order_date,'dd'),
    b.order_id
    order by b.order_date desc
""")

dfVentaOrden.createOrReplaceTempView("venta_orden")

####################################################
#HALLAMOS LOS PROMEDIOS POR DIA
dfPregunta3_1=spark.sql("""select year,month,day,round(avg(subtotal),2) as subtotal_avg from venta_orden group by year,month,day order by year,month,day """)

##ESCRIBIMOS EL DELTA  EN LA CAPA GOLD
dfPregunta3_1.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{gold_path}/pregunta_3_1")

##DEFINIMOS EL NOMBRE DE LA TABLA OUTPUT
tabla="gold.pregunta_3_1"

##INSERTAMOS LA DATA EN SQL DATABASE
try:
    dfPregunta3_1.write \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .mode("overwrite") \
        .option("url", url) \
        .option("dbtable", tabla) \
        .option("user", username) \
        .option("password", password) \
        .save()
except ValueError as error :
    print("Connector write failed", error)
    
####################################################
#HALLAMOS LOS PROMEDIOS POR MES
dfPregunta3_2=spark.sql("""select year AS YEAR,month AS MONTH,round(avg(subtotal),2) as TOTAL_AVG from venta_orden group by year,month order by year,month """)

##ESCRIBIMOS EL DELTA  EN LA CAPA GOLD
dfPregunta3_2.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{gold_path}/pregunta_3_2")

##DEFINIMOS EL NOMBRE DE LA TABLA OUTPUT
tabla="gold.pregunta_3_2"

##INSERTAMOS LA DATA EN SQL DATABASE
try:
    dfPregunta3_2.write \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .mode("overwrite") \
        .option("url", url) \
        .option("dbtable", tabla) \
        .option("user", username) \
        .option("password", password) \
        .save()
except ValueError as error :
    print("Connector write failed", error)

    
    
####################################################
#HALLAMOS LOS PROMEDIOS POR AÑO
dfPregunta3_3=spark.sql("""select year AS YEAR,round(avg(subtotal),2) as  TOTAL_AVG from venta_orden group by year order by year """)

##ESCRIBIMOS EL DELTA  EN LA CAPA GOLD
dfPregunta3_3.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{gold_path}/pregunta_3_3")

##DEFINIMOS EL NOMBRE DE LA TABLA OUTPUT
tabla="gold.pregunta_3_3"

##INSERTAMOS LA DATA EN SQL DATABASE
try:
    dfPregunta3_3.write \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .mode("overwrite") \
        .option("url", url) \
        .option("dbtable", tabla) \
        .option("user", username) \
        .option("password", password) \
        .save()
except ValueError as error :
    print("Connector write failed", error)



# COMMAND ----------

# MAGIC %md ##PREGUNTA 4

# COMMAND ----------

##DEFINIMOS LA RUTA silver
silver_orders='/mnt/rodrigoc/dbfs/delta/silver/orders'
silver_order_items='/mnt/rodrigoc/dbfs/delta/silver/order_items'

##LEEMOS LA FUENTE NECESARIAS
ordersDF=spark.read.format("delta").load(f"{silver_orders}")
orderItemsDF=spark.read.format("delta").load(f"{silver_order_items}")

##HALLAMOS LAS MONTOS TOTALES POR ORDEN
dfTotalOrder=orderItemsDF.select("order_item_subtotal","order_item_order_id").groupBy("order_item_order_id").sum("order_item_subtotal").toDF("ORDER_ID","TOTAL")

##HALLAMOS LAS ORDENES CANCELADAS MAYORES A $1000
dfPregunta4=dfTotalOrder.alias("T").join(
ordersDF.alias("O"),
f.col("T.ORDER_ID")==f.col("O.ORDER_ID")
).select(
    f.col("T.ORDER_ID"),
    round(f.col("T.TOTAL"),2).alias("TOTAL"),
    f.col("O.ORDER_DATE"),
    f.col("O.ORDER_CUSTOMER_ID").alias("CUSTOMER_ID"),
    f.col("O.ORDER_STATUS")    
).filter("ORDER_STATUS=='CANCELED' AND  TOTAL>1000")


##ESCRIBIMOS EL DELTA  EN LA CAPA GOLD
dfPregunta4.write.mode("overwrite").format("delta").save(f"{gold_path}/pregunta_4")

##DEFINIMOS EL NOMBRE DE LA TABLA OUTPUT
tabla="gold.pregunta_4"

##INSERTAMOS LA DATA EN SQL DATABASE
try:
    dfPregunta4.write \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .mode("overwrite") \
        .option("url", url) \
        .option("dbtable", tabla) \
        .option("user", username) \
        .option("password", password) \
        .save()
except ValueError as error :
    print("Connector write failed", error)


# COMMAND ----------

# MAGIC %md ##PREGUNTA 5

# COMMAND ----------

##DEFINIMOS LA RUTA silver
silver_orders='/mnt/rodrigoc/dbfs/delta/silver/orders'
silver_order_items='/mnt/rodrigoc/dbfs/delta/silver/order_items'
silver_products='/mnt/rodrigoc/dbfs/delta/silver/products'
silver_categories='/mnt/rodrigoc/dbfs/delta/silver/categories'

##LEEMOS LA FUENTE NECESARIAS
ordersDF=spark.read.format("delta").load(f"{silver_orders}")
orderItemsDF=spark.read.format("delta").load(f"{silver_order_items}")
productsDF=spark.read.format("delta").load(f"{silver_products}")
categoriesDF=spark.read.format("delta").load(f"{silver_categories}")


##HALLAMOS LAS INGRESOS POR CATEGORIA Y TRIMESTRE
dfPregunta5=orderItemsDF.alias("D").join(
    ordersDF.alias("O"),
    f.col("D.ORDER_ITEM_ORDER_ID")==f.col("O.ORDER_ID")
).join(
    productsDF.alias("P"),
    f.col("D.ORDER_ITEM_PRODUCT_ID")==f.col("P.PRODUCT_ID")    
).join(
    categoriesDF.alias("C"),
    f.col("P.PRODUCT_CATEGORY_ID")==f.col("C.CATEGORY_ID")
).select(
    year(f.col("O.ORDER_DATE")).alias("YEAR"),
    quarter(f.col("O.ORDER_DATE")).alias("TRIMESTER"),
    f.upper(f.col("C.CATEGORY_NAME")).alias("CATEGORY_NAME"),  
    f.col("D.ORDER_ITEM_SUBTOTAL").alias("TOTAL")    
).groupBy("YEAR","TRIMESTER","CATEGORY_NAME").\
agg(round(sum("TOTAL"),2)).\
toDF("YEAR","TRIMESTER","CATEGORY_NAME","TOTAL")


##ESCRIBIMOS EL DELTA  EN LA CAPA GOLD
dfPregunta5.write.mode("overwrite").format("delta").save(f"{gold_path}/pregunta_5")

##DEFINIMOS EL NOMBRE DE LA TABLA OUTPUT
tabla="gold.pregunta_5"

##INSERTAMOS LA DATA EN SQL DATABASE
try:
    dfPregunta5.write \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .mode("overwrite") \
        .option("url", url) \
        .option("dbtable", tabla) \
        .option("user", username) \
        .option("password", password) \
        .save()
except ValueError as error :
    print("Connector write failed", error)

