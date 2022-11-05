# Databricks notebook source
##DECLARAMOS LAS LIBRERIAS
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,FloatType, DoubleType, BooleanType,TimestampType
import pyspark.sql.functions as F
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
silver_path = '/mnt/rodrigoc/dbfs/delta/silver'

##DEFINIMOS LAS CREDENCIALES DE BASE DE DATOS 
username = dbutils.secrets.get(scope="scope_rcc", key="databaseuser")
password = dbutils.secrets.get(scope="scope_rcc", key="databasepasswords")
server_name = dbutils.secrets.get(scope="scope_rcc", key="databaseserver")
database_name = dbutils.secrets.get(scope="scope_rcc", key="databasename")
url = server_name + ";" + "databaseName=" + database_name + ";"

# COMMAND ----------

# MAGIC %md ##PARA CATEGORIES

# COMMAND ----------

##DEFINIMOS LA RUTA BRONZE
bronze_categories='/mnt/rodrigoc/dbfs/delta/bronze/categories'

##LEEMOS LA FUENTE DE CATEGORIES 
categoriesDF=spark.read.format("delta").load(f"{bronze_categories}")

##APLICAMOS LAS REGLAS DE NEGOCIO
silverCategoriesDF=categoriesDF.filter("category_department_id is not null").filter(categoriesDF.category_name.isNotNull()).filter("category_id is not null")

##ESCRIBIMOS EL DELTA  EN LA CAPA SILVER
categoriesDF.write.mode("overwrite").format("delta").save(f"{silver_path}/categories")

##DEFINIMOS EL NOMBRE DE LA TABLA OUTPUT
tabla="silver.categories"

##INSERTAMOS LA DATA EN SQL DATABASE
try:
    silverCategoriesDF.write \
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

# MAGIC %md ##PARA CUSTOMER

# COMMAND ----------

##DEFINIMOS LA RUTA BRONZE
bronze_customer='/mnt/rodrigoc/dbfs/delta/bronze/customer'

##LEEMOS LA FUENTE DE CATEGORIES 
customerDF=spark.read.format("delta").load(f"{bronze_customer}")

##APLICAMOS LAS REGLAS DE NEGOCIO
silverCustomerDF=customerDF.filter("customer_id is not null").filter(customerDF.customer_fname.isNotNull()).filter("custome_email is not null").filter("customer_password is not null")

##ESCRIBIMOS EL DELTA  EN LA CAPA SILVER
silverCustomerDF.write.mode("overwrite").format("delta").save(f"{silver_path}/customer")

##DEFINIMOS EL NOMBRE DE LA TABLA OUTPUT
tabla="silver.customer"

##INSERTAMOS LA DATA EN SQL DATABASE
try:
    silverCustomerDF.write \
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

# MAGIC %md ##PARA DEPARTMENTS

# COMMAND ----------

##DEFINIMOS LA RUTA BRONZE
bronze_departments='/mnt/rodrigoc/dbfs/delta/bronze/departments'

##LEEMOS LA FUENTE DE CATEGORIES 
departmentsDF=spark.read.format("delta").load(f"{bronze_departments}")

##APLICAMOS LAS REGLAS DE NEGOCIO
silverDepartmentsDF=departmentsDF.filter("department_id is not null").filter(departmentsDF.department_name.isNotNull())

##ESCRIBIMOS EL DELTA  EN LA CAPA SILVER
silverDepartmentsDF.write.mode("overwrite").format("delta").save(f"{silver_path}/departments")

##DEFINIMOS EL NOMBRE DE LA TABLA OUTPUT
tabla="silver.departments"

##INSERTAMOS LA DATA EN SQL DATABASE
try:
    silverDepartmentsDF.write \
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

# MAGIC %md ##PARA ORDER_ITEMS

# COMMAND ----------

##DEFINIMOS LA RUTA BRONZE
bronze_order_items='/mnt/rodrigoc/dbfs/delta/bronze/order_items'

##LEEMOS LA FUENTE DE CATEGORIES 
orderItemsDF=spark.read.format("delta").load(f"{bronze_order_items}")

##APLICAMOS LAS REGLAS DE NEGOCIO
silverOrderItemsDF=orderItemsDF.filter("order_item_id is not null").\
                                filter(orderItemsDF.order_item_order_id.isNotNull()).\
                                filter("order_item_product_id is not null").\
                                filter("order_item_quantity >0").\
                                filter("order_item_subtotal >0").\
                                filter("order_item_product_price >0")
##ESCRIBIMOS EL DELTA  EN LA CAPA SILVER
silverOrderItemsDF.write.mode("overwrite").format("delta").save(f"{silver_path}/order_items")

##DEFINIMOS EL NOMBRE DE LA TABLA OUTPUT
tabla="silver.order_items"

##INSERTAMOS LA DATA EN SQL DATABASE
try:
    silverOrderItemsDF.write \
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

# MAGIC %md ##PARA ORDERS

# COMMAND ----------

##DEFINIMOS LA RUTA BRONZE
bronze_orders='/mnt/rodrigoc/dbfs/delta/bronze/orders'

##LEEMOS LA FUENTE DE CATEGORIES 
ordersDF=spark.read.format("delta").load(f"{bronze_orders}")

##APLICAMOS LAS REGLAS DE NEGOCIO
silverOrdersDF=ordersDF.filter("order_id is not null").\
                                filter(ordersDF.order_date.isNotNull()).\
                                filter("order_customer_id is not null")

##ESCRIBIMOS EL DELTA  EN LA CAPA SILVER
silverOrdersDF.write.mode("overwrite").format("delta").save(f"{silver_path}/orders")

##DEFINIMOS EL NOMBRE DE LA TABLA OUTPUT
tabla="silver.orders"

##INSERTAMOS LA DATA EN SQL DATABASE
try:
    silverOrdersDF.write \
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

# MAGIC %md ##PARA PRODUCTS

# COMMAND ----------

##DEFINIMOS LA RUTA BRONZE
bronze_products='/mnt/rodrigoc/dbfs/delta/bronze/products'

##LEEMOS LA FUENTE DE CATEGORIES 
productsDF=spark.read.format("delta").load(f"{bronze_products}")

##APLICAMOS LAS REGLAS DE NEGOCIO
silverProductsDF=productsDF.filter("product_id is not null").\
                                filter(productsDF.product_category_id.isNotNull()).\
                                filter("product_name is not null").\
                                filter("product_price >0")

##ESCRIBIMOS EL DELTA  EN LA CAPA SILVER
silverProductsDF.write.mode("overwrite").format("delta").save(f"{silver_path}/products")

##DEFINIMOS EL NOMBRE DE LA TABLA OUTPUT
tabla="silver.products"

##INSERTAMOS LA DATA EN SQL DATABASE
try:
    silverProductsDF.write \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .mode("overwrite") \
        .option("url", url) \
        .option("dbtable", tabla) \
        .option("user", username) \
        .option("password", password) \
        .save()
except ValueError as error :
    print("Connector write failed", error)
