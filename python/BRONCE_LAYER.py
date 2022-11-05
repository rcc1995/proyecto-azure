# Databricks notebook source

##DECLARAMOS LAS LIBRERIAS
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, BooleanType,DateType
from pyspark.sql.functions import *

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
bronze_path = '/mnt/rodrigoc/dbfs/delta/bronze'
silver_path = '/mnt/rodrigoc/dbfs/delta/silver'
gold_path = '/mnt/rodrigoc/dbfs/delta/gold'

# COMMAND ----------

# MAGIC %md ##PARA CATEGORIES

# COMMAND ----------

##DEFINIMOS LA RUTA DE CATEGORIES
CATEGORIES_DATA =  '/mnt/rodrigoc/dbfs/source/categories/'

##DEFINIMOS EL SCHEMA DE CATEGORIES
categories_schema = StructType([
    StructField('category_id',            IntegerType(), nullable=True),
    StructField('category_department_id', IntegerType(), nullable=True),
    StructField('category_name',          StringType(), nullable=True)])

##LEEMOS LA FUENTE DE CATEGORIES 
categoriesDF = spark.read.csv(CATEGORIES_DATA + "/categories.csv",schema=categories_schema)

##ESCRIBIMOS EL DELTA DE CATEGORIES EN LA CAPA BRONCE
categoriesDF.write.mode("overwrite").format("delta").save(f"{bronze_path}/categories")

# COMMAND ----------

# MAGIC %md ##PARA CUSTOMER

# COMMAND ----------

##DEFINIMOS LA RUTA DE CATEGORIES
CUSTOMER_DATA='/mnt/rodrigoc/dbfs/source/customer/'

##DEFINIMOS EL SCHEMA DE CATEGORIES
customer_schema=StructType([
    StructField('customer_id',IntegerType(),nullable=True),
    StructField('customer_fname',StringType(),nullable=True),
    StructField('customer_lname',StringType(),nullable=True),
    StructField('custome_email',StringType(),nullable=True),
    StructField('customer_password',StringType(),nullable=True),
    StructField('customer_street',StringType(),nullable=True),
    StructField('customer_city',StringType(),nullable=True),
    StructField('customer_state',StringType(),nullable=True),
    StructField('customer_zipcode',StringType(),nullable=True)    
])

##LEEMOS LA FUENTE DE CATEGORIES 
customerDF=spark.read.csv(CUSTOMER_DATA+'/customer.csv',schema=customer_schema,sep='|')

##ESCRIBIMOS EL DELTA DE CATEGORIES EN LA CAPA BRONCE
customerDF.write.mode("overwrite").format("delta").save(f"{bronze_path}/customer")

# COMMAND ----------

# MAGIC %md ##PARA DEPARTMENTS

# COMMAND ----------

##DEFINIMOS LA RUTA DE CATEGORIES
DEPARTMENTS_DATA='/mnt/rodrigoc/dbfs/source/departments/'

##DEFINIMOS EL SCHEMA DE CATEGORIES
departments_schema=StructType([
    StructField('department_id',IntegerType(),nullable=True),
    StructField('department_name',StringType(),nullable=True)  
])

##LEEMOS LA FUENTE DE CATEGORIES 
departmentsDF=spark.read.csv(DEPARTMENTS_DATA+'/departments.csv',schema=departments_schema,sep='|')

##ESCRIBIMOS EL DELTA DE CATEGORIES EN LA CAPA BRONCE
departmentsDF.write.mode("overwrite").format("delta").save(f"{bronze_path}/departments")



# COMMAND ----------

# MAGIC %md ##PARA ORDER_ITEMS

# COMMAND ----------

##DEFINIMOS LA RUTA DE CATEGORIES
ORDER_ITEMS_DATA='/mnt/rodrigoc/dbfs/source/order_items/'

##DEFINIMOS EL SCHEMA DE CATEGORIES
order_items_schema=StructType([
    StructField('order_item_id',IntegerType(),nullable=True),
    StructField('order_item_order_id',IntegerType(),nullable=True),
    StructField('order_item_product_id',IntegerType(),nullable=True),
    StructField('order_item_quantity',IntegerType(),nullable=True),
    StructField('order_item_subtotal',DoubleType(),nullable=True),
    StructField('order_item_product_price',DoubleType(),nullable=True)
])

##LEEMOS LA FUENTE DE CATEGORIES 
orderItemsDF=spark.read.csv(ORDER_ITEMS_DATA+'/order_items.csv',schema=order_items_schema,sep='|')

##ESCRIBIMOS EL DELTA DE CATEGORIES EN LA CAPA BRONCE
orderItemsDF.write.mode("overwrite").format("delta").save(f"{bronze_path}/order_items")

# COMMAND ----------

# MAGIC %md ##PARA ORDERS

# COMMAND ----------

##DEFINIMOS LA RUTA DE CATEGORIES
ORDERS_DATA='/mnt/rodrigoc/dbfs/source/orders/'

##DEFINIMOS EL SCHEMA DE CATEGORIES
orders_schema=StructType([
    StructField('order_id',IntegerType(),nullable=True),
    StructField('order_date',DateType(),nullable=True),
    StructField('order_customer_id',IntegerType(),nullable=True),
    StructField('order_status',StringType(),nullable=True)
])

##LEEMOS LA FUENTE DE CATEGORIES 
orderItemsDF=spark.read.csv(ORDERS_DATA+'/orders.csv',schema=orders_schema,sep='|')

##ESCRIBIMOS EL DELTA DE CATEGORIES EN LA CAPA BRONCE
orderItemsDF.write.mode("overwrite").format("delta").save(f"{bronze_path}/orders")


# COMMAND ----------

# MAGIC %md ##PARA PRODUCTS

# COMMAND ----------

##DEFINIMOS LA RUTA DE CATEGORIES
PRODUCTS_DATA='/mnt/rodrigoc/dbfs/source/products/'

##DEFINIMOS EL SCHEMA DE CATEGORIES
products_schema=StructType([
    StructField('product_id',IntegerType(),nullable=True),
    StructField('product_category_id',IntegerType(),nullable=True),
    StructField('product_name',StringType(),nullable=True),
    StructField('product_description',StringType(),nullable=True),
    StructField('product_price',DoubleType(),nullable=True),
    StructField('product_image',StringType(),nullable=True)
])

##LEEMOS LA FUENTE DE CATEGORIES 
productsItemsDF=spark.read.csv(PRODUCTS_DATA+'/products.csv',schema=products_schema,sep='|')

##ESCRIBIMOS EL DELTA DE CATEGORIES EN LA CAPA BRONCE
productsItemsDF.write.mode("overwrite").format("delta").save(f"{bronze_path}/products")

