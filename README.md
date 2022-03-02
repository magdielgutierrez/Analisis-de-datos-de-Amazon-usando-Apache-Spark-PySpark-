# Analisis de datos de Amazon usando Apache Spark (PySpark)

Realizar un flujo ETL usando herramientas como Airbyte para extraer los datos, almacenarlo en un capa de stagging en  Google Cloud Storage y Bigquery
para las transformaciones y acciones basadas en la API de Spark en Dataproc que van desde simples hasta bastante complejas.
Se mantienen don fuentes de datos:

1-Estática: Base de datos en Cockroach con las siguiente tablas: clients, tasas_cambio_pais_anual,compras, external_products_products

1.1 Estatica: Bucket de GCS llamado _zophia-proyecto-final-de_ tabla de historico de compras.

2-Dinámica: dataset *amazon_daily_updates*  en Bigquery que diariamente recibe nuevos registros de las compras realizadas en el día, que se agregan a la tabla compras.

## Contenido
Una breve sinopsis de lo que es cada caso de uso y qué funcionalidad de SPARK SQL se uso.

| Sección                                                                             |        Funciones |
|:------------------------------------------------------------------------------------|:--------------------|
|[1.Revisando el Data Set Cockroach](#1-Revisando-el-Data-Set-Cockroach)||
|[2.Extracción de la data de Cockroach a una capa de staging GCS](#2-Extracción-de-la-data-de-Cockroach-a-una-capa-de-staging-Google-Cloud-Storage)||
|[3.Extracción de la data GCS a una capa de staging BigQuery](#3-Extracción-de-la-data-GCS-a-una-capa-de-staging-BigQuery)||
|[4.Transformación y limpieza de la data](#4-Transformación-y-limpieza-de-la-data)||
|[4.1Creando la sesión de Spark](#41-Creando-la-sesión-de-Spark)||
|[4.2.Creando tabla de productos]|REGEXP_EXTRACT, REGEXP_REPLACE, TRANSLATE, COL, CONCAT, LAST, INNER JOIN|
|[4.3.Creando tabla de productos]|REGEXP_EXTRACT, REGEXP_REPLACE, TRANSLATE, COL, CONCAT, LAST, INNER JOIN|
|[4.4 Creando tabla pr_products_avg_price](#4.4Creando_tabla_pr_products_avg_price)|COUNTDISTINCT, MEAN|
|[4.5 Creando tabla pr_products_price_ranges](#4.5 Creando tabla pr_products_price_ranges)|FIRST, LAST, MIN,MAX|
|[4.6 Creando tabla pr_product_rate_avg](#4.6 Creando tabla pr_product_rate_avg)|xxxxx|
|[4.8 Creando tabla pr_compras](#4.8 Creando tabla pr_compras)|dddd|
|[4.9 Creando tabla pr_compras_mensuales](#4.9 Creando tabla pr_compras_mensuales)|dddd|
|[4.10 Creando tabla pr_compras_anuales](#4.10 Creando tabla pr_compras_anuales)|MONTH, YEAR, COL, SORT, COUNTDISTINCT, COUNT, AVG, SUM, INNER JOIN|

## 1. Revisando el Data Set Cockroach
Tablas de data set:

![image](https://user-images.githubusercontent.com/46491988/156089505-20df6bd0-9f42-4568-b1d8-c49a48b0c12d.png)

Descripcion de tablas:
```bash
>SHOW COLUMNS FROM clients

column_name   |data_type   |is_nullable|column_default|generation_expression|indices  |is_hidden|
--------------+------------+-----------+--------------+---------------------+---------+---------+
id            |VARCHAR(20) |true       |              |                     |{primary}|false    |
nombre        |VARCHAR(150)|true       |              |                     |{primary}|false    |
direccion     |VARCHAR(200)|true       |              |                     |{primary}|false    |
email         |VARCHAR(100)|true       |              |                     |{primary}|false    |
telefono      |VARCHAR(13) |true       |              |                     |{primary}|false    |
numero_tarjeta|VARCHAR(20) |true       |              |                     |{primary}|false    |
isprime       |VARCHAR     |true       |              |                     |{primary}|false    |
rowid         |INT8        |false      |unique_rowid()|                     |{primary}|true     |

>SHOW COLUMNS FROM compras

column_name |data_type|is_nullable|column_default|generation_expression|indices  |is_hidden|
------------+---------+-----------+--------------+---------------------+---------+---------+
id          |STRING   |true       |              |                     |{primary}|false    |
client_id   |STRING   |true       |              |                     |{primary}|false    |
product_id  |STRING   |true       |              |                     |{primary}|false    |
cantidad    |INT8     |true       |              |                     |{primary}|false    |
precio      |FLOAT8   |true       |              |                     |{primary}|false    |
envio_id    |STRING   |true       |              |                     |{primary}|false    |
isprime     |STRING   |true       |              |                     |{primary}|false    |
fecha_compra|DATE     |true       |              |                     |{primary}|false    |
metodo_pago |STRING   |true       |              |                     |{primary}|false    |
rowid       |INT8     |false      |unique_rowid()|                     |{primary}|true     |

>SHOW COLUMNS FROM products

column_name            |data_type   |is_nullable|column_default|generation_expression|indices  |is_hidden|
-----------------------+------------+-----------+--------------+---------------------+---------+---------+
isbestseller           |VARCHAR     |true       |              |                     |{primary}|false    |
product_title          |VARCHAR(500)|true       |              |                     |{primary}|false    |
product_main_image_url |VARCHAR(100)|true       |              |                     |{primary}|false    |
app_sale_price         |VARCHAR(10) |true       |              |                     |{primary}|false    |
app_sale_price_currency|VARCHAR(5)  |true       |              |                     |{primary}|false    |
isprime                |VARCHAR     |true       |              |                     |{primary}|false    |
product_detail_url     |VARCHAR(50) |true       |              |                     |{primary}|false    |
product_id             |VARCHAR(20) |true       |              |                     |{primary}|false    |
evaluate_rate          |VARCHAR(50) |true       |              |                     |{primary}|false    |
original_price         |VARCHAR(15) |true       |              |                     |{primary}|false    |
country                |VARCHAR(2)  |true       |              |                     |{primary}|false    |
rowid                  |INT8        |false      |unique_rowid()|                     |{primary}|true     |

>SHOW COLUMNS FROM external_products

column_name            |data_type   |is_nullable|column_default|generation_expression|indices  |is_hidden|
-----------------------+------------+-----------+--------------+---------------------+---------+---------+
isbestseller           |VARCHAR     |true       |              |                     |{primary}|false    |
product_title          |VARCHAR(500)|true       |              |                     |{primary}|false    |
product_main_image_url |VARCHAR(250)|true       |              |                     |{primary}|false    |
app_sale_price         |VARCHAR(15) |true       |              |                     |{primary}|false    |
app_sale_price_currency|VARCHAR(3)  |true       |              |                     |{primary}|false    |
isprime                |VARCHAR     |true       |              |                     |{primary}|false    |
product_detail_url     |VARCHAR(250)|true       |              |                     |{primary}|false    |
product_id             |VARCHAR(15) |true       |              |                     |{primary}|false    |
evaluate_rate          |VARCHAR(50) |true       |              |                     |{primary}|false    |
original_price         |VARCHAR(15) |true       |              |                     |{primary}|false    |
country                |VARCHAR(2)  |true       |              |                     |{primary}|false    |
rowid                  |INT8        |false      |unique_rowid()|                     |{primary}|true     |


>SHOW COLUMNS FROM tasas_cambio_pais_anual

column_name |data_type  |is_nullable|column_default|generation_expression|indices  |is_hidden|
------------+-----------+-----------+--------------+---------------------+---------+---------+
Country-name|VARCHAR(50)|true       |              |                     |{primary}|false    |
Alpha-2-code|VARCHAR(2) |true       |              |                     |{primary}|false    |
Alpha-3-code|VARCHAR(3) |true       |              |                     |{primary}|false    |
currency    |VARCHAR(5) |true       |              |                     |{primary}|false    |
Year        |INT8       |true       |              |                     |{primary}|false    |
value       |FLOAT4     |true       |              |                     |{primary}|false    |
rowid       |INT8       |false      |unique_rowid()|                     |{primary}|true     |

```
[Back to Top](#Contenido)

## 2. Extracción de la data de Cockroach a una capa de staging Google Cloud Storage

Actividades:

  1. Crear una fuente en Airbyte. 
  
  2. Crear un destino en Airbyte con el conector sea de tipo Google Cloud Storage (GCS), con output format  CSV .
  
  3. Crear una conexión entre la fuente y el destino para extraer las tablas.


**Flujo de Trabajo**

![image](https://user-images.githubusercontent.com/46491988/156093345-8ce8ad19-7391-4448-b2d7-fe5b66100a54.png)

**Captura de pantalla: conexión fuente-destino**

![airbyte_connection](https://user-images.githubusercontent.com/46491988/156090789-23b854d0-b727-4397-934e-c6cada09d220.jpg)

**Captura de pantalla: archivos CSV en GCS**

![gcs_data](https://user-images.githubusercontent.com/46491988/156092574-5c849fca-c90d-4a4c-b593-e75510d547c0.jpg)

[Back to Top](#Contenido)

## 3. Extracción de la data GCS a una capa de staging BigQuery

**Flujo de trabajo**

![01](https://user-images.githubusercontent.com/46491988/156093553-73fa4846-33e4-407e-86ef-fcf56fd7d6d7.jpg)

Se realiza la extracción de los archivos CSV bucket amazon_magdielgutierrez y zophia-proyecto-final-de a dataset en becade_mgutierrez en Bigquery
  
Actividades:

  1. Crear un dataset nuevo en BigQuery
  2. Crear tablas de con el prefijo stg_{table}

_Actividades ralziadas desde la consola de Google Platform_


Captura de pantalla: resultado de nuevo dataset en Bigquery

![02](https://user-images.githubusercontent.com/46491988/156097097-b5dbe461-658c-4ae1-968a-1aafc1d1cb93.jpg)


[Back to Top](#Contenido)

## 4. Transformación y limpieza de la data

Retos de la data a tranformar
- Formato de fecha TIMESTAMP a DATE
- Datos tipo BOLEANN a STRING
- Datos tipo LONG a INT
- Uso de expresiones regulares para extraer evaluacion de productos dato tipo STRING a DOUBLE
- Uso de expresiones regulares para eliminar caracteres [coma, puntos, simbolos] de precio de productos dato tipo STRING a DOUBLE
- Renombrar columnas y nombre de datos en diferentes tablas para cumplir los estándares del _naming convention_


### 4.1 Creando la sesión de Spark
```PySpark
#Start session Spark
from pyspark.sql import SparkSession
from pyspark import SparkContext
spark = SparkSession.builder \
  .appName('dataset_amazon') \
  .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar') \
  .getOrCreate()

spark.conf.set("spark.sql.repl.eagerEval.enabled",True)
```

### 4.2 Creando tabla de productos

La tabla pr_products es creada a partir de stg_products y stg_external_productos

Cargando datos a dataframe:
```PySpark
#name table products
table_products = "becade_mgutierrez.stg_products" # or becade_mgutierrez.stg_external_productos

#load table to dataframe
stg_products = spark.read \
  .format("bigquery") \
  .option("table", table_products) \
  .load()
   
#select columns from table
raw_products = stg_products.select('product_id','country','app_sale_price','evaluate_rate','isbestseller','isprime','app_sale_price_currency')

#clean column app_sale_price drop values 'None'
raw_products = raw_products.where(raw_products.app_sale_price != 'None')

# fill empty rows evaluate_rate
df_raw_products= raw_products.withColumn("evaluate_rate", when(col("evaluate_rate")=="" ,None)  \
                               .otherwise(col("evaluate_rate"))) 
                               
#clean column app_sale_price drop values 'None'
df_raw_products = df_raw_products.where(df_raw_products.evaluate_rate != "None")

#drop duplicates rows products
df_raw_products = df_raw_products.dropDuplicates()


#clean column evaluate_rate extract format {n.n} &&  replace characters {,} by {.}
df_clean_rate = df_raw_products \
                .withColumn('clean_rate', regexp_extract(col('evaluate_rate'), r'([0-9][\.\,][0-9])',1)) \
                .withColumn('clean_rate', translate(col('clean_rate'), ',', '.'))
                
#concat columns  number_price + decimal_price = app_sale_price_us
df_clean_products_raw=df_raw_price.select('product_id','country','isbestseller','isprime','app_sale_price_currency','clean_rate',
                                          concat(df_raw_price.number_price,df_raw_price.decimal_price).alias("app_sale_price"))
                                          
```

Mostrando los resultados de tranformación:

ANTES:
```PySpark

```

DESPUES: 
```PySpark
df_clean_products_raw.show(5)
+----------+-------+------------+-------+-----------------------+----------+--------------+
|product_id|country|isbestseller|isprime|app_sale_price_currency|clean_rate|app_sale_price|
+----------+-------+------------+-------+-----------------------+----------+--------------+
|B07FXP7HVS|     IT|        true|  false|                      €|       4.1|         18.19|
|B077T5RQF7|     IT|        true|   true|                      €|       4.4|         50.48|
|B074VMTP68|     DE|        true|   true|                      €|       4.4|         29.99|
|B00QHC01C2|     NL|       false|   true|                      €|       4.5|         29.72|
|B01GFJWHZ0|     NL|        true|   true|                      €|       4.5|         21.43|
+----------+-------+------------+-------+-----------------------+----------+--------------+
```

Los precios actuales  [app_sale_price] estan en la moneda local de cada pais, necesitos convertirlo en dolar, para ello tenemos la tabla pr_tasas_cambio_pais_anual

Cargando datos a dataframe:
```PySpark
#name table exchange
table_exchange = "becade_mgutierrez.stg_tasas_cambio_pais_anual"

#load table
stg_exchange = spark.read \
  .format("bigquery") \
  .option("table", table_exchange) \
  .load()
  
#select columns from table
raw_exchange = stg_exchange.select('Alpha_2_code','Alpha_3_code','Country_name','Year','currency','value')

#rename columns
raw_exchange = raw_exchange.withColumnRenamed('Alpha_2_code','country_code') \
                           .withColumnRenamed('Alpha_3_code','country_code_iso') \
                           .withColumnRenamed('Country_name','country_name') \
                           .withColumnRenamed('Year','year_rate') \
                           .withColumnRenamed('currency','currency_name') \
                           .withColumnRenamed('value','value_rate')
                           
#group by and select last value_rate 
df_group_rate = raw_exchange.select('country_code','year_rate','value_rate') \
        .groupBy('country_code',) \
        .agg(max('year_rate').alias('max_year'),last('value_rate').alias('value_exchange')) \
        .orderBy('country_code',asceding=False)

```

Mostramos la tasa de cambia actual para cada pais:

```PySpark
#Show row exchange
df_group_rate.show(5)
+------------+--------+--------------+
|country_code|max_year|value_exchange|
+------------+--------+--------------+
|          AR|    2020|      70.53917|
|          AT|    2020|      0.875506|
|          AU|    2020|      1.453085|
|          BE|    2020|      0.875506|
|          BG|    2020|      1.716333|
+------------+--------+--------------+
```

Ya tenemos una dataframe de productos y otra de tasa de cambias , hacemos un join.


```PySpark
#join dataframe df_clean_products_raw && df_exchange_group
df_merge_rows = df_group_rate.alias('rate') \
                .join(df_clean_products_raw.alias('price'), col('price.country') == col('rate.country_code'), "inner")
                          
#equivalente en dólares del precio de cada uno de los productos
df_raw_products=df_merge_rows.withColumn('app_sale_price_us', col('app_sale_price')/col('value_exchange'))

#rename columns
df_full_products = df_raw_products.withColumnRenamed('isprime','product_is_prime') \
                           .withColumnRenamed('app_sale_price_currency','product_price_currency') \
                           .withColumnRenamed('isbestseller','product_is_bestseller') \
                           .withColumnRenamed('clean_rate','product_rate') \
                           .withColumnRenamed('app_sale_price','product_price') \
                           .withColumnRenamed('country_code','product_country') \
                           .withColumnRenamed('app_sale_price_us','product_price_us')
```
Mostramos el resultado:


```PySpark
#Show row exchange
df_full_products.show(2)
+----------+---------------------+----------------+----------------------+------------+-------------+---------------+------------------+
|product_id|product_is_bestseller|product_is_prime|product_price_currency|product_rate|product_price|product_country|  product_price_us|
+----------+---------------------+----------------+----------------------+------------+-------------+---------------+------------------+
|B07FXP7HVS|                 true|           false|                     €|         4.1|        18.19|             IT|20.776556642672926|
|B077T5RQF7|                 true|            true|                     €|         4.4|        50.48|             IT| 57.65808572414124|
|B074VMTP68|                 true|            true|                     €|         4.4|        29.99|             DE| 34.25447683967899|
|B00QHC01C2|                false|            true|                     €|         4.5|        29.72|             NL|33.946083750425466|
|B01GFJWHZ0|                 true|            true|                     €|         4.5|        21.43|             NL|24.477273713715267|
+----------+---------------------+----------------+----------------------+------------+-------------+---------------+------------------+
#Display Schema
df_full_products.printSchema()
root
 |-- product_id: string (nullable = true)
 |-- product_is_bestseller: string (nullable = true)
 |-- product_is_prime: string (nullable = true)
 |-- product_price_currency: string (nullable = true)
 |-- product_rate: double (nullable = true)
 |-- product_price: double (nullable = true)
 |-- product_country: string (nullable = true)
 |-- product_price_us: double (nullable = true)
```

Finalmente guardamos nuestro dataframe en Bigquery


```PySpark
df_full_products.write \
  .format("bigquery") \
  .option("table","becade_mgutierrez.pr_products_standard_price") \
  .option("temporaryGcsBucket", "amazon_magdielgutierrez") \
  .mode('overwrite') \
  .save()

```

### 4.4 Creando tabla pr_products_avg_price
### 4.5 Creando tabla pr_products_price_ranges
### 4.6 Creando tabla pr_product_rate_avg
### 4.7 Creando tabla pr_clients
### 4.8 Creando tabla pr_compras
### 4.9 Creando tabla pr_compras_mensuales
### 4.10 Creando tabla pr_compras_anuales

## 5 Tabla de hechos

Desempeño de ventas por producto a nivel anual, es decir la cantidad total obtenida de la venta de ese producto por año..

● La cantidad de usuarios que compraron este producto en ese año.

● La cantidad de unidades que se vendieron de ese producto durante ese año.

● El evaluation_rating actual del producto


## 6 Agregar información de cargas incrementales





