# Analisis de datos de Amazon usando Apache Spark (PySpark)

Realizar un flujo ETL usando herramientas como Airbyte para extraer los datos, almacenarlo en un capa de stagging en  Google Cloud Storage y Bigquery
para las transformaciones y acciones basadas en la API de Spark en Dataproc que van desde simples hasta bastante complejas.
Se mantienen don fuentes de datos:

1-Estática: Base de datos en Cockroach con las siguiente tablas: clients, tasas_cambio_pais_anual,compras, external_products_products

1.1 Estatica: Bucket de GCS llamado _zophia-proyecto-final-de_ tabla de historico de compras.

2-Dinámica: dataset *amazon_daily_updates*  en Bigquery que diariamente recibe nuevos registros de las compras realizadas en el día, que se agregan a la tabla compras.

<p align="center">
  Flujo completo:
  <br><br>
  <img src="https://user-images.githubusercontent.com/46491988/156366696-fd5d55fe-b0bc-4bd3-bf88-a34948206742.jpg">
</p>




## Contenido
Una breve sinopsis de lo que es cada caso de uso y qué funcionalidad de SPARK SQL se uso.

| Sección                                                                             |        Funciones |
|:------------------------------------------------------------------------------------|:--------------------|
|[1. Revisando el Data Set Cockroach](#1-Revisando-el-Data-Set-Cockroach)||
|[2. Extracción de la data de Cockroach a una capa de staging GCS](#2-Extracción-de-la-data-de-Cockroach-a-una-capa-de-staging-Google-Cloud-Storage)||
|[3. Extracción de la data GCS a una capa de staging BigQuery](#3-Extracción-de-la-data-GCS-a-una-capa-de-staging-BigQuery)||
|[4. Transformación y limpieza de la data](#4-Transformación-y-limpieza-de-la-data)||
|[4.1 Creando tabla de productos](#41-Creando-tabla-de-productos)|REGEXP_EXTRACT, REGEXP_REPLACE, WHEN, TRANSLATE, COL, GROUP BY, AGG, ORDERBY, CONCAT, LAST, INNER JOIN|
|[4.2 Creando tabla promedio de precio de productos](#42-Creando-tabla-promedio-de-precio-de-productos)|COUNTDISTINCT, MEAN, GROUP BY, AGG, SORT|
|[4.3 Creando tabla rango de precios de productos](#43-Creando-tabla-rango-de-precios-de-productos)|GROUP BY, AGG, FIRST, LAST, MIN, MAX|
|[4.4 Creando tabla promedio de evaluación](#44-Creando-tabla-promedio-de-evaluación)|COUNTDISTINCT, AVG, GROUP BY, AGG|
|[4.5 Creando tabla de compras](#45-Creando-tabla-de-compras)|TODATE, COL|
|[4.6 Creando tabla de compras anuales](#46-Creando-tabla-de-compras-anuales)|COUNTDISTINCT, MONTH, COUNT, SUM, AVG, WINDOWS, LAG|
|[4.7 Creando tabla de compras mensuales](#47-Creando-tabla-de-compras-anuales)|YEAR, COUNTDISTINCT, COUNT, SUM, AVG, COL, INNER JOIN|
|[5. Tabla de hechos](#5-Tabla-de-hechos)|YEAR, SUM, COUNTDISTINCT, GROUPBY, AGG, SORT, COL , INNER JOIN|
|[6. Información de cargas incrementales desde BigQuery](#6-Información-de-cargas-incrementales-desde-BigQuery)||
|[6.1 Carga de datos de compras diarias](#61-Carga-de-datos-de-compras-diarias)|FILTER, CAST|
|[6.2 Calculo de compras anuales](#62-Calculo-de-compras-anuales)|FILTER, DATE_TRUNC, ADD_MONTHS, COL, MONTH, YEAR, GROUPBY, AGG,COUNT, COUNTDISTINCT, SUM, AVG, INNER JOIN, SORT|
|[6.3 Calculo de compras mensuales](#63-Calculo-de-compras-mensuales)|FILTER, DATE_TRUNC, ADD_MONTHS, COL, MONTH, YEAR, GROUPBY, AGG, COUNT, COUNTDISTINCT, SUM, AVG, INNER JOIN|


## 1. Revisando el Data Set Cockroach
Tablas de data set de Amazon:

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

<p align="center">
 Conexión fuente-destino
  <br><br>
  <img src="https://user-images.githubusercontent.com/46491988/156367928-4a077108-d5de-4fea-aa54-fad637135770.jpg">
</p>

<p align="center">
Archivos CSV en GCS
  <br><br>
  <img src="https://user-images.githubusercontent.com/46491988/156092574-5c849fca-c90d-4a4c-b593-e75510d547c0.jpg">
</p>



[Back to Top](#Contenido)

## 3. Extracción de la data GCS a una capa de staging BigQuery

Se realiza la extracción de los archivos CSV bucket amazon_magdielgutierrez y zophia-proyecto-final-de a dataset en becade_mgutierrez en Bigquery
  
Actividades:

  1. Crear un dataset nuevo en BigQuery
  2. Crear tablas de con el prefijo stg_{table}

_Actividades realizadas desde la consola de Google Platform_

Captura de pantalla: resultado de nuevo dataset en Bigquery

![02](https://user-images.githubusercontent.com/46491988/156097097-b5dbe461-658c-4ae1-968a-1aafc1d1cb93.jpg)


[Back to Top](#Contenido)

## 4. Transformación y limpieza de la data

Retos de la data a tranformar
- Formato de fecha TIMESTAMP a DATE
- Datos tipo BOLEANN a STRING
- Datos tipo LONG a INT
- Uso de expresiones regulares para extraer evaluacion de productos dato tipo STRING a DOUBLE
- Uso de expresiones regulares para eliminar carácteres [coma, puntos, simbolos] de precio de productos dato tipo STRING a DOUBLE
- Renombrar columnas y nombre de datos en diferentes tablas para cumplir los estándares del _naming convention_


### 4.1 Creando tabla de productos

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

Ya tenemos una dataframe de productos y otra de tasa de cambios , hacemos un join.

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
                           
 #cast type                          
df_full_products = df_full_products.withColumn("product_price",df_full_products.product_price.cast(DoubleType()))  \
                                    .withColumn("product_rate",df_full_products.product_rate.cast(DoubleType())) \
                                    .withColumn("product_is_bestseller",df_full_products.product_is_bestseller.cast(StringType())) \
                                    .withColumn("product_is_prime",df_full_products.product_is_prime.cast(StringType())) 
```
Mostramos el resultado y esquema de dataframe:

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

Guardamos nuestro dataframe en Bigquery

```PySpark
df_full_products.write \
  .format("bigquery") \
  .option("table","becade_mgutierrez.pr_products_standard_price") \
  .option("temporaryGcsBucket", "amazon_magdielgutierrez") \
  .mode('overwrite') \
  .save()

```

[Back to Top](#Contenido)

### 4.2 Creando tabla promedio de precio de productos
Creamos la tabla pr_products_avg_price , que consite en conocer en cuantos paises se ha vendido un producto y su precio promedio.

```PySpark
#calculated avg product_price_us && count product_country
df_avg_price=pr_products_price.select('product_id','product_price_us','product_country') \
        .groupBy('product_id') \
        .agg(mean('product_price_us').alias('product_avg_price_us'),countDistinct('product_country').alias('country_count')).sort('country_count', ascending=False)
```

Resultado:
```PySpark
#Show row products   
df_avg_price.show(3)
+----------+--------------------+-------------+
|product_id|product_avg_price_us|country_count|
+----------+--------------------+-------------+
|B00MNV8E0C|   24.73453126271878|            7|
|B007B9NV8Q|  20.634105088517593|            7|
|B00X4SCCFG|  17.748030361843725|            6|
+----------+--------------------+-------------+
```
Guardamos el dataframe en Bigquery
```PySpark
df_avg_price.write \
  .format("bigquery") \
  .option("table","becade_mgutierrez.pr_products_avg_price") \
  .option("temporaryGcsBucket", "amazon_magdielgutierrez") \
  .mode('overwrite') \
  .save()
```
### 4.3 Creando tabla rango de precios de productos
Creamos la tabla pr_products_range_price , que consite en conocer en cuantos paises se ha vendido un producto y su precio promedio, asi como tambien su precio
maximo , minimo y el pais.

```PySpark
#operations min_price && max_price
df_price_range=pr_products_price.select('product_id','product_country','product_price_us') \
        .groupBy('product_id') \
        .agg(min('product_price_us').alias('product_min_price'),first('product_country').alias('country_mix_price'), \
            max('product_price_us').alias('product_max_price'),last('product_country').alias('country_max_price'))  
            
#InnerJoin de dataframe df_avg_price && df_price_range
df_full_ranges = df_avg_price.alias('A').join(df_price_range.alias('B'), col('A.product_id') == col('B.product_id'), "inner") 

```

Verficamos que los datos sean correctos, buscando un producto en especifico:

```PySpark
#List product test
pr_products_price.filter(pr_products_price.product_id == "B00MNV8E0C").show(truncate=False)
+----------+---------------------+----------------+----------------------+------------+-------------+---------------+------------------+
|product_id|product_is_bestseller|product_is_prime|product_price_currency|product_rate|product_price|product_country|product_price_us  |
+----------+---------------------+----------------+----------------------+------------+-------------+---------------+------------------+
|B00MNV8E0C|true                 |false           |                      |4.3         |1146.0       |JP             |10.732891667660974|
|B00MNV8E0C|true                 |false           |€                     |4.6         |76.76        |DE             |87.67501307815138 |
|B00MNV8E0C|true                 |true            |₹                     |4.6         |1179.0       |IN             |15.909874809931647|
|B00MNV8E0C|true                 |false           |€                     |4.6         |13.79        |IT             |15.750891484467267|
|B00MNV8E0C|false                |true            |€                     |4.6         |11.69        |NL             |13.352278568050933|
|B00MNV8E0C|false                |false           |£                     |4.7         |11.49        |GB             |14.73076923076923 |
|B00MNV8E0C|true                 |true            |$                     |4.7         |14.99        |US             |14.99             |
+----------+---------------------+----------------+----------------------+------------+-------------+---------------+------------------+

#TEST PRODUCT
df_full_ranges.filter(df_full_ranges.product_id == "B00MNV8E0C").show(truncate=False)
+----------+-------------+--------------------+-----------------+-----------------+------------------+-----------------+
|product_id|country_count|product_avg_price_us|product_max_price|country_max_price|product_min_price |country_mix_price|
+----------+-------------+--------------------+-----------------+-----------------+------------------+-----------------+
|B00MNV8E0C|7            |24.73453126271878   |87.67501307815138|US               |10.732891667660974|JP               |
+----------+-------------+--------------------+-----------------+-----------------+------------------+-----------------+
```

Resultado:
```PySpark
#Show row mergedf_merge_rows
df_full_ranges.show(2)

#Show schema
df_full_ranges.printSchema()
+----------+-------------+--------------------+-----------------+-----------------+-----------------+-----------------+
|product_id|country_count|product_avg_price_us|product_max_price|country_max_price|product_min_price|country_mix_price|
+----------+-------------+--------------------+-----------------+-----------------+-----------------+-----------------+
|B09S5G7BXW|            1|                 0.0|              0.0|               US|              0.0|               US|
|B09S2RQ19K|            1|               99.99|            99.99|               US|            99.99|               US|
+----------+-------------+--------------------+-----------------+-----------------+-----------------+-----------------+
root
 |-- product_id: string (nullable = true)
 |-- country_count: long (nullable = false)
 |-- product_avg_price_us: double (nullable = true)
 |-- product_max_price: double (nullable = true)
 |-- country_max_price: string (nullable = true)
 |-- product_min_price: double (nullable = true)
 |-- country_mix_price: string (nullable = true)
```

Guardamos el dataframe en Bigquery
```PySpark
df_full_ranges.write \
  .format("bigquery") \
  .option("table","becade_mgutierrez.pr_products_range_price") \
  .option("temporaryGcsBucket", "amazon_magdielgutierrez") \
  .mode('overwrite') \
  .save()
```
[Back to Top](#Contenido)

### 4.4 Creando tabla promedio de evaluación

Creando la tabla  que consiste en obtener el promedio de evalucacion de cada producto..

```PySpark
#operations avg evaluate_rate products
df_product_rate=pr_products_price.select('product_id','product_country','product_rate') \
        .groupBy('product_id') \
        .agg(avg('product_rate').alias('product_avg_rate'),countDistinct('product_country').alias('country_count')) 
```
Resultado:
```PySpark
df_product_rate.show(2)
+----------+----------------+-------------+
|product_id|product_avg_rate|country_count|
+----------+----------------+-------------+
|B08J3QQ11H|             4.6|            1|
|9804370085|             4.8|            1|
+----------+----------------+-------------+
```
Guardando dataframe en Bigquery
```PySpark
df_product_rate.write \
  .format("bigquery") \
  .option("table","becade_mgutierrez.pr_product_avg_rate") \
  .option("temporaryGcsBucket", "amazon_magdielgutierrez") \
  .mode('overwrite') \
  .save()
```

[Back to Top](#Contenido)

### 4.5 Creando tabla de clientes



```PySpark
```

```PySpark
```

```PySpark
```

[Back to Top](#Contenido)
### 4.6 Creando tabla de compras

La tabla pr_compras  es el resultado de la tablas stg_compras y stg_historico_compras

Cargamos los datos al dataframe
```PySpark
#name table compras
table_compras = "becade_mgutierrez.stg_compras" # or stg_historico_compras

#load table
raw_compras = spark.read \
  .format("bigquery") \
  .option("table", table_compras) \
  .load()
  
#selct columns  
df_raw_compras = raw_compras.select('fecha_compra','client_id','precio','product_id','cantidad','isprime')

#cast
df_raw_compras = df_raw_compras.withColumn("datetime", to_date("fecha_compra")) \
                               .withColumn("cantidad",df_raw_compras.cantidad.cast(IntegerType())) \
                                .withColumn("isprime",df_raw_compras.isprime.cast(StringType()))
#drop columns                               
df_raw_compras = df_raw_compras.drop('fecha_compra')

#rename columns
df_raw_compras = df_raw_compras.withColumnRenamed('datetime','purchase_date') \
                                .withColumnRenamed('cantidad','product_quantity') \
                                .withColumnRenamed('precio','product_price') \
                                .withColumnRenamed('isprime','client_is_prime')
```

Resultado:
```PySpark
df_raw_compras.show(2)
+-----------------+-------------+----------+----------------+-------------+
|        client_id|product_price|product_id|product_quantity|purchase_date|
+-----------------+-------------+----------+----------------+-------------+
|209-696678-32-117|       236.99|B00N69D6AS|               1|   2018-04-18|
|209-696678-32-117|       236.99|B00N69D6AS|               1|   2018-05-05|
+-----------------+-------------+----------+----------------+-------------+
```

Guardando datos en Bigquery:
```PySpark
df_raw_compras.write \
  .format("bigquery") \
  .option("table","becade_mgutierrez.pr_compras") \
  .option("temporaryGcsBucket", "amazon_magdielgutierrez") \
  .mode('overwrite') \
  .save()
```

[Back to Top](#Contenido)

### 4.7 Creando tabla de compras anuales


```PySpark

#separando fecha en mes y año
df_new_sales= raw_compras.withColumn('month_sales',month(raw_compras.purchase_date)) \
                .withColumn('year_sales',year(raw_compras.purchase_date))
                
#obtneiendo la cantidad de clientes diferentes que comoraron al año
#group by year_sales | countDistinct client_id
df_ordenes_year = df_new_sales.select('year_sales','purchase_date','month_sales','client_id') \
        .groupBy('year_sales','purchase_date','client_id') \
        .agg(countDistinct('client_id').alias('total_compras')) \
        .sort(['year_sales', 'purchase_date'], ascending=True)

#obteniendo el total de ordenes por año
#sum ordenes by client_id | groupby year_sales | count total_compras
sum_ordenes_year = df_ordenes_year.select('year_sales','total_compras') \
        .groupBy('year_sales') \
        .agg(count('total_compras').alias('total_compras')) \
        .sort('year_sales', ascending=True)
   
#obteniendo el promedio de venta por año y el total de venta por año
#groupBy year_sales | sum product_price | avg product_price
df_sales = df_new_sales.select('year_sales','month_sales','product_price','client_id') \
        .groupBy('year_sales') \
        .agg(sum('product_price').alias('venta_total_year'), \
             avg('product_price').alias('avg_venta_mensual')) \
         .sort('year_sales', ascending=True)
         
#InnerJoin df_sales && sum_ordenes_year
full_table_year = df_sales.alias('A').join(sum_ordenes_year.alias('B'), col('A.year_sales') == col('B.year_sales'), "inner") 
```
Resultado
```PySpark
full_table_year.show()
+----------+--------------------+------------------+-------------+
|year_sales|    venta_total_year| avg_venta_mensual|total_compras|
+----------+--------------------+------------------+-------------+
|      2010|1.8495069750010703E7|104.32923661415366|        32717|
|      2011|1.8595521430009063E7|104.14100184255835|        32746|
|      2012| 1.891395686000886E7|104.80852955197692|        32899|
|      2013| 1.855743942000939E7|103.84979669274121|        32733|
|      2014|1.8657688250007752E7| 105.1688391664802|        32713|
|      2015| 1.872384307000746E7|104.68142492945773|        32957|
|      2016| 1.867335246000713E7|104.62258287908166|        32854|
|      2017| 1.872034121000714E7|104.20509554746833|        32753|
|      2018|1.8488971880010866E7|104.16204820233499|        32724|
|      2019|1.8643064240007747E7|104.42012243827817|        32755|
|      2020|1.8892210430008642E7|104.74258421676042|        32913|
+----------+--------------------+------------------+-------------+
```

Guardando dataframe en Bigquery

```PySpark
full_table_year.write \
  .format("bigquery") \
  .option("table","becade_mgutierrez.pr_compras_anuales") \
  .option("temporaryGcsBucket", "amazon_magdielgutierrez") \
  .mode('overwrite') \
  .save()
```

[Back to Top](#Contenido)

### 4.8 Creando tabla de compras mensuales




```PySpark
###compras por Mes
df_ordenes_month = df_new_sales.select('year_sales','purchase_date','month_sales','client_id') \
        .groupBy('year_sales','month_sales','purchase_date','client_id') \
        .agg(countDistinct('client_id').alias('total_compras')) \
        .sort(['year_sales', 'purchase_date'], ascending=True)
 
 sum_ordenes_month = df_ordenes_month.select('year_sales','month_sales','total_compras') \
        .groupBy('year_sales','month_sales') \
        .agg(count('total_compras').alias('total_compras_mes')) \
        .sort(['year_sales','month_sales'], ascending=True)
        
df_month = df_new_sales.select('year_sales','month_sales','product_price') \
        .groupBy('year_sales','month_sales') \
        .agg(sum('product_price').alias('venta_total_mes')) \
        .sort(['year_sales','month_sales'], ascending=True)
 
 df_month_raw = df_month.withColumn('venta_total_mes_anterior',lag(df_month['venta_total_mes']).over(Window.orderBy("month_sales","year_sales")))
 
 df_month_raw= df_month_raw.na.fill(value=0,subset=["venta_total_mes_anterior"])
 
 full_table_month = df_month_raw.alias('A').join(sum_ordenes_month.alias('B'), \
                (col('A.month_sales') == col('B.month_sales')) & (col('A.year_sales') == col('B.year_sales')) , "inner") 
```

```PySpark
full_table_month.show(13)
+----------+-----------+------------------+------------------------+-----------------+
|year_sales|month_sales|   venta_total_mes|venta_total_mes_anterior|total_compras_mes|
+----------+-----------+------------------+------------------------+-----------------+
|      2010|          1|1241494.2399999024|                     0.0|             2618|
|      2011|          1|1220903.2599999004|      1241494.2399999024|             2586|
|      2012|          1|1207546.9799999103|      1220903.2599999004|             2577|
|      2013|          1|1229377.7699999062|      1207546.9799999103|             2569|
|      2014|          1|1288639.0199998915|      1229377.7699999062|             2602|
|      2015|          1|1270299.0199999036|      1288639.0199998915|             2630|
|      2016|          1|1345551.9299998868|      1270299.0199999036|             2619|
|      2017|          1|1145680.2099999334|      1345551.9299998868|             2529|
|      2018|          1|1198162.2299999034|      1145680.2099999334|             2555|
|      2019|          1|1221042.8499999032|      1198162.2299999034|             2605|
|      2020|          1|1253193.3999999021|      1221042.8499999032|             2610|
|      2010|          2|1230857.7399999062|      1253193.3999999021|             2406|
|      2011|          2|1168152.5199999078|      1230857.7399999062|             2401|
+----------+-----------+------------------+------------------------+-----------------+
```

```PySpark
full_table_month.write \
  .format("bigquery") \
  .option("table","becade_mgutierrez.pr_compras_mensuales") \
  .option("temporaryGcsBucket", "amazon_magdielgutierrez") \
  .mode('overwrite') \
  .save()
```
[Back to Top](#Contenido)

## 5 Tabla de hechos

Desempeño de ventas por producto a nivel anual, es decir la cantidad total obtenida de la venta de ese producto por año..

● La cantidad de usuarios que compraron este producto en ese año.

● La cantidad de unidades que se vendieron de ese producto durante ese año.

● El evaluation_rating actual del producto


```PySpark
#separamos la fecha en años
df_fact_raw= raw_compras.withColumn('purchase_year',year(raw_compras.purchase_date)) 

#groupby by purchase_year and product_id | sum product_price | countDistinct client_id | sum product_quantity
df_fact_sales = df_fact_raw.select('*') \
                .groupBy('purchase_year','product_id') \
                .agg(sum('product_price').alias('purchase_sales'),
                     countDistinct('client_id').alias('client_quantity'), 
                     sum('product_quantity').alias('product_quantity_sales') ) \
                .sort(['purchase_year', 'product_id'], ascending=False)
                
#InnerJoin df_fact_sales &&  raw_rate
full_table_fact = df_fact_sales.alias('A').join(raw_rate.alias('B'), \
                (col('A.product_id') == col('B.product_id')) , "inner") 
                 
```
Resultado:

```PySpark
full_table_fact.show(13)
+-------------+----------+------------------+---------------+----------------------+----------------+
|purchase_year|product_id|    purchase_sales|client_quantity|product_quantity_sales|product_avg_rate|
+-------------+----------+------------------+---------------+----------------------+----------------+
|         2020|B09FM7CKG1| 56771.82000000001|            100|                  1959|             3.6|
|         2020|B09FCXXGT5|112020.12000000007|            100|                  3749|             4.8|
|         2020|B09BNK4592|111496.56000000007|            100|                  1862|             4.7|
|         2020|B098RKWHHZ| 695430.1299999999|            100|                  1987|             4.8|
|         2020|B098P1M628| 551117.1599999984|            100|                 11484|             3.9|
|         2020|B097Y38X79| 537275.8800000008|            100|                 11532|             3.9|
|         2020|B0975P2RBR| 79900.01999999999|            100|                  1998|             3.6|
|         2020|B0931NN4PR|          195552.0|            100|                  2016|             4.8|
|         2020|B0914YGQSH|117304.92000000004|            100|                  1959|             4.6|
|         2020|B08ZS9PQ78| 87760.38000000005|            100|                  9762|             4.6|
|         2020|B08Z11QHBG| 686016.0399999997|            100|                  1966|             4.8|
|         2020|B08XXWHLQF|49484.959999999985|            100|                  1904|             4.1|
|         2020|B08X2K6B1Z| 78747.40000000004|            100|                  1802|             4.8|
+-------------+----------+------------------+---------------+----------------------+----------------+
```


Guardando dataframe en Bigquery
```PySpark
raw_compras_historico.write \
  .format("bigquery") \
  .option("table","becade_mgutierrez.pr_fact_compras") \
  .option("temporaryGcsBucket", "amazon_magdielgutierrez") \
  .mode('append') \
  .save()
```

[Back to Top](#Contenido)


## 6. Información de cargas incrementales desde BigQuery

Desarrolla un proceso que lleve los datos incrementales de manera diaria a la tabla de compras en la capa de staging de BigQuery para tener un respaldo de los datos tal cual
llegan de la fuente, la temporalidad de estos procesos se definen a continuación:

1- Extraer diariamente los datos del día anterior y llevarlos a la capa de staging (stg_compras) de BigQuery.

2- Una vez cada primero del mes podrías realizar la limpieza y transformaciones necesarias para obtener los datos que debes insertar en la tabla de compras_mensuales aplicando esto a todas las compras con fecha del mes anterior.

3- Aplicar una lógica similar a la anterior pero de manera anual en vez de mensual.

La única información que se verá afectada por estos datos incrementales serán los que tengan que ver con la tabla de compras

[Back to Top](#Contenido)

## 6.1 Carga de datos de compras diarias

```PySpark
# # #filter data previus day
raw_previus_day= raw_compras.filter(raw_compras.fecha_compra == date_sub(current_date(),1))

raw_previus_day = raw_previus_day.select('fecha_compra','client_id','precio','product_id','cantidad','isprime')

raw_previus_day = raw_previus_day.withColumn("cantidad",raw_previus_day.cantidad.cast(IntegerType())) \
                                .withColumn("isprime",raw_previus_day.isprime.cast(StringType()))
                                
#rename columns
raw_previus_final = raw_previus_day.withColumnRenamed('fecha_compra','purchase_date') \
                                .withColumnRenamed('cantidad','product_quantity') \
                                .withColumnRenamed('precio','product_price') \
                               .withColumnRenamed('isprime','client_is_prime')  


```
Guardamos dataset
```PySpark
raw_previus_day.write \
  .format("bigquery") \
  .option("table","becade_mgutierrez.pr_compras") \
  .option("temporaryGcsBucket", "amazon_magdielgutierrez") \
  .mode('append') \
  .save()

```
[Back to Top](#Contenido)

## 6.2 Calculo de compras anuales
```PySpark
df_sales_current_year = raw_current_year.filter(  (date_trunc("month", col("purchase_date")) != date_trunc("month", current_date())) &
                                                (date_trunc("year", col("purchase_date")) == date_trunc("year", current_date())))
 
 
df_new_sales= df_sales_current_year.withColumn('month_sales',month(df_sales_current_year.purchase_date)) \
                .withColumn('year_sales',year(df_sales_current_year.purchase_date))

df_ordenes_year = df_new_sales.select('year_sales','purchase_date','month_sales','client_id') \
        .groupBy('year_sales','purchase_date','client_id') \
        .agg(countDistinct('client_id').alias('total_compras')) \
        .sort(['year_sales', 'purchase_date'], ascending=True)
        
 sum_ordenes_year = df_ordenes_year.select('year_sales','total_compras') \
        .groupBy('year_sales') \
        .agg(count('total_compras').alias('total_compras')) \
        .sort('year_sales', ascending=True)       

df_sales = df_new_sales.select('year_sales','month_sales','product_price','client_id') \
        .groupBy('year_sales') \
        .agg(sum('product_price').alias('venta_total_year'), \
             avg('product_price').alias('avg_venta_mensual')) \
         .sort('year_sales', ascending=True)
         
#InnerJoin
full_table_year = df_sales.alias('A').join(sum_ordenes_year.alias('B'), col('A.year_sales') == col('B.year_sales'), "inner") 

```

Guardamos dataframe *****
```PySpark
full_table_year.write \
  .format("bigquery") \
  .option("table","becade_mgutierrez.pr_compras_anuales") \
  .option("temporaryGcsBucket", "amazon_magdielgutierrez") \
  .mode('append') \
  .save()
```
[Back to Top](#Contenido)

## 6.3 Calculo de compras mensuales

```PySpark

```
[Back to Top](#Contenido)




