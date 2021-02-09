# Databricks notebook source
# DBTITLE 1,Libraries
import pandas as pd
from pyspark.sql.functions import col
from pyspark.sql.functions import *
from pyspark.sql import functions as f
from pyspark.sql.functions import udf, struct, col
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType


# COMMAND ----------

# DBTITLE 1,Categories
apdos_female = Counter(words(open('/dbfs/data/jebbit/apdos_female.txt', encoding='latin1').read()))
apdos_male = Counter(words(open('/dbfs/data/jebbit/apdos_male.txt', encoding='latin1').read()))
dish_care = Counter(words(open('/dbfs/data/jebbit/dish_care.txt', encoding='latin1').read()))
fabric_enhancers = Counter(words(open('/dbfs/data/jebbit/fabric_enhancers.txt', encoding='latin1').read()))
fhc_multipurpose = Counter(words(open('/dbfs/data/jebbit/fhc_multipurpose.txt', encoding='latin1').read()))
hair_care_female = Counter(words(open('/dbfs/data/jebbit/hair_care_female.txt', encoding='latin1').read()))
hair_care_male = Counter(words(open('/dbfs/data/jebbit/hair_care_male.txt', encoding='latin1').read()))
laundry = Counter(words(open('/dbfs/data/jebbit/laundry.txt', encoding='latin1').read()))
oral_care_toothbrush = Counter(words(open('/dbfs/data/jebbit/oral_care_toothbrush.txt', encoding='latin1').read()))
oral_care_toothpaste = Counter(words(open('/dbfs/data/jebbit/oral_care_toothpaste.txt', encoding='latin1').read()))

# COMMAND ----------

# DBTITLE 1,ML
def replace_brands_name():

# COMMAND ----------

# DBTITLE 1,Replace strings using udf
def replace_string(value):
  brands = ['lady speed stick','speed stick','herbal essences', 'palmolive optims', 'head & shoulders', 'tio nacho', 'maestro limpio', 'magia blanca', 'blanca nieves', 'brilla king', 'top terra', 'arm & hammer', 'mon bijou', 'baby soft', 'home care', 'old spice', 'dr fresh', 'h & s', 'head & shoulder']
  new_brands = ['ladyspeedstick','speedstick','herbalessences', 'palmoliveoptims', 'head&shoulders', 'tionacho', 'maestrolimpio', 'magiablanca', 'blancanieves', 'brillaking', 'topterra', 'arm&hammer', 'monbijou', 'babysoft', 'homecare', 'oldspice', 'drfresh', 'head&shoulder', 'head & shoulders']
  
  for i in range(len(brands)): 
    if brands[i] in value:
        value = value.replace(brands[i], new_brands[i])

  return value

# COMMAND ----------

# DBTITLE 1,Split data
def split_data(data, variable): 
  replace_string_udf = udf(replace_string, StringType())
  new_data = data.withColumn(variable, replace_string_udf(variable))
  
  split_brands = new_data.withColumn(variable, f.regexp_replace(f.col(variable),' ', ','))
    
  split_brands = split_brands.select(
        "jebbit_id",
        f.split(variable, ",").alias(variable),
        f.posexplode(f.split(variable, ",")).alias("pos", "val")
    )\
  .drop("val")\
    .select(
        "jebbit_id",
        f.concat(f.lit(variable),f.col("pos").cast("string")).alias("name"),
        f.expr(variable + "[pos]").alias("val"))
  
  return split_brands
  

# COMMAND ----------

# DBTITLE 1,Unaided awareness
def unaided_awareness(data):
  selected_feat = [s for s in data.columns if s.startswith('unaided') or s.startswith('jebbit_id') or s.startswith('top_of_mind')]
  ua = data.select(*selected_feat)
  
  
  spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

  schema = StructType([
  StructField('jebbit_id', StringType(), True),
  StructField('name', StringType(), True),
  StructField('val', StringType(), True)
  ])
  
  result = spark.createDataFrame(spark.sparkContext.emptyRDD(),schema)
  
  for ele in selected_feat:
    ua = ua.withColumn(ele, lower(col(ele)))
    if ele != 'jebbit_id':
      new_process = split_data(ua, ele)
      result = result.union(new_process) 
   
  return result
  

# COMMAND ----------

# DBTITLE 1,Data Validation/Filter
def filter_null(data):
  return data.filter(col("jebbit_id").isNotNull())


def validation(data):
  data = data.withColumnRenamed("pg_-_category","pg_category") \
  .withColumnRenamed("pg_-_country","pg_country") \
  .withColumnRenamed("pg_-_experience_type","pg_experience_type") 
  columns = ['experience_date', 'campaign_id', 'user_session_id', 'jebbit_id', 'brand', 'jugar_start', 'source', 'outcome', 'url_params', 'pg_category', 'pg_experience_type', 'pg_country', 'country_code']
  
  for col in columns:
    if not col in data.columns:
      print ('Error')
          
  return data.select(*columns)
  

# COMMAND ----------

# DBTITLE 1,Main
def main():
  path_example = '/dbfs/data/jebbit/raw/20210201.json'
  data = pd.read_json(path_example)
  data = spark.createDataFrame(data)
  
  data = filter_null(data)
  data_fct = validation(data)
  
  data = [['11', 'rexona palmolive optims', 'dove', 'pantene h&s']]
  data = pd.DataFrame(data, columns=['jebbit_id', 'top_of_mind', 'unaided_awareness_2', 'unaided_awareness_3'])
  data = spark.createDataFrame(data)
  ua = unaided_awareness(data)
  
  ua = replace_brands_name(ua)
  
  
  
  ua.show(50)

  
if __name__ == '__main__':
  main()