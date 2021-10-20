import json
import os
import findspark
import shutil
from query import xmlToOracle
 

# tornar o pyspark "importável"
findspark.add_packages('com.databricks:spark-xml_2.12:0.13.0')

findspark.init()

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import explode
from pyspark.sql.types import *
from pyspark.sql.functions import struct



#Config variables
xml_path = r"C:\GDrive\Adejo\SouzaCruz\xml\\"
#schema_path= r"C:\GDrive\Adejo\SouzaCruz\projeto\config\nfe_schema.json"
schema_path= r"C:\GDrive\Adejo\SouzaCruz\spark_xml_to_oracle\config\nfe_schema.json"

# Oracle Database Connection
jdbc_string = r'jdbc:oracle:thin:@db202110181402_high?TNS_ADMIN=C:/wallet'
driver = 'oracle.jdbc.driver.OracleDriver'
user = 'ADMIN'
password='w8Z4c85_SwJQau'


# Map all files in directory

xmls_list = []
xmls_list_processing = []

for f in os.listdir(xml_path+r"landing\\"):
  if os.path.isfile(os.path.join(xml_path+r"landing\\", f)):
    xmls_list.append(r""+xml_path+r"landing\\"+f)
    xmls_list_processing.append(r""+xml_path+r'processing\\'+f)

print("Files to be processed: "+str(len(xmls_list)))

# Moving files to processing dir

for i in xmls_list:
  shutil.move(i,xml_path+r'processing\\')

# Starting local session and import data do Spark

if len(xmls_list) > 0:

  spark = SparkSession \
            .builder \
            .config('spark.executor.memory', '5g') \
            .config('spark.driver.memory','5g')  \
            .config('spark.submit.deployMode','client') \
            .config('spark.yarn.queue','short') \
            .config('spark.executor.cores',4) \
            .master('local[*]') \
            .appName("ETL_xml_to_SAFX") \
            .getOrCreate()


  spark.sparkContext.setLogLevel("INFO")

  """```
  from pyspark.sql import SparkSession,SQLContext
  sql_jar="/path/to/sql_jar_file/sqljdbc42.jar"
  spark_snow_jar="/usr/.../snowflake/spark-snowflake_2.11-2.5.5-spark_2.3.jar"
  snow_jdbc_jar="/usr/.../snowflake/snowflake-jdbc-3.10.3.jar"
  oracle_jar="/usr/path/to/oracle_jar_file//v12/jdbc/lib/oracle6.jar"
  spark=(SparkSession
  .builder
  .master('yarn')
  .appName('Spark job new_job')
  .config('spark.driver.memory','10g')
  .config('spark.submit.deployMode','client')
  .config('spark.executor.memory','15g')
  .config('spark.executor.cores',4)
  .config('spark.yarn.queue','short')
  .config('spark.jars','{},{},{},{}'.frmat(sql_jar,spark_snow_jar,snow_jdbc_jar,oracle_jar))
  .enableHiveSupport()
  .getOrCreate())
  ```
  """

  # Load XMLs in dataframe

  # Load schema
  json_f = open(schema_path)

  newSchema = StructType.fromJson(json.load(json_f))

  df = spark.read \
      .format("com.databricks.spark.xml") \
      .option("rootTag", "hierarchy") \
      .option("rowTag", "nfeProc") \
      .schema(newSchema) \
      .load(','.join(xmls_list_processing))
      #.option("fetchsize","500")  \
      #.load('/content/xml/42200133009911009519550050000063501420824555.xml')
      #

  # Writing result to parquet
  df.write.parquet(xml_path+r'\processing\xml_processing.parquet')
  df = spark.read.parquet(xml_path+r'\processing\xml_processing.parquet')

  df_estab = spark.read \
      .format("jdbc") \
      .option('driver',driver) \
      .option('url',jdbc_string) \
      .option('user',user) \
      .option('password',password) \
      .option("fetchsize","500")  \
      .option('dbtable','MSAF.ESTABELECIMENTO') \
      .load()

  df_estab.createOrReplaceTempView('ESTABELECIMENTO')

  df_result_item = df \
              .select(\
                  df['*'],\
                  explode(df.NFe.infNfe.det)
                  )


  # Visao de capa
  df.createOrReplaceTempView('XML_RAW_CAPA')
  df_sql_capa = spark.sql(xmlToOracle.spark_capa)

  # Visao de item
  df_result_item.createOrReplaceTempView('XML_RAW_ITEM')
  df_sql_item = spark.sql(xmlToOracle.spark_item)

  # Visao de pessoa fisica juridica
  df_sql_pessoafisjur = spark.sql(xmlToOracle.spark_pessoafisjur)


  
  # Writing the result to Oracle
  df_sql_capa \
        .write \
        .format("jdbc") \
        .option('driver',driver) \
        .option('url',jdbc_string) \
        .option('user',user) \
        .option('password',password) \
        .option('dbtable','MSAF.SAFX07_TEMP') \
        .mode('overwrite') \
        .save()
        #.option("batchsize","500")  \
        
  df_sql_item \
        .write \
        .format("jdbc") \
        .option('driver',driver) \
        .option('url',jdbc_string) \
        .option('user',user) \
        .option('password',password) \
        .option('dbtable','MSAF.SAFX08_TEMP') \
        .mode('overwrite') \
        .save()

  df_sql_pessoafisjur \
        .write \
        .format("jdbc") \
        .option('driver',driver) \
        .option('url',jdbc_string) \
        .option('user',user) \
        .option('password',password) \
        .option('dbtable','MSAF.SAFX04_TEMP') \
        .mode('overwrite') \
        .save()

  print(df_sql_item.count())

  # Fetch the driver manager from your spark context
  driver_manager = spark._sc._gateway.jvm.java.sql.DriverManager

  # Create a connection object using a jdbc-url, + sql uname & pass
  con = driver_manager.getConnection(jdbc_string, user, password)


  # Write your SQL statement as a string
  
  # Create callable statement and execute it
  exec_statement = con.prepareCall(xmlToOracle.oracle_safx07)
  exec_statement.execute()

  exec_statement = con.prepareCall(xmlToOracle.oracle_safx08)
  exec_statement.execute()

  exec_statement = con.prepareCall(xmlToOracle.oracle_safx04)
  exec_statement.execute()



  # Close connection
  exec_statement.close()
  con.close()
  spark.stop()

  # Move the files


  # Moving XML to processed dir
  for i in xmls_list_processing:
    shutil.move(i,xml_path+r"processed\\")

  # Removing parquet file
  shutil.rmtree(xml_path+r'\processing\xml_processing.parquet')