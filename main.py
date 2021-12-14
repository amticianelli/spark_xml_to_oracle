import json
import os
import findspark
import shutil
from query import xmlToOracle
from datetime import datetime

# tornar o pyspark "importável"
# findspark.add_packages('com.databricks:spark-xml_2.12:0.13.0')

findspark.init()

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import explode, row_number
from pyspark.sql.types import *
from pyspark.sql.functions import struct
from config.config import Config



# Config variables
xml_path = Config.xml_path
schema_path= Config.schema_path

# Oracle Database Connection
jdbc_string = Config.jdbc_string
driver = Config.driver
user = Config.user
password= Config.password

# Read Cod Docto number position
def getCodDocto():
  docto = 900000061429
  if(os.path.exists(xml_path+'/num_controle_counter.txt')):
    f = open(xml_path+'/num_controle_counter.txt','r')
    docto = f.readline()
    f.close()
  else:
    # Create the position file if is doesn't exist
    f = open(xml_path+'/num_controle_counter.txt','w')
    f.write('900000061429')
    f.close()
  
  return docto

def setCodDocto(docto_number):
  f = open(xml_path+'/num_controle_counter.txt','w')
  f.write(str(docto_number))
  f.close()

# Map all files in directory

xmls_list = []
xmls_list_processing = []

for f in os.listdir(xml_path+r"landing\\"):
  if os.path.isfile(os.path.join(xml_path+r"landing\\", f)):
    xmls_list.append(r""+xml_path+r"landing\\"+f)
    xmls_list_processing.append(r""+xml_path+r'processing\\'+f)

print("Files to be processed: "+str(len(xmls_list)))

# Moving files to processing dir

try:
  for i in xmls_list:
    shutil.move(i,xml_path+r'processing\\')
except Exception as e:
  print('File already existis')

# Starting local session and import data do Spark

if len(xmls_list) > 0:

  try:
    spark = SparkSession \
              .builder \
              .config('spark.executor.memory', Config.spark_mem) \
              .config('spark.driver.memory', Config.spark_mem)  \
              .config('spark.submit.deployMode','client') \
              .config('spark.yarn.queue','short') \
              .config('spark.executor.cores',Config.spark_core) \
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
    output_name = 'xml_processing_'+datetime.today().strftime("%Y%m%d%H%M%S")+'.parquet'
    df.write.parquet(xml_path+r'\processing\\'+output_name)
    df = spark.read.parquet(xml_path+r'\processing\\'+output_name)



    # Returning Mastersaf DW tables
    df_estab = spark.read \
        .format("jdbc") \
        .option('driver',driver) \
        .option('url',jdbc_string) \
        .option('user',user) \
        .option('password',password) \
        .option("fetchsize","500")  \
        .option('dbtable','MSAF.ESTABELECIMENTO') \
        .load()

    
    # Reading pessoa_fis_jur table

    df_x04 = spark.read \
        .format("jdbc") \
        .option('driver',driver) \
        .option('url',jdbc_string) \
        .option('user',user) \
        .option('password',password) \
        .option("fetchsize","500")  \
        .option('dbtable','MSAF.X04_PESSOA_FIS_JUR') \
        .load()

    df_cfop = spark.read \
        .format("jdbc") \
        .option('driver',driver) \
        .option('url',jdbc_string) \
        .option('user',user) \
        .option('password',password) \
        .option("fetchsize","500")  \
        .option('query',xmlToOracle.oracle_param_cfop) \
        .load()

    df_ncm = spark.read \
        .format("jdbc") \
        .option('driver',driver) \
        .option('url',jdbc_string) \
        .option('user',user) \
        .option('password',password) \
        .option("fetchsize","500")  \
        .option('query',xmlToOracle.oracle_param_produto) \
        .load()


    df_estab.createOrReplaceTempView('ESTABELECIMENTO')
    df_cfop.createOrReplaceTempView('MSAFCFOP')
    df_ncm.createOrReplaceTempView('MSAFNCM')


    # Exploding itens
    df_result_item = df \
                .select(\
                    df['*'],\
                    explode(df.NFe.infNfe.det)
                    )


    # Setting getDocto function as an UDF
    spark.udf.register("getDoctoPython",getCodDocto)

    # Visao de capa
    df.createOrReplaceTempView('XML_RAW_CAPA')
    df_sql_capa = spark.sql(xmlToOracle.spark_capa)

    ### Separando registros de capa
    df_sql_capa_error = df_sql_capa \
                          .where("COD_CFO = 'NP'")
    
    df_sql_capa = df_sql_capa \
                          .where("COD_CFO != 'NP'")
    ##



    # Visao de item
    df_result_item.createOrReplaceTempView('XML_RAW_ITEM')
    df_sql_item = spark.sql(xmlToOracle.spark_item)

    ### Seprando registros de item
    df_sql_item_error = df_sql_item \
                              .where("COD_CFO = 'NP' OR COD_PRODUTO = 'NP' OR COD_NATUREZA_OP = 'NP'")
    
    df_sql_item =       df_sql_item \
                              .where("COD_CFO != 'NP' AND COD_PRODUTO != 'NP' AND COD_NATUREZA_OP != 'NP'")


    # Visao de pessoa fisica juridica
    df_x04.createOrReplaceTempView('X04_PESSOA_FIS_JUR')
    df_sql_pessoafisjur = spark.sql(xmlToOracle.spark_pessoafisjur)


    print('Writing the results to temp tables')
    
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

    # Show bad NF

    df_sql_capa_error \
      .repartition(1) \
      .write \
      .option('header',True) \
      .csv(xml_path+r"\\error\\param_error_capa_"+str(datetime.today().strftime("%Y%m%d%H%M%S"))) \


    df_sql_item_error \
      .repartition(1) \
      .write \
      .option('header',True) \
      .csv(xml_path+r"\\error\\param_error_item_"+str(datetime.today().strftime("%Y%m%d%H%M%S"))) \

    print(df_sql_item.count())

    print('Writing the results to SAFX tables')

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


    # Get docto position and record it to the text file
    capa_docto = getCodDocto()
    setCodDocto(str(int(capa_docto)+df_sql_capa.count()))


    # Close connections
    exec_statement.close()
    con.close()
    spark.stop()
  except Exception as e:
    for i in xmls_list_processing:
      shutil.move(i,xml_path+r"error\\")
    if output_name != "":
      shutil.rmtree(xml_path+r'\processing\\'+output_name)
    f = open(xml_path+r"error\\xml_error_"+str(datetime.today().strftime("%Y%m%d%H%M%S"))+'.txt','a')
    docto = f.write(str(e))
    f.close()
    raise e
  # Move the files


  # Moving XML to processed dir
  for i in xmls_list_processing:
    try:
      shutil.move(i,xml_path+r"processed\\")
    except Exception as e:
      print('Already existent file: ' + i)
      os.remove(i)

    
    #shutil.move(i,xml_path+r'\processing\\')

  # Removing parquet file
  shutil.rmtree(xml_path+r'\processing\\'+output_name)