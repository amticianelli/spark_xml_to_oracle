import json
import os
import findspark
import shutil
from query import xmlToOracle
from datetime import datetime
# tornar o pyspark "importável"
#findspark.add_packages('com.databricks:spark-xml_2.12:0.13.0')

findspark.init()

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import explode, row_number
from pyspark.sql.types import *
from pyspark.sql.functions import struct
from pyspark.sql.functions import input_file_name
from config.config import Config


# Config variables
xml_path = Config.xml_path
schema_path= Config.schema_path

# Oracle Database Connection
jdbc_string = Config.jdbc_string
driver = Config.driver
user = Config.user
password= Config.password


def tagAvulsa(cpf: str,insc_estad: str,vlr: str):
  if cpf != None and insc_estad != None:
    return 0.0
  else:
    return 0.0 if vlr is None else float(vlr)


def setCodDocto(spark):
  df = spark.sql("""
  SELECT MAX(NUM_CONTROLE_DOCTO) AS NUM_DOCTO FROM DF_CAPA
  """)

  df.write \
          .format("jdbc") \
          .option('driver',driver) \
          .option('url',jdbc_string) \
          .option('user',user) \
          .option('password',password) \
          .option('dbtable','MSAF.NUM_DOCTO') \
          .mode('overwrite') \
          .save()

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
  print('File already exists')

# Starting local session and import data do Spark

fileName = datetime.today().strftime("%Y%m%d%H%M%S")

if len(xmls_list) > 0:
  try:
    spark = SparkSession \
              .builder \
              .config('spark.executor.memory', Config.spark_mem) \
              .config('spark.driver.memory', Config.spark_mem)  \
              .config('spark.submit.deployMode','client') \
              .config('spark.yarn.queue','short') \
              .config('spark.executor.cores',Config.spark_core) \
              .config('spark.sql.shuffle.partitions','4') \
              .config('spark.sql.session.timeZone','UTC') \
              .master('local[*]') \
              .appName("ETL_xml_to_SAFX") \
              .getOrCreate()


    spark.sparkContext.setLogLevel("WARN")

    # Load XMLs in dataframe

    # Load schema
    json_f = open(schema_path)

    json_schema = json.load(json_f)
    newSchema = StructType.fromJson(json_schema)

    df = spark.read \
        .format("com.databricks.spark.xml") \
        .option("rootTag", "hierarchy") \
        .option("rowTag", "nfeProc") \
        .schema(newSchema) \
        .load(','.join(xmls_list_processing))
        #
        
        #.option("fetchsize","500")  \
        #.load('/content/xml/42200133009911009519550050000063501420824555.xml')
        # .option("inferSchema", "false") \
    
    df = df.withColumn("filename",input_file_name())

    ## Verifying the schema
    df.show()
    #raise 'Schema field error, please contact the support for adding the new fields'
    df_schemaError = df.where('NFe is null')
    df = df.where('NFe is not null')


    # Writing result to parquet
    output_name = 'xml_processing_'+fileName+'.parquet'
    df.write.parquet(xml_path+r'\processing\\'+output_name)
    df = spark.read.parquet(xml_path+r'\processing\\'+output_name)

    # Show bad schema
    if df_schemaError.rdd.count() > 0:
      print('There are differences in the schema')
      df_schemaError \
        .select("filename") \
        .repartition(1) \
        .write \
        .option('header',True) \
        .csv(xml_path+r"\\error\\schema_error_"+str(fileName)) \

       
      xmls_list_schemaerror = []
      for row in df_schemaError.rdd.collect():
        schemaErrorFile = row['filename'].replace('file:/','')
        errorFileName = schemaErrorFile[schemaErrorFile.rfind('/')+1:]
        xmls_list_schemaerror.append(r""+xml_path+r'processing\\'+errorFileName)

        # Removing files
        for i in xmls_list_processing:
          if errorFileName in i:
              xmls_list_processing.remove(i)
           

      df_schemaError_dummy = spark.read \
        .format("com.databricks.spark.xml") \
        .option("rootTag", "hierarchy") \
        .option("rowTag", "nfeProc") \
        .load(','.join(xmls_list_schemaerror))

      # Comparing schemas
      error_schema: dict = json.loads(df_schemaError_dummy.schema.json())
      merged_schema = json_schema.copy()
      correct_schema: dict = json_schema.copy()
      merged_schema.update(error_schema)

      #print('Error {}:'.format(error_schema))
      #print('Correct {}:'.format(correct_schema))
      #print('Merged {}:'.format(merged_schema))

      f = open(schema_path,'w')
      f.write("{}".format(json.dumps(merged_schema, indent=2)))
      f.close()


      # 

      #print('Type of: {} and {}'.format(type(error_schema),type(correct_schema)))
      #f = open(xml_path+r"error\\schema_diff_batch.txt",'w')
      #json_object = json.loads(schema_diff(newSchema,error_schema))
      #json_object = DeepDiff(correct_schema ,error_schema, ignore_order=True)
      #print('DeepDiff out Type of: {}'.format(type(json_object)))
      #json_object = json.loads(json_object.to_json())
      #f.write("{}".format(json.dumps(json_object, indent=12)))
      #f.close()

      # Removing files
      for i in xmls_list_schemaerror:
        try:
          shutil.move(i,xml_path+r"error\\")
        except Exception as e:
          print('Already existent file: ' + i)
          os.remove(i)

    #####
    ## Uncomment the line below to use only the XML conversion
    #exit(0)

    # Returning Mastersaf DW tables
    # Adjustment requested by Jorge to remove an especific estab (08/15/2022)
    df_estab = spark.read \
        .format("jdbc") \
        .option('driver',driver) \
        .option('url',jdbc_string) \
        .option('user',user) \
        .option('password',password) \
        .option("fetchsize","500")  \
        .option('query',"""
                          SELECT * 
                          FROM MSAF.ESTABELECIMENTO 
                          WHERE 1=1
                          AND 
                            (
                              (CGC != '33009911046988' AND COD_ESTAB != 'BRBX')
                              OR
                              (CGC = '33009911046988' AND COD_ESTAB = 'BRBQ')
                            )
                  """) \
        .load() \
        .cache()
    
    # Reading pessoa_fis_jur table

    df_x04 = spark.read \
        .format("jdbc") \
        .option('driver',driver) \
        .option('url',jdbc_string) \
        .option('user',user) \
        .option('password',password) \
        .option("fetchsize","500")  \
        .option('query',"""
                          WITH v1 AS (
                          SELECT 
                            x04.*,
                            RANK() OVER(PARTITION BY CPF_CGC ORDER BY VALID_FIS_JUR DESC,IND_FIS_JUR, IDENT_FIS_JUR DESC) AS POSICAO
                          FROM MSAF.X04_PESSOA_FIS_JUR x04
                          )
                          SELECT * FROM v1 WHERE POSICAO = 1
                        """) \
        .load() \
        .cache()

    # Returning CFOP

    df_cfop = spark.read \
        .format("jdbc") \
        .option('driver',driver) \
        .option('url',jdbc_string) \
        .option('user',user) \
        .option('password',password) \
        .option("fetchsize","500")  \
        .option('query',xmlToOracle.oracle_param_cfop) \
        .load()\
        .cache()

    # Returning PRODUTO
    
    df_ncm = spark.read \
        .format("jdbc") \
        .option('driver',driver) \
        .option('url',jdbc_string) \
        .option('user',user) \
        .option('password',password) \
        .option("fetchsize","500")  \
        .option('query',xmlToOracle.oracle_param_produto) \
        .load()\
        .cache()


    df_estab.createOrReplaceTempView('ESTABELECIMENTO')
    df_cfop.createOrReplaceTempView('MSAFCFOP')
    df_ncm.createOrReplaceTempView('MSAFNCM')


    # Exploding itens
    df_result_item = df \
                .select(\
                    df['*'],\
                    explode(df.NFe.infNfe.det)
                    )


    print('Number of headers to be processed: '+str(df.count()))
    df.limit(10).show()
    print('Number of itens to be processed: '+str(df_result_item.count()))
    

    # Setting functions as an UDF
    #spark.udf.register("getDoctoPython",getCodDocto) ## DEPRECATED
    #spark.udf.register("setTagAvulsa",tagAvulsa,FloatType())
    spark.udf.register("setTagAvulsa",tagAvulsa,FloatType())

    # Getting value for NUM_CONTROLE_DOCTO function

    df_docto = spark.read \
        .format("jdbc") \
        .option('driver',driver) \
        .option('url',jdbc_string) \
        .option('user',user) \
        .option('password',password) \
        .option("fetchsize","500")  \
        .option('query',"""
                          SELECT NUM_DOCTO+1 AS NUM_DOCTO FROM MSAF.NUM_DOCTO
                        """) \
        .load()\
        .cache()

    df_docto.createOrReplaceTempView('NUM_DOCTO')

    

    # Visao de pessoa fisica juridica
    df_x04.createOrReplaceTempView('X04_PESSOA_FIS_JUR')
    

    # Visao de item
    df_result_item.createOrReplaceTempView('XML_RAW_ITEM')
    df_sql_item = spark.sql(xmlToOracle.spark_item) \
        .cache()
    df_sql_item.createOrReplaceTempView('XML_ITEM') # Para utilizacao na capa


    # Visao de capa
    df.createOrReplaceTempView('XML_RAW_CAPA')
    df_sql_capa = spark.sql(xmlToOracle.spark_capa)
    df_sql_capa.createOrReplaceTempView('DF_CAPA')

    

    ### Separando registros de capa
    df_sql_capa_error = df_sql_capa \
                          .where("COD_CFO = 'NP'")
    
    df_sql_capa = df_sql_capa \
                          .where("COD_CFO != 'NP'")
    ##


    df_sql_pessoafisjur = spark.sql(xmlToOracle.spark_pessoafisjur)
    

    ### Seprando registros de item
    df_sql_item_error = df_sql_item \
                              .where("COD_CFO = 'NP' OR COD_PRODUTO = 'NP' OR COD_NATUREZA_OP = 'NP'")
    
    df_sql_item =       df_sql_item \
                              .where("COD_CFO != 'NP' AND COD_PRODUTO != 'NP' AND COD_NATUREZA_OP != 'NP'")



    print('Number of headers to be inserted: '+str(df_sql_capa.count()))
    print('Number of items to be inserted: '+str(df_sql_item.count()))
    

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
    if df_sql_capa_error.rdd.count() > 0:
      df_sql_capa_error \
        .repartition(1) \
        .write \
        .option('header',True) \
        .csv(xml_path+r"\\error\\param_error_capa_"+str(fileName)) \

      print('Number of headers with errors: '+str(df_sql_capa_error.count()))

    if df_sql_item_error.rdd.count() > 0:
      df_sql_item_error \
        .repartition(1) \
        .write \
        .option('header',True) \
        .csv(xml_path+r"\\error\\param_error_item_"+str(fileName)) \

      print('Number of itens with errors: '+str(df_sql_item_error.count()))


      
        
      

      # Moving the bad XMLs to the error dir
      #for row in df_schemaError.rdd.collect():
      #  print(row['filename'])
      #  try:
      #    xmls_list_processing.remove(row['filename'])
      #  except Exception as e:
      #    print(e)
      

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
    setCodDocto(spark)


    # Close connections
    exec_statement.close()
    con.close()
    spark.stop()
  except Exception as e:
    print('Error in the execution')
    for i in xmls_list_processing:
        try:
          shutil.move(i,xml_path+r"error\\")
        except Exception as e2:
          shutil.rmtree(i)
          f = open(xml_path+r"error\\xml_error_"+str(fileName)+'.txt','a')
          docto = f.write(str(e2))
        if output_name != "":
          shutil.rmtree(xml_path+r'\processing\\'+output_name)
        f = open(xml_path+r"error\\xml_error_"+str(fileName)+'.txt','a')
        docto = f.write(str(e))
        f.close()
        xmls_list_processing.remove(i)
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
  # shutil.rmtree(xml_path+r'\processing\\'+output_name)