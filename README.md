# spark_xml_to_oracle
Souza Cruz ETL process to convert XML to different visions in Oracle. The program will be reading a predefined directory for new files with the xml extension, and ingest them to the Oracle Database.

# Architecture
![alt text](https://github.com/amticianelli/spark_xml_to_oracle/blob/main/img/XML_To_SAFX_Architecture.png)

# Installation

Necessary tools:

    - Apache Spark:
    - Hadoop:
    - Java 1.8:
    - Oracle JDBC Libraries:
    - Spark XML library:
    
## Spark installation

    - Download Apache Spark 3.1.2: https://spark.apache.org/downloads.html
    - Download the file winutils.exe e move to a local directory: https://github.com/cdarlint/winutils
    - Create an Environment Variable called: SPARK_HOME, and point it to the extracted directory with the spark binaries and winutils files donwloaded previously
    - Download Spark XML source, and move it to the spark Jars diretory in SPARK_HOME: https://repo1.maven.org/maven2/com/databricks/spark-xml_2.10/0.2.0/spark-xml_2.10-0.2.0.jar
    - Download Oracle JDBC driver, and move it to the spark Jars diretory in SPARK_HOME: https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html
    
    
 ## Program configuration
    - Set the environment variable ORACLE_PASSWORD for the database;
    - Open the file config/config.py and set the database configuration connection;
    - Run the scripts on the directory config/plsql/ in the Mastersaf DW schema user in Oracle;

## Program run
    - Run the program with the following command: 
``` python    
python main.py
```
