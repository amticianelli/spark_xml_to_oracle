
class Config:
    # Path onde serao lidos os XML
    xml_path = r"C:\GDrive\Adejo\SouzaCruz\xml\\"
    schema_path= r"C:\GDrive\Adejo\SouzaCruz\spark_xml_to_oracle\config\nfe_schema.json"

    # Conexao Oracle Mastersaf DW
    jdbc_string = r'jdbc:oracle:thin:@db202110181402_high?TNS_ADMIN=C:/wallet'
    driver = 'oracle.jdbc.driver.OracleDriver'
    user = 'ADMIN'
    password='w8Z4c85_SwJQau'

    # Spark configurations (nao alterar)
    spark_mem = '5g'
    spark_core = 4