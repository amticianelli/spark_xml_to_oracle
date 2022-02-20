
class Config:
    # Path onde serao lidos os XML
    xml_path = r"C:\GDrive\Adejo\SouzaCruz\xml\\"
    schema_path= r"C:\GDrive\Adejo\SouzaCruz\spark_xml_to_oracle\config\nfe_schema.json"
    #schema_path= r"C:\GDrive\Adejo\SouzaCruz\spark_xml_to_oracle\new_architecture_current.json"

    # Conexao Oracle Mastersaf DW
    jdbc_string = r''
    driver = 'oracle.jdbc.driver.OracleDriver'
    user = 'ADMIN'
    password=''

    # Spark configurations (nao alterar)
    spark_mem = '5g'
    spark_core = 4