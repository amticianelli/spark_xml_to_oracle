create or replace PACKAGE ADEJO_SOUZAC_XML_CPAR IS

    -- autor   : Alberto M. Ticianelli
	-- created : 31/10/2021
	-- purpose : XMl to SAFX

  FUNCTION PARAMETROS RETURN VARCHAR2;
  FUNCTION NOME RETURN VARCHAR2;
  FUNCTION MODULO RETURN VARCHAR2;
  FUNCTION VERSAO RETURN VARCHAR2;
  FUNCTION DESCRICAO RETURN VARCHAR2;
  FUNCTION TIPO RETURN VARCHAR2;

END ADEJO_SOUZAC_XML_CPAR;
/

create or replace PACKAGE BODY ADEJO_SOUZAC_XML_CPAR IS

	mcod_empresa empresa.cod_empresa%TYPE;
	mcod_usuario     usuario_estab.cod_usuario%TYPE;

  FUNCTION PARAMETROS RETURN VARCHAR2 IS
		pstr VARCHAR2(5000);
    query_x04 VARCHAR2(5000);
  BEGIN

		mcod_empresa := LIB_PARAMETROS.RECUPERAR('EMPRESA');
		mcod_usuario := LIB_PARAMETROS.RECUPERAR('USUARIO');

 

  
	  pstr := pstr||  'CFOP NatOp|CFOP NatOp|null|CFOP NatOp|TextListBox|Varchar2||0000|'||
                      'select distinct TO_CHAR(x2012.cod_cfo) cod_cfo, descricao from msaf.x2012_cod_fiscal x2012 order by 1;';

    pstr := pstr||  'Produto|Produto|null|Produto|TextListBox|Varchar2||0000|'||
                      'SELECT distinct COD_NBM, DESCRICAO FROM MSAF.X2043_COD_NBM order by 1;';

    pstr := pstr||  'CNPJ|CNPJ|null|CNPJ|TextListBox|Varchar2||0000|SELECT * FROM CNPJ_X04;';




      RETURN pstr;
  END PARAMETROS;

  FUNCTION NOME RETURN VARCHAR2 IS
  BEGIN
    RETURN 'Parametrizacao para relatorio de vendas';
  END NOME;

  FUNCTION MODULO RETURN VARCHAR2 IS
  BEGIN
    RETURN 'Relatorios Vendas';
  END MODULO;

  FUNCTION VERSAO RETURN VARCHAR2 IS
  BEGIN
    RETURN 'V1.0';
  END VERSAO;

  FUNCTION DESCRICAO RETURN VARCHAR2 IS
  BEGIN
    RETURN null;
  END DESCRICAO;

  FUNCTION TIPO RETURN VARCHAR2 IS
  BEGIN
    RETURN 'Parametros';
  END;
END ADEJO_SOUZAC_XML_CPAR;
/