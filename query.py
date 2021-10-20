
class xmlToOracle:

    spark_capa = """
        SELECT
        ESTAB.COD_EMPRESA AS COD_EMPRESA,
        ESTAB.COD_ESTAB AS COD_ESTAB,
        '1' AS MOVTO_E_S,
        '1' AS NORM_DEV,
        'NFE' AS COD_DOCTO,
        '1' AS IDENT_FIS_JUR,
        'M'||SUBSTR(NFe.infNfe.dest.CNPJ,1,3) || SUBSTR(NFe.infNfe.dest.CNPJ,(-1*length(NFe.infNfe.dest.CNPJ)),4) AS COD_FIS_JUR,
        LPAD(NFe.infNfe.ide.cNF,12,'0') AS NUM_DOCFIS,
        NFe.infNfe.ide.serie AS SERIE_DOCFIS,
        DATE_FORMAT(NFe.infNfe.ide.dhEmi,'yyyyMMdd') AS DATA_EMISSAO,
        '1' AS COD_CLASS_DOC_FIS,
        ''||NFe.infNfe.ide.mod AS COD_MODELO,
        ''||NFe.infNfe.det[0].prod.CFOP AS COD_CFO,
        ''||SUBSTR(NFe.infNfe.ide.NFref[0].refNFe,1,12) AS NUM_DOCFIS_REF,
        ''||NFe.infNfe.ide.NFref[0].refNF.serie AS SERIE_DOCFIS_REF,
        DATE_FORMAT(NFe.infNfe.ide.dhEmi,'yyyyMMdd') AS DATA_SAIDA_REC, -- Reviewing
        LPAD(REPLACE(REPLACE(STRING(FORMAT_NUMBER(NFe.infNfe.total.ICMSTot.vProd,2)),'.'),','),15,'0') AS VLR_PRODUTO,
        LPAD(REPLACE(REPLACE(STRING(FORMAT_NUMBER(NFe.infNfe.total.ICMSTot.vNF,2)),'.'),','),15,'0') AS VLR_TOT_NOTA, 
        'N' AS SITUACAO,
        '0'  AS NUM_CONTROLE_DOCTO, -- Create sequence
        '3' AS IND_FATURA,
        protNFe.infProt.chNFe AS NUM_AUTENTIC_NFE,
        DATE_FORMAT(NFe.infNfe.ide.dhEmi,'yyyyMMdd') AS DAT_LANC_PIS_COFINS -- Reviewing
        FROM XML_RAW_CAPA
        LEFT JOIN ESTABELECIMENTO ESTAB ON 1=1
        AND ESTAB.CGC = XML_RAW_CAPA.NFe.infNfe.dest.CNPJ
    """

    spark_item = """
        SELECT
        ESTAB.COD_EMPRESA AS COD_EMPRESA,
        ESTAB.COD_ESTAB AS COD_ESTAB,
        '1' AS MOVTO_E_S,
        '1' AS NORM_DEV,
        'NFE' AS COD_DOCTO,
        '1' AS IDENT_FIS_JUR,
        'M'||SUBSTR(NFe.infNfe.dest.CNPJ,1,3) || SUBSTR(NFe.infNfe.dest.CNPJ,(-1*length(NFe.infNfe.dest.CNPJ)),4) AS COD_FIS_JUR,
        LPAD(NFe.infNfe.ide.cNF,12,'0') AS NUM_DOCFIS,
        NFe.infNfe.ide.serie AS SERIE_DOCFIS,
        '1' AS IND_PRODUTO,
        'CDTESTE001' AS COD_PRODUTO, -- To be defined (De/Para)
        NFe.infNfe.det[0]._nItem AS NUM_ITEM,
        NFe.infNfe.det[0].prod.CFOP AS COD_CFO, --(De/Para)
        'TST' AS COD_NATUREZA_OP,
        LPAD(REPLACE(REPLACE(STRING(FORMAT_NUMBER(NFe.infNfe.det[0].prod.qCom,6)),'.'),','),11,'0') AS QUANTIDADE,
        NFe.infNfe.det[0].prod.uCom AS COD_MEDIDA,
        NFe.infNfe.det[0].prod.NCM AS COD_NBM,
        LPAD(REPLACE(REPLACE(STRING(FORMAT_NUMBER(NFe.infNfe.det[0].prod.vUnCom,4)),'.'),','),15,'0') AS VLR_UNIT,
        LPAD(REPLACE(REPLACE(STRING(FORMAT_NUMBER(NFe.infNfe.det[0].prod.vProd,2)),'.'),','),15,'0') AS VLR_ITEM,
        NFe.infNfe.det[0].imposto.ICMS.ICMS40.orig AS COD_SITUACAO_A,
        CASE WHEN NFe.infNfe.det[0].imposto.ICMS.ICMS00.vicms > 0 THEN '90' ELSE '41' END AS COD_SITUACAO_B,
        '00003' AS COD_FEDERAL,
        CASE WHEN NFe.infNfe.det[0].imposto.ICMS.ICMS00.vicms > 0 THEN '3' ELSE '2' END AS TRIB_ICMS,
        LPAD(REPLACE(REPLACE(STRING(FORMAT_NUMBER(NFe.infNfe.det[0].imposto.ICMS.ICMS00.vbc,2)),'.'),','),15,'0') AS BASE_ICMS,
        '3' AS TRIB_IPI,
        LPAD(REPLACE(REPLACE(STRING(FORMAT_NUMBER(NFe.infNfe.det[0].imposto.IPI.IPITrib.vBC,2)),'.'),','),15,'0') AS BASE_IPI,
        LPAD(REPLACE(REPLACE(STRING(FORMAT_NUMBER(NFe.infNfe.det[0].prod.vProd,2)),'.'),','),15,'0') AS VLR_CONTAB_ITEM,
        '70' AS COD_SITUACAO_PIS,
        '70' AS COD_SITUACAO_COFINS,
        DATE_FORMAT(NFe.infNfe.ide.dhEmi,'yyyyMMdd') AS DAT_LANC_PIS_COFINS
        FROM XML_RAW_ITEM
        LEFT JOIN ESTABELECIMENTO ESTAB ON 1=1
        AND ESTAB.CGC = XML_RAW_ITEM.NFe.infNfe.dest.CNPJ
    """



    spark_pessoafisjur = """
        SELECT DISTINCT
        '1' AS IND_FIS_JUR,
        'M'||SUBSTR(NFe.infNfe.dest.CNPJ,1,3) || SUBSTR(NFe.infNfe.dest.CNPJ,(-1*length(NFe.infNfe.dest.CNPJ)),4) AS COD_FIS_JUR,
        DATE_FORMAT(NFe.infNfe.ide.dhEmi,'yyyyMMdd') AS DATA_X04,
        '1' AS IND_CONTEM_COD,
        NFe.infNfe.emit.Xnome AS RAZAO_SOCIAL,
        NFe.infNfe.emit.CNPJ AS CPF_CGC,
        NFe.infNfe.emit.IE AS INSC_ESTADUAL,
        NFe.infNfe.emit.Xnome AS NOME_FANTASIA,
        NFe.infNfe.emit.enderEmit.xLgr AS ENDERECO,
        NFe.infNfe.emit.enderEmit.nro AS NUM_ENDERECO,
        NFe.infNfe.emit.enderEmit.XBairro AS BAIRRO,
        NFe.infNfe.emit.enderEmit.Xmun AS CIDADE,
        NFe.infNfe.emit.enderEmit.UF AS UF,
        NFe.infNfe.emit.enderEmit.CEP AS CEP,
        SUBSTR(NFe.infNfe.emit.enderEmit.cPais,1,3) AS COD_PAIS,
        SUBSTR(NFe.infNfe.emit.enderEmit.cmun,1,5) AS COD_MUNICIPIO
        FROM XML_RAW_CAPA
    """

    oracle_safx07 = """
    INSERT INTO MSAF.SAFX07
        (
        COD_EMPRESA,
        COD_ESTAB,
        MOVTO_E_S,
        NORM_DEV,
        COD_DOCTO,
        IDENT_FIS_JUR,
        COD_FIS_JUR,
        NUM_DOCFIS,
        SERIE_DOCFIS,
        DATA_EMISSAO,
        COD_CLASS_DOC_FIS,
        COD_MODELO,
        COD_CFO,
        NUM_DOCFIS_REF,
        SERIE_DOCFIS_REF,
        DATA_SAIDA_REC,
        VLR_PRODUTO,
        VLR_TOT_NOTA,
        SITUACAO,
        NUM_CONTROLE_DOCTO,
        IND_FATURA,
        NUM_AUTENTIC_NFE,
        DAT_LANC_PIS_COFINS
        )
        SELECT * FROM MSAF.SAFX07_TEMP
    """

    oracle_safx08 = """
    INSERT INTO MSAF.SAFX08
        (
        COD_EMPRESA,
        COD_ESTAB,
        MOVTO_E_S,
        NORM_DEV,
        COD_DOCTO,
        IND_FIS_JUR,
        COD_FIS_JUR,
        NUM_DOCFIS,
        SERIE_DOCFIS,
        IND_PRODUTO,
        COD_PRODUTO,
        NUM_ITEM,
        COD_CFO,
        COD_NATUREZA_OP,
        QUANTIDADE,
        COD_MEDIDA,
        COD_NBM,
        VLR_UNIT,
        VLR_ITEM,
        COD_SITUACAO_A,
        COD_SITUACAO_B,
        COD_FEDERAL,
        TRIB_ICMS,
        BASE_ICMS,
        TRIB_IPI,
        BASE_IPI,
        VLR_CONTAB_ITEM,
        COD_SITUACAO_PIS,
        COD_SITUACAO_COFINS,
        DAT_LANC_PIS_COFINS
        )
        SELECT * FROM MSAF.SAFX08_TEMP
    """

    oracle_safx04 = """
    INSERT INTO MSAF.SAFX04
        (
        IND_FIS_JUR,
        COD_FIS_JUR,
        DATA_X04,
        IND_CONTEM_COD,
        RAZAO_SOCIAL,
        CPF_CGC,
        INSC_ESTADUAL,
        NOME_FANTASIA,
        ENDERECO,
        NUM_ENDERECO,
        BAIRRO,
        CIDADE,
        UF,
        CEP,
        COD_PAIS,
        COD_MUNICIPIO
        )
        SELECT * FROM MSAF.SAFX04_TEMP
    """