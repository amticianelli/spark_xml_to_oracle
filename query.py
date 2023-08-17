
class xmlToOracle:

    spark_capa = """
        SELECT
            XI.COD_EMPRESA,
            XI.COD_ESTAB,
            '1' AS MOVTO_E_S,
            '1' AS NORM_DEV,
            CASE
                WHEN NFe.infNfe.ide.nNF IS NOT NULL THEN 'NFE'
                WHEN NFe.infNfe.ide.mod = '57' THEN 'YD'
                ELSE 'YJ'
            END AS COD_DOCTO,
            XI.IDENT_FIS_JUR,
            XI.COD_FIS_JUR,
            LPAD(NVL(NFe.infNfe.ide.nNF,NFe.infNfe.ide.nCT),9,'0') AS NUM_DOCFIS,
            NFe.infNfe.ide.serie AS SERIE_DOCFIS,
            DATE_FORMAT(SUBSTR(NFe.infNfe.ide.dhEmi,1,10),'yyyyMMdd') AS DATA_EMISSAO,
            '1' AS COD_CLASS_DOC_FIS,
            NFe.infNfe.ide.mod AS COD_MODELO,
            NVL(MSAFCFOP.novo_cfo,'NP') AS COD_CFO,
            ''||SUBSTR(NFe.infNfe.ide.NFref[0].refNFe,26,9) AS NUM_DOCFIS_REF,
            ''||SUBSTR(NFe.infNfe.ide.NFref[0].refNFe,23,3) AS SERIE_DOCFIS_REF,
            DATE_FORMAT(CURRENT_DATE(),'yyyyMMdd') AS DATA_SAIDA_REC, -- Reviewing
            CASE 
                WHEN NFe.infNfe.ide.nCT IS NULL THEN
                    LPAD(REPLACE(REPLACE(FORMAT_NUMBER(setTagAvulsa(NFe.infNfe.emit.CPF,NFe.infNfe.emit.IE,NFe.infNfe.total.ICMSTot.vProd),2),'.'),','),17,'0')
                ELSE
                    LPAD(REPLACE(REPLACE(NFe.infNfe.Vprest.vTPrest,'.'),','),17,'0')
            END AS VLR_PRODUTO,
            CASE 
                WHEN NFe.infNfe.ide.nCT IS NULL THEN
                    LPAD(REPLACE(REPLACE(FORMAT_NUMBER(setTagAvulsa(NFe.infNfe.emit.CPF,NFe.infNfe.emit.IE,NFe.infNfe.total.ICMSTot.vNF),2),'.'),','),17,'0')
                ELSE
                    LPAD(REPLACE(REPLACE(NFe.infNfe.Vprest.vTPrest,'.'),','),17,'0')
            END AS VLR_TOT_NOTA,
            'N' AS SITUACAO,
            (row_number() over (partition by DATE_FORMAT(CURRENT_DATE(),'yyyyMMdd') order by NVL(NFe.infNfe.ide.nNF,NFe.infNfe.ide.nCT) ASC))+(SELECT NUM_DOCTO FROM NUM_DOCTO) AS NUM_CONTROLE_DOCTO, -- Create sequence
            '3' AS IND_FATURA,
            protNFe.infProt.chNFe AS NUM_AUTENTIC_NFE,
            DATE_FORMAT(CURRENT_DATE(),'yyyyMMdd') AS DAT_LANC_PIS_COFINS,
            LPAD(REPLACE(REPLACE(FORMAT_NUMBER(setTagAvulsa(NFe.infNfe.emit.CPF,NFe.infNfe.emit.IE,XI.BASE_ISEN_ICMS),2),'.'),','),17,'0') AS BASE_ISEN_ICMS,
            LPAD(REPLACE(REPLACE(FORMAT_NUMBER(setTagAvulsa(NFe.infNfe.emit.CPF,NFe.infNfe.emit.IE,XI.BASE_OUTR_ICMS),2),'.'),','),17,'0') AS BASE_OUTR_ICMS,
            LPAD(REPLACE(REPLACE(FORMAT_NUMBER(setTagAvulsa(NFe.infNfe.emit.CPF,NFe.infNfe.emit.IE,XI.BASE_IPI),2),'.'),','),17,'0') AS BASE_OUTR_IPI,
            NFe.infNfe.ide.mod AS COD_MODELO_COTEPE,
            CASE 
                WHEN NFe.infNfe.emit.CPF IS NOT NULL AND NFe.infNfe.emit.IE IS NOT NULL THEN 'S' 
                ELSE NULL
            END AS IND_NF_REG_ESPECIAL,
            LPAD(REPLACE(REPLACE(FORMAT_NUMBER(setTagAvulsa(NFe.infNfe.emit.CPF,NFe.infNfe.emit.IE,NFe.infNfe.total.ICMSTot.vDesc),2),'.'),','),17,'0') AS VLR_DESCONTO,
            LPAD(REPLACE(REPLACE(NFe.infNfe.total.ICMSTot.vDesc,'.'),','),17,'0') AS VLR_ABAT_NTRIBUTADO,
            '2' AS IND_TP_FRETE,
            COALESCE(X04.UF,NFe.infNfe.ide.UFIni) AS UF_ORIG_DEST,
            NVL(estab_dest.UF,NFe.infNfe.ide.UFFim) AS UF_DESTINO,
            NVL(X04.COD_MUNICIPIO,SUBSTR(NFe.infNfe.ide.cMunIni,3,5)) AS COD_MUNICIPIO_ORIG,
            NVL(estab_dest.cod_municipio,SUBSTR(NFe.infNfe.ide.cMunFim,3,5)) AS COD_MUNICIPIO_DEST,
            NVL(MSAFCFOP.novo_natop,'NP') AS COD_NATUREZA_OP
        FROM XML_RAW_CAPA
        LEFT JOIN X04_PESSOA_FIS_JUR X04 ON 1=1
            AND LPAD(NVL(NFe.infNfe.emit.CNPJ,NFe.infNfe.emit.CPF),14,'0') = LPAD(X04.CPF_CGC,14,'0')
        LEFT JOIN ESTABELECIMENTO estab_dest ON 1=1
            AND estab_dest.CGC = NFe.infNfe.dest.CNPJ
            AND estab_dest.COD_ESTAB LIKE 'BR%'
            AND  
                (
                    NFe.infNfe.ide.toma3.toma = '3'
                    OR
                    NFe.infNfe.ide.mod = '55'
                )
        LEFT JOIN ESTABELECIMENTO estab_rem ON 1=1
            AND estab_rem.CGC = NFe.infNfe.rem.CNPJ
            AND estab_rem.COD_ESTAB LIKE 'BR%'
            AND NFe.infNfe.ide.toma3.toma = '0'
        LEFT JOIN ESTABELECIMENTO estab_toma ON 1=1
            AND estab_toma.COD_ESTAB LIKE 'BR%'
            AND estab_toma.CGC = NVL(NFe.infNfe.ide.toma4.CNPJ,NFe.infNfe.toma.CNPJ)
            AND (
                    (
                        NFe.infNfe.ide.toma4.toma = '4'
                        AND
                        estab_toma.CGC LIKE '33009911%'
                    )
                OR
                    (
                        NFe.infNfe.ide.toma3.toma IS NULL
                        AND
                        NFe.infNfe.ide.toma4.toma IS NULL
                    )
                )
        LEFT JOIN ESTABELECIMENTO estab_exped ON 1=1
            AND estab_exped.CGC = NFe.infNfe.exped.CNPJ
            AND estab_exped.COD_ESTAB LIKE 'BR%'
            AND NFe.infNfe.ide.toma3.toma = '1'
        LEFT JOIN ESTABELECIMENTO estab_receb ON 1=1
            AND estab_receb.CGC = NFe.infNfe.receb.CNPJ
            AND estab_receb.COD_ESTAB LIKE 'BR%'
            AND NFe.infNfe.ide.toma3.toma = '2'
        LEFT JOIN MSAFCFOP ON 1=1
            AND NVL(NFe.infNfe.det[0].prod.CFOP,NFe.infNfe.ide.CFOP) = MSAFCFOP.cod_cfo
        LEFT JOIN (
            SELECT 
                COD_EMPRESA,
                COD_ESTAB,
                NUM_DOCFIS,
                COD_FIS_JUR,
                IDENT_FIS_JUR,
                SUM(BASE_IPI)/100 AS BASE_IPI,
                SUM(CASE TRIB_ICMS WHEN '3' THEN BASE_ICMS ELSE 0 END)/100 AS BASE_OUTR_ICMS,
                SUM(CASE TRIB_ICMS WHEN '2' THEN BASE_ICMS ELSE 0 END)/100 AS BASE_ISEN_ICMS
            FROM XML_ITEM
            GROUP BY
                COD_EMPRESA,
                COD_ESTAB,
                NUM_DOCFIS,
                COD_FIS_JUR,
                IDENT_FIS_JUR
        ) XI ON 1=1
            AND XI.COD_EMPRESA = coalesce(estab_dest.COD_EMPRESA,estab_toma.COD_EMPRESA,estab_rem.COD_EMPRESA,estab_exped.COD_EMPRESA,estab_receb.COD_EMPRESA)
            AND XI.COD_ESTAB = coalesce(estab_dest.COD_ESTAB,estab_toma.COD_ESTAB,estab_rem.COD_ESTAB,estab_exped.COD_ESTAB,estab_receb.COD_ESTAB)
            AND XI.NUM_DOCFIS = LPAD(NVL(NFe.infNfe.ide.nNF,NFe.infNfe.ide.nCT),9,'0')
        WHERE 1=1
    """

    spark_item_cte = """
        SELECT
            coalesce(estab_dest.COD_EMPRESA,estab_toma.COD_EMPRESA,estab_rem.COD_EMPRESA,estab_exped.COD_EMPRESA,estab_receb.COD_EMPRESA) AS COD_EMPRESA,
            coalesce(estab_dest.COD_ESTAB,estab_toma.COD_ESTAB,estab_rem.COD_ESTAB,estab_exped.COD_ESTAB,estab_receb.COD_ESTAB) AS COD_ESTAB,
            DATE_FORMAT(CURRENT_DATE(),'yyyyMMdd') AS DATA_FISCAL,
            '1' AS MOVTO_E_S,
            '1' AS NORM_DEV,
            CASE 
                WHEN NFe.infNfe.ide.mod = '57' THEN 'YD'
                ELSE 'YJ'
            END AS COD_DOCTO,
            NVL(X04.IND_FIS_JUR,'1') AS IND_FIS_JUR,
            NVL(X04.COD_FIS_JUR,(CASE 
                WHEN NFe.infNfe.emit.CNPJ IS NOT NULL THEN
                    'M'||SUBSTR(NFe.infNfe.emit.CNPJ,1,8) || SUBSTR(NFe.infNfe.emit.CNPJ,-4)
                ELSE
                    'M'||SUBSTR(NFe.infNfe.emit.CPF,1,8) || SUBSTR(NFe.infNfe.emit.CPF,-4)
                END)) AS COD_FIS_JUR,
            LPAD(NFe.infNfe.ide.nCT,9,'0') AS NUM_DOCFIS,
            NFe.infNfe.ide.serie AS SERIE_DOCFIS,
            '8' AS IND_PRODUTO,
            NVL(MSAFNCM.material,'NP') AS COD_PRODUTO,
            '1' AS NUM_ITEM,
            NVL(MSAFCFOP.novo_cfo,'NP') AS COD_CFO,
            '' AS NUM_DOCFIS_REF,
            '' AS SERIE_DOCFIS_REF,
            NVL(MSAFCFOP.novo_natop,'NP') AS COD_NATUREZA_OP,
            '1000000' AS QUANTIDADE,
            NVL(MSAFNCM.cod_und_padrao,'NP') AS COD_MEDIDA,
            MSAFNCM.cod_ncm AS COD_NBM,
            LPAD(REPLACE(REPLACE(NFe.infNfe.Vprest.vTPrest,'.'),','),19,'0') AS VLR_UNIT,
            LPAD(REPLACE(REPLACE(NFe.infNfe.Vprest.vTPrest,'.'),','),17,'0') AS VLR_ITEM,
            '0' AS COD_SITUACAO_A,
            CASE 
                WHEN NVL(NFe.infNfe.imp.ICMS.ICMS00.vICMS,0) > 0 THEN '90'
            ELSE '41'
            END AS COD_SITUACAO_B,
            '00049' AS COD_FEDERAL,
            CASE 
                WHEN NVL(NFe.infNfe.imp.ICMS.ICMS00.vICMS,0) > 0 THEN '3'
            ELSE '2'
            END AS TRIB_ICMS,
            LPAD(REPLACE(REPLACE(NFe.infNfe.Vprest.vTPrest,'.'),','),17,'0') AS BASE_ICMS,
            '3' AS TRIB_IPI,
            LPAD(REPLACE(REPLACE(NFe.infNfe.Vprest.vTPrest,'.'),','),17,'0') AS BASE_IPI,
            LPAD(REPLACE(REPLACE(NFe.infNfe.Vprest.vTPrest,'.'),','),17,'0') AS VLR_CONTAB_ITEM,
            '70' AS COD_SITUACAO_PIS,
            '70' AS COD_SITUACAO_COFINS,
            DATE_FORMAT(CURRENT_DATE(),'yyyyMMdd') AS DAT_LANC_PIS_COFINS,
            'N' AS IND_BEM_PATR,
            NVL(MSAFNCM.cod_und_padrao,'NP') AS COD_UND_PADRAO,
            '@' AS VLR_DESCONTO,
            '@' AS COD_TRIB_IPI
        FROM XML_RAW_CAPA
        LEFT JOIN X04_PESSOA_FIS_JUR X04 ON 1=1
            AND LPAD(NVL(NFe.infNfe.emit.CNPJ,NFe.infNfe.emit.CPF),14,'0') = LPAD(X04.CPF_CGC,14,'0')
        LEFT JOIN ESTABELECIMENTO estab_dest ON 1=1
            AND estab_dest.CGC = NFe.infNfe.dest.CNPJ
            AND estab_dest.COD_ESTAB LIKE 'BR%'
            AND 
                (
                    NFe.infNfe.ide.toma3.toma = '3'
                    OR
                    NFe.infNfe.ide.mod = '55'
                )
        LEFT JOIN ESTABELECIMENTO estab_rem ON 1=1
            AND estab_rem.CGC = NFe.infNfe.rem.CNPJ
            AND estab_rem.COD_ESTAB LIKE 'BR%'
            AND NFe.infNfe.ide.toma3.toma = '0'
        LEFT JOIN ESTABELECIMENTO estab_toma ON 1=1
            AND estab_toma.COD_ESTAB LIKE 'BR%'
            AND estab_toma.CGC = NVL(NFe.infNfe.ide.toma4.CNPJ,NFe.infNfe.toma.CNPJ)
            AND (
                    (
                        NFe.infNfe.ide.toma4.toma = '4'
                        AND
                        estab_toma.CGC LIKE '33009911%'
                    )
                OR
                    (
                        NFe.infNfe.ide.toma3.toma IS NULL
                        AND
                        NFe.infNfe.ide.toma4.toma IS NULL
                    )
                )
        LEFT JOIN ESTABELECIMENTO estab_exped ON 1=1
            AND estab_exped.CGC = NFe.infNfe.exped.CNPJ
            AND estab_exped.COD_ESTAB LIKE 'BR%'
            AND NFe.infNfe.ide.toma3.toma = '1'
        LEFT JOIN ESTABELECIMENTO estab_receb ON 1=1
            AND estab_receb.CGC = NFe.infNfe.receb.CNPJ
            AND estab_receb.COD_ESTAB LIKE 'BR%'
            AND NFe.infNfe.ide.toma3.toma = '2'
        LEFT JOIN MSAFCFOP ON 1=1
            AND NFe.infNfe.ide.CFOP = MSAFCFOP.cod_cfo
        LEFT JOIN MSAFNCM ON 1=1
            AND MSAFNCM.cod_ncm = (CASE NFe.infNfe.ide.mod WHEN '57' THEN '1050113' ELSE '104011200' END)
        WHERE 1=1
            AND NFe.infNfe.ide.nCT IS NOT NULL
    """

    spark_item = """
        SELECT
            coalesce(estab_dest.COD_EMPRESA,estab_toma.COD_EMPRESA,estab_rem.COD_EMPRESA,estab_exped.COD_EMPRESA,estab_receb.COD_EMPRESA) AS COD_EMPRESA,
            coalesce(estab_dest.COD_ESTAB,estab_toma.COD_ESTAB,estab_rem.COD_ESTAB,estab_exped.COD_ESTAB,estab_receb.COD_ESTAB) AS COD_ESTAB,
            DATE_FORMAT(CURRENT_DATE(),'yyyyMMdd') AS DATA_FISCAL,
            '1' AS MOVTO_E_S,
            '1' AS NORM_DEV,
            'NFE' AS COD_DOCTO,
            coalesce(X04_PARAM.IND_FIS_JUR,X04.IND_FIS_JUR,'1') AS IDENT_FIS_JUR,
            coalesce(MSAFCNPJ.COD_FIS_JUR, X04.COD_FIS_JUR,(CASE 
                WHEN NFe.infNfe.emit.CNPJ IS NOT NULL THEN
                    'M'||SUBSTR(NFe.infNfe.emit.CNPJ,1,8) || SUBSTR(NFe.infNfe.emit.CNPJ,-4)
                ELSE
                    'M'||SUBSTR(NFe.infNfe.emit.CPF,1,8) || SUBSTR(NFe.infNfe.emit.CPF,-4)
                END)) AS COD_FIS_JUR,
            LPAD(NFe.infNfe.ide.nNF,9,'0') AS NUM_DOCFIS,
            NFe.infNfe.ide.serie AS SERIE_DOCFIS,
            '8' AS IND_PRODUTO,
            NVL(MSAFNCM.material,'NP') AS COD_PRODUTO,
            col._nItem AS NUM_ITEM,
            NVL(MSAFCFOP.novo_cfo,'NP') AS COD_CFO,
            ''||SUBSTR(NFe.infNfe.ide.NFref[0].refNFe,26,9) AS NUM_DOCFIS_REF,
            ''||SUBSTR(NFe.infNfe.ide.NFref[0].refNFe,23,3) AS SERIE_DOCFIS_REF,
            NVL(MSAFCFOP.novo_natop,'NP') AS COD_NATUREZA_OP,
            LPAD(REPLACE(REPLACE(FORMAT_NUMBER(DOUBLE(col.prod.qCom),6),'.'),','),17,'0') AS QUANTIDADE,
            NVL(MSAFNCM.cod_und_padrao,'NP') AS COD_MEDIDA,
            LPAD(col.prod.NCM,8,'0') AS COD_NBM,
            LPAD(REPLACE(REPLACE(FORMAT_NUMBER(setTagAvulsa(NFe.infNfe.emit.CPF,NFe.infNfe.emit.IE,col.prod.vUnCom),4),'.'),','),19,'0') AS VLR_UNIT,
            LPAD(REPLACE(REPLACE(FORMAT_NUMBER(setTagAvulsa(NFe.infNfe.emit.CPF,NFe.infNfe.emit.IE,col.prod.vProd),2),'.'),','),17,'0') AS VLR_ITEM,
            NVL(CASE 
                WHEN col.imposto.ICMS.ICMS00.orig IS NOT NULL THEN col.imposto.ICMS.ICMS00.orig
                WHEN col.imposto.ICMS.ICMS10.orig IS NOT NULL THEN col.imposto.ICMS.ICMS10.orig
                WHEN col.imposto.ICMS.ICMS30.orig IS NOT NULL THEN col.imposto.ICMS.ICMS30.orig
                WHEN col.imposto.ICMS.ICMS40.orig IS NOT NULL THEN col.imposto.ICMS.ICMS40.orig
                WHEN col.imposto.ICMS.ICMS51.orig IS NOT NULL THEN col.imposto.ICMS.ICMS51.orig
                WHEN col.imposto.ICMS.ICMS60.orig IS NOT NULL THEN col.imposto.ICMS.ICMS60.orig
                WHEN col.imposto.ICMS.ICMS70.orig IS NOT NULL THEN col.imposto.ICMS.ICMS70.orig
                WHEN col.imposto.ICMS.ICMS90.orig IS NOT NULL THEN col.imposto.ICMS.ICMS90.orig
                WHEN col.imposto.ICMS.ICMSPart.orig IS NOT NULL THEN col.imposto.ICMS.ICMSPart.orig
                WHEN col.imposto.ICMS.ICMSST.orig IS NOT NULL THEN col.imposto.ICMS.ICMSST.orig
                WHEN col.imposto.ICMS.ICMSSN101.orig IS NOT NULL THEN col.imposto.ICMS.ICMSSN101.orig
                WHEN col.imposto.ICMS.ICMSSN102.orig IS NOT NULL THEN col.imposto.ICMS.ICMSSN102.orig
                WHEN col.imposto.ICMS.ICMSSN201.orig IS NOT NULL THEN col.imposto.ICMS.ICMSSN201.orig
                WHEN col.imposto.ICMS.ICMSSN202.orig IS NOT NULL THEN col.imposto.ICMS.ICMSSN202.orig
                WHEN col.imposto.ICMS.ICMSSN500.orig IS NOT NULL THEN col.imposto.ICMS.ICMSSN500.orig
                WHEN col.imposto.ICMS.ICMSSN900.orig IS NOT NULL THEN col.imposto.ICMS.ICMSSN900.orig
            END,0) AS COD_SITUACAO_A,
            CASE WHEN col.imposto.ICMS.ICMS00.vicms > 0 THEN '90' ELSE '41' END AS COD_SITUACAO_B,
            '00003' AS COD_FEDERAL,
            CASE WHEN col.imposto.ICMS.ICMS00.vicms > 0 THEN '3' ELSE '2' END AS TRIB_ICMS,
            LPAD(REPLACE(REPLACE(FORMAT_NUMBER(setTagAvulsa(NFe.infNfe.emit.CPF,NFe.infNfe.emit.IE,col.prod.vProd - NVL(col.prod.vDesc,0) + NVL(col.imposto.IPI.IPITrib.vIPI,0) + NVL(col.prod.vOutro,0) + NVL(col.prod.vFrete,0) + NVL(col.prod.vSeg,0) 
                + coalesce(col.imposto.ICMS.ICMS10.vICMSST,col.imposto.ICMS.ICMS70.vICMSST,col.imposto.ICMS.ICMS30.vICMSST,col.imposto.ICMS.ICMS90.vICMSST,col.imposto.ICMS.ICMSSN201.vICMSST,col.imposto.ICMS.ICMSSN201.vICMSST,col.imposto.ICMS.ICMSSN900.vICMSST,0)
                + coalesce(col.imposto.ICMS.ICMS10.vFCPST,col.imposto.ICMS.ICMS70.vFCPST,col.imposto.ICMS.ICMS30.vFCPST,col.imposto.ICMS.ICMS90.vFCPST,col.imposto.ICMS.ICMSSN201.vFCPST,col.imposto.ICMS.ICMSSN201.vFCPST,col.imposto.ICMS.ICMSSN900.vFCPST,0)
            ),2),'.'),','),17,'0') AS BASE_ICMS,
            '3' AS TRIB_IPI,
            LPAD(REPLACE(REPLACE(FORMAT_NUMBER(setTagAvulsa(NFe.infNfe.emit.CPF,NFe.infNfe.emit.IE,col.prod.vProd - NVL(col.prod.vDesc,0) + NVL(col.imposto.IPI.IPITrib.vIPI,0) + NVL(col.prod.vOutro,0) + NVL(col.prod.vFrete,0) + NVL(col.prod.vSeg,0) 
                + coalesce(col.imposto.ICMS.ICMS10.vICMSST,col.imposto.ICMS.ICMS70.vICMSST,col.imposto.ICMS.ICMS30.vICMSST,col.imposto.ICMS.ICMS90.vICMSST,col.imposto.ICMS.ICMSSN201.vICMSST,col.imposto.ICMS.ICMSSN201.vICMSST,col.imposto.ICMS.ICMSSN900.vICMSST,0)
                + coalesce(col.imposto.ICMS.ICMS10.vFCPST,col.imposto.ICMS.ICMS70.vFCPST,col.imposto.ICMS.ICMS30.vFCPST,col.imposto.ICMS.ICMS90.vFCPST,col.imposto.ICMS.ICMSSN201.vFCPST,col.imposto.ICMS.ICMSSN201.vFCPST,col.imposto.ICMS.ICMSSN900.vFCPST,0)
            ),2),'.'),','),17,'0') AS BASE_IPI,
            LPAD(REPLACE(REPLACE(FORMAT_NUMBER(setTagAvulsa(NFe.infNfe.emit.CPF,NFe.infNfe.emit.IE,col.prod.vProd - NVL(col.prod.vDesc,0) + NVL(col.imposto.IPI.IPITrib.vIPI,0) + NVL(col.prod.vOutro,0) + NVL(col.prod.vFrete,0) + NVL(col.prod.vSeg,0) 
                + coalesce(col.imposto.ICMS.ICMS10.vICMSST,col.imposto.ICMS.ICMS70.vICMSST,col.imposto.ICMS.ICMS30.vICMSST,col.imposto.ICMS.ICMS90.vICMSST,col.imposto.ICMS.ICMSSN201.vICMSST,col.imposto.ICMS.ICMSSN201.vICMSST,col.imposto.ICMS.ICMSSN900.vICMSST,0)
                + coalesce(col.imposto.ICMS.ICMS10.vFCPST,col.imposto.ICMS.ICMS70.vFCPST,col.imposto.ICMS.ICMS30.vFCPST,col.imposto.ICMS.ICMS90.vFCPST,col.imposto.ICMS.ICMSSN201.vFCPST,col.imposto.ICMS.ICMSSN201.vFCPST,col.imposto.ICMS.ICMSSN900.vFCPST,0)
            ),2),'.'),','),17,'0') AS VLR_CONTAB_ITEM,
            '70' AS COD_SITUACAO_PIS,
            '70' AS COD_SITUACAO_COFINS,
            DATE_FORMAT(CURRENT_DATE(),'yyyyMMdd') AS DAT_LANC_PIS_COFINS,
            'N' AS IND_BEM_PATR,
            --col.prod.uCom AS COD_UND_PADRAO,
            NVL(MSAFNCM.cod_und_padrao,'NP') AS COD_UND_PADRAO,
            LPAD(REPLACE(REPLACE(FORMAT_NUMBER(setTagAvulsa(NFe.infNfe.emit.CPF,NFe.infNfe.emit.IE,col.prod.vDesc),2),'.'),','),17,'0') AS VLR_DESCONTO,
            '03' AS COD_TRIB_IPI 
        FROM XML_RAW_ITEM
        LEFT JOIN X04_PESSOA_FIS_JUR X04 ON 1=1
            AND LPAD(NVL(NFe.infNfe.emit.CNPJ,NFe.infNfe.emit.CPF),14,'0') = LPAD(X04.CPF_CGC,14,'0')
        LEFT JOIN ESTABELECIMENTO estab_dest ON 1=1
            AND estab_dest.CGC = XML_RAW_ITEM.NFe.infNfe.dest.CNPJ
            AND estab_dest.COD_ESTAB LIKE 'BR%'
            AND 
                (
                    NFe.infNfe.ide.toma3.toma = '3'
                    OR
                    NFe.infNfe.ide.mod = '55'
                )
        LEFT JOIN ESTABELECIMENTO estab_rem ON 1=1
            AND estab_rem.CGC = XML_RAW_ITEM.NFe.infNfe.rem.CNPJ
            AND estab_rem.COD_ESTAB LIKE 'BR%'
            AND NFe.infNfe.ide.toma3.toma = '0'
        LEFT JOIN ESTABELECIMENTO estab_toma ON 1=1
            AND estab_toma.CGC = XML_RAW_ITEM.NFe.infNfe.toma.CNPJ
            AND estab_toma.COD_ESTAB LIKE 'BR%'
            AND (
                    NFe.infNfe.ide.toma4.toma = '4'
                    AND
                    estab_toma.cod_estab LIKE '33009911%'
                )
        LEFT JOIN ESTABELECIMENTO estab_exped ON 1=1
            AND estab_exped.CGC = XML_RAW_ITEM.NFe.infNfe.exped.CNPJ
            AND estab_exped.COD_ESTAB LIKE 'BR%'
            AND NFe.infNfe.ide.toma3.toma = '1'
        LEFT JOIN ESTABELECIMENTO estab_receb ON 1=1
            AND estab_exped.CGC = XML_RAW_ITEM.NFe.infNfe.receb.CNPJ
            AND estab_exped.COD_ESTAB LIKE 'BR%'
            AND NFe.infNfe.ide.toma3.toma = '2'
        LEFT JOIN MSAFCFOP ON 1=1
            AND XML_RAW_ITEM.col.prod.CFOP = MSAFCFOP.cod_cfo
        LEFT JOIN MSAFCNPJ ON 1=1
            AND NFe.infNfe.emit.CNPJ = MSAFCNPJ.CNPJ
        LEFT JOIN X04_PESSOA_FIS_JUR X04_PARAM ON 1=1
            AND X04_PARAM.COD_FIS_JUR = MSAFCNPJ.COD_FIS_JUR
        LEFT JOIN MSAFNCM ON 1=1
            AND XML_RAW_ITEM.col.prod.NCM = MSAFNCM.cod_ncm
        WHERE 1=1
    UNION ALL
    {}
    """.format(spark_item_cte)

    spark_pessoafisjur = """
        SELECT DISTINCT
            '1' AS IND_FIS_JUR,
            CASE 
                WHEN NFe.infNfe.emit.CNPJ IS NOT NULL THEN
                    'M'||SUBSTR(NFe.infNfe.emit.CNPJ,1,8) || SUBSTR(NFe.infNfe.emit.CNPJ,-4)
                ELSE
                    'M'||SUBSTR(NFe.infNfe.emit.CPF,1,8) || SUBSTR(NFe.infNfe.emit.CPF,-4)
            END AS COD_FIS_JUR,
            DATE_FORMAT(SUBSTR(NFe.infNfe.ide.dhEmi,1,10),'yyyyMMdd') AS DATA_X04,
            '1' AS IND_CONTEM_COD,
            NFe.infNfe.emit.Xnome AS RAZAO_SOCIAL,
            NVL(LPAD(NFe.infNfe.emit.CNPJ,14,'0'),LPAD(NFe.infNfe.emit.CPF,11,'0')) AS CPF_CGC,
            NFe.infNfe.emit.IE AS INSC_ESTADUAL,
            SUBSTR(NFe.infNfe.emit.Xnome,1,50) AS NOME_FANTASIA,
            SUBSTR(NFe.infNfe.emit.enderEmit.xLgr,1,50) AS ENDERECO,
            SUBSTR(NFe.infNfe.emit.enderEmit.nro,1,10) AS NUM_ENDERECO,
            SUBSTR(NFe.infNfe.emit.enderEmit.XBairro,1,20) AS BAIRRO,
            NFe.infNfe.emit.enderEmit.Xmun AS CIDADE,
            NFe.infNfe.emit.enderEmit.UF AS UF,
            NFe.infNfe.emit.enderEmit.CEP AS CEP,
            NVL(SUBSTR(NFe.infNfe.emit.enderEmit.cPais,1,3),'105') AS COD_PAIS,
            SUBSTR(NFe.infNfe.emit.enderEmit.cmun,3,5) AS COD_MUNICIPIO
        FROM XML_RAW_CAPA XRC
        LEFT JOIN X04_PESSOA_FIS_JUR X04 ON 1=1
            AND LPAD(NVL(NFe.infNfe.emit.CNPJ,NFe.infNfe.emit.CPF),14,'0') = LPAD(X04.CPF_CGC,14,'0')
        WHERE 1=1
            AND X04.CPF_CGC IS NULL
    """

    ranked_produto = """
    WITH v1 AS (
      SELECT 
        x2013.*,
        RANK() OVER(PARTITION BY COD_PRODUTO ORDER BY VALID_PRODUTO DESC) AS POSICAO
      FROM MSAF.x2013_produto x2013
      )
      SELECT * FROM v1 WHERE POSICAO = 1
    """

    oracle_param_produto = """
        select distinct 
            det.nome_param,
            det.Conteudo cod_ncm,
            det.descricao,
            det.valor material,
            x2017.cod_und_padrao
        from msaf.fpar_param_det det
        join msaf.fpar_parametros par on 1=1
            and det.id_parametro   = par.id_parametros
        left join ({}) x2013 on 1=1
            and det.valor = x2013.cod_produto
        left join x2017_und_padrao x2017 on 1=1
            and x2013.ident_und_padrao = x2017.ident_und_padrao
        where 1=1
            and par.nome_framework = 'ADEJO_SOUZAC_XML_CPAR'
            and det.nome_param     = 'Produto'
    """.format(ranked_produto)

    

    oracle_param_cfop = """
        select distinct 
            det.nome_param,
            det.Conteudo cod_cfo, 
            det.descricao, 
            SUBSTR(det.valor,1,4) novo_cfo, 
            SUBSTR(det.valor,6,3) novo_natop
        from msaf.fpar_param_det det
        join msaf.fpar_parametros par on 1=1
            and det.id_parametro   = par.id_parametros
        where 1=1
            and par.nome_framework = 'ADEJO_SOUZAC_XML_CPAR'
            and det.nome_param     = 'CFOP NatOp'
    """

    oracle_param_cnpj = """
        select distinct 
            det.nome_param,
            det.Conteudo CNPJ, 
            det.valor cod_fis_jur
        from msaf.fpar_param_det det
        join msaf.fpar_parametros par on 1=1
            and det.id_parametro   = par.id_parametros
        where 1=1
            and par.nome_framework = 'ADEJO_SOUZAC_XML_CPAR'
            and det.nome_param     = 'CNPJ'
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
        DAT_LANC_PIS_COFINS,
        BASE_ISEN_ICMS,
        BASE_OUTR_ICMS,
        BASE_OUTR_IPI,
        COD_MODELO_COTEPE,
        IND_NF_REG_ESPECIAL,
        VLR_DESCONTO,
        VLR_ABAT_NTRIBUTADO,
        IND_TP_FRETE,
        UF_ORIG_DEST,
        UF_DESTINO,
        COD_MUNICIPIO_ORIG,
        COD_MUNICIPIO_DEST,
        COD_NATUREZA_OP
        )
        SELECT * FROM MSAF.SAFX07_TEMP
    """

    oracle_safx08 = """
    INSERT INTO MSAF.SAFX08
        (
        COD_EMPRESA,
        COD_ESTAB,
        DATA_FISCAL,
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
        NUM_DOCFIS_REF,
        SERIE_DOCFIS_REF,
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
        DAT_LANC_PIS_COFINS,
        IND_BEM_PATR,
        COD_UND_PADRAO,
        VLR_DESCONTO,
        COD_TRIB_IPI
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