CREATE OR REPLACE VIEW CNPJ_X04 AS
SELECT DISTINCT CPF_CGC, CPF_CGC  
    FROM (
            WITH 
                v1 AS (
                        SELECT 
                            x04.*,
                            RANK() OVER(
                                        PARTITION BY CPF_CGC ORDER BY VALID_FIS_JUR DESC,IND_FIS_JUR, IDENT_FIS_JUR DESC
                                        ) AS POSICAO 
                        FROM MSAF.X04_PESSOA_FIS_JUR x04
                    ) SELECT * FROM v1 WHERE POSICAO = 1
            );