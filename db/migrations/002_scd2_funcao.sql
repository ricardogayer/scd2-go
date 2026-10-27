/* =====================================================================
   >>> COM FUNÇÃO <<<  (estratégia -strategy funcao)
   Toda a regra SCD2 numa função PL/pgSQL; o Go só faz
   SELECT dw.scd2_merge_sales_territory($1).
   A alternativa sem função está em db/dw/query.sql, seção "SEM FUNÇÃO".
   ===================================================================== */

-- ---------------------------------------------------------------------
-- MERGE SCD2 (set-based, uma passada)
--
-- O MERGE aplica uma ação por linha de origem, mas uma mudança SCD2
-- exige duas (fechar a versão atual + inserir a nova). Por isso a
-- origem é a staging duplicada para as linhas alteradas:
--   Ramo A: todas as linhas, merge_key preenchida -> fecha se mudou /
--           insere se a chave não tem versão atual (nova ou reativada)
--   Ramo B: só as alteradas, merge_key NULL -> nunca casa -> insere
--           a nova versão
-- Retorna o id gravado em dw.etl_log.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dw.scd2_merge_sales_territory(p_processo TEXT)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    v_ts     TIMESTAMPTZ := now();  -- instante da transação: fecha e abre versões no mesmo ponto
    v_origem INT;
    v_id     BIGINT;
BEGIN
    -- Estatísticas frescas da staging recém-carregada para o planner
    ANALYZE dw.stg_sales_territory;

    SELECT count(*) INTO v_origem FROM dw.stg_sales_territory;
    IF v_origem = 0 THEN
        RAISE EXCEPTION 'staging vazia: carga abortada para não fechar a dimensão inteira';
    END IF;

    -- historico: chaves que já existiam na dimensão, lidas UMA vez.
    -- MATERIALIZED é necessário: um EXISTS correlacionado sobre a dimensão
    -- pode virar Seq Scan repetido por linha (quando as estatísticas dizem
    -- que ela está vazia, p.ex. após TRUNCATE + VACUUM), e cada repetição
    -- percorre também as páginas que o próprio MERGE acabou de inserir:
    -- custo quadrático (1 milhão de linhas: minutos em vez de segundos).
    WITH historico AS MATERIALIZED (
        SELECT DISTINCT sales_territory_id FROM dw.dim_sales_territory
    ),
    resultado AS (
        MERGE INTO dw.dim_sales_territory AS t
        USING (
            SELECT s.sales_territory_id AS merge_key,
                   s.sales_territory_id, s.region, s.country, s."group", s.row_hash,
                   CASE WHEN h.sales_territory_id IS NOT NULL
                        THEN 'REATIVADA' ELSE 'NOVA' END AS tipo
            FROM dw.stg_sales_territory s
            LEFT JOIN historico h ON h.sales_territory_id = s.sales_territory_id

            UNION ALL

            SELECT NULL::INT,
                   s.sales_territory_id, s.region, s.country, s."group", s.row_hash,
                   'NOVA_VERSAO'
            FROM dw.stg_sales_territory s
            JOIN dw.dim_sales_territory d
              ON d.sales_territory_id = s.sales_territory_id
             AND d.is_current
            WHERE d.row_hash <> s.row_hash
        ) AS s
            ON  t.sales_territory_id = s.merge_key
            AND t.is_current

        -- Mudou: fecha a versão atual
        WHEN MATCHED AND t.row_hash <> s.row_hash THEN
            UPDATE SET valid_to   = v_ts,
                       is_current = FALSE

        -- Nova chave, reativação ou nova versão
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (sales_territory_id, region, country, "group", row_hash,
                    valid_from, is_current, is_deleted)
            VALUES (s.sales_territory_id, s.region, s.country, s."group", s.row_hash,
                    v_ts, TRUE, FALSE)

        -- Sumiu da origem: fecha e marca exclusão (só a versão atual)
        WHEN NOT MATCHED BY SOURCE AND t.is_current THEN
            UPDATE SET valid_to   = v_ts,
                       is_current = FALSE,
                       is_deleted = TRUE

        RETURNING merge_action() AS acao,
                  s.tipo         AS tipo,
                  new.is_deleted AS excluida
    )
    INSERT INTO dw.etl_log (processo, inicio, fim, linhas_origem,
                            novas, novas_versoes, reativadas, expiradas, excluidas)
    SELECT p_processo, v_ts, clock_timestamp(), v_origem,
           count(*) FILTER (WHERE acao = 'INSERT' AND tipo = 'NOVA'),
           count(*) FILTER (WHERE acao = 'INSERT' AND tipo = 'NOVA_VERSAO'),
           count(*) FILTER (WHERE acao = 'INSERT' AND tipo = 'REATIVADA'),
           count(*) FILTER (WHERE acao = 'UPDATE' AND NOT excluida),
           count(*) FILTER (WHERE acao = 'UPDATE' AND excluida)
    FROM resultado
    RETURNING id INTO v_id;

    RETURN v_id;
END;
$$;
