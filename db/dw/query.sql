-- =====================================================================
-- COMUM às duas estratégias
-- =====================================================================

-- name: AcquireLoadLock :exec
-- Lock liberado automaticamente no COMMIT/ROLLBACK.
SELECT pg_advisory_xact_lock(sqlc.arg(lock_key)::bigint);

-- name: TruncateStaging :exec
TRUNCATE dw.stg_sales_territory;

-- name: InsertStaging :copyfrom
INSERT INTO dw.stg_sales_territory (sales_territory_id, region, country, "group")
VALUES ($1, $2, $3, $4);

-- name: CountStaging :one
SELECT count(*) FROM dw.stg_sales_territory;

-- name: CountCurrent :one
SELECT count(*) FROM dw.dim_sales_territory WHERE is_current;

-- name: CountToBeDeleted :one
-- Versões atuais que sumiram da origem (serão fechadas como excluídas).
SELECT count(*)
FROM dw.dim_sales_territory d
WHERE d.is_current
  AND NOT EXISTS (SELECT 1 FROM dw.stg_sales_territory s
                  WHERE s.sales_territory_id = d.sales_territory_id);

-- name: GetCurrentVersionAt :one
-- Lookup da chave substituta vigente numa data (para a carga das fatos).
SELECT sales_territory_key
FROM dw.dim_sales_territory
WHERE sales_territory_id = sqlc.arg(sales_territory_id)::int
  AND valid_from <= sqlc.arg(at)::timestamptz
  AND valid_to   >  sqlc.arg(at)::timestamptz;

-- =====================================================================
-- >>> COM FUNÇÃO <<<  (-strategy funcao)
-- A regra SCD2 inteira está em db/migrations/002_scd2_funcao.sql;
-- aqui fica só a chamada. Usado por internal/scd2/com_funcao.go.
-- =====================================================================

-- name: MergeSalesTerritory :one
SELECT dw.scd2_merge_sales_territory(sqlc.arg(processo)::text)::bigint AS log_id;

-- name: GetEtlLog :one
SELECT * FROM dw.etl_log WHERE id = $1;

-- =====================================================================
-- >>> SEM FUNÇÃO <<<  (-strategy sem-funcao)
-- A regra SCD2 em três comandos set-based tipados pelo sqlc, chamados em
-- ordem pelo Go na mesma transação. Usado por internal/scd2/sem_funcao.go.
-- now() devolve o instante de início da transação, então fechamento e
-- abertura de versões usam o mesmo timestamp sem passá-lo como parâmetro.
-- =====================================================================

-- name: ExpireChanged :execrows
-- Passo 1: fecha a versão atual das chaves cujo hash mudou.
UPDATE dw.dim_sales_territory AS d
SET valid_to   = now(),
    is_current = FALSE
FROM dw.stg_sales_territory AS s
WHERE d.sales_territory_id = s.sales_territory_id
  AND d.is_current
  AND d.row_hash <> s.row_hash;

-- name: CloseDeleted :execrows
-- Passo 2: fecha e marca como excluída a versão atual das chaves que
-- sumiram da origem.
UPDATE dw.dim_sales_territory AS d
SET valid_to   = now(),
    is_current = FALSE,
    is_deleted = TRUE
WHERE d.is_current
  AND NOT EXISTS (SELECT 1 FROM dw.stg_sales_territory s
                  WHERE s.sales_territory_id = d.sales_territory_id);

-- name: InsertNewVersions :one
-- Passo 3: insere uma versão atual para toda chave da staging que ficou
-- sem versão atual. Isso cobre de uma vez:
--   - chaves novas          (nunca existiram na dimensão)
--   - novas versões         (acabaram de ser fechadas no passo 1)
--   - reativações           (só têm histórico antigo, excluído antes)
-- atuais e historico leem a dimensão como estava ANTES deste INSERT, e são
-- MATERIALIZED para lê-la uma única vez. Sem isso, quando as estatísticas
-- dizem que a dimensão está vazia (p.ex. após TRUNCATE + VACUUM), o
-- planner escolhe Seq Scan da dimensão repetido por linha da staging, e
-- cada repetição percorre também as páginas que este INSERT acabou de
-- gravar: custo quadrático (1 milhão de linhas: minutos em vez de segundos).
WITH atuais AS MATERIALIZED (
    SELECT sales_territory_id FROM dw.dim_sales_territory WHERE is_current
),
historico AS MATERIALIZED (
    SELECT sales_territory_id,
           bool_or(valid_to = now() AND NOT is_deleted) AS fechada_agora
    FROM dw.dim_sales_territory
    WHERE NOT is_current
    GROUP BY sales_territory_id
),
inseridas AS (
    INSERT INTO dw.dim_sales_territory
           (sales_territory_id, region, country, "group", row_hash, valid_from)
    SELECT s.sales_territory_id, s.region, s.country, s."group", s.row_hash, now()
    FROM dw.stg_sales_territory s
    WHERE NOT EXISTS (SELECT 1 FROM atuais a
                      WHERE a.sales_territory_id = s.sales_territory_id)
    RETURNING sales_territory_id
)
SELECT count(*) FILTER (WHERE h.sales_territory_id IS NULL)::int AS novas,
       count(*) FILTER (WHERE h.fechada_agora)::int              AS novas_versoes,
       count(*) FILTER (WHERE NOT h.fechada_agora)::int          AS reativadas
FROM inseridas i
LEFT JOIN historico h ON h.sales_territory_id = i.sales_territory_id;

-- name: InsertEtlLog :one
INSERT INTO dw.etl_log (processo, inicio, fim, linhas_origem,
                        novas, novas_versoes, reativadas, expiradas, excluidas)
VALUES (sqlc.arg(processo), now(), clock_timestamp(), sqlc.arg(linhas_origem),
        sqlc.arg(novas), sqlc.arg(novas_versoes), sqlc.arg(reativadas),
        sqlc.arg(expiradas), sqlc.arg(excluidas))
RETURNING *;
