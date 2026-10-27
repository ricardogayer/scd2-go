/* =====================================================================
   DW - SCD Tipo 2 para dim_sales_territory (PostgreSQL 18)

   Só DDL, compartilhada pelas duas estratégias de carga:
     - COM função: 002_scd2_funcao.sql (MERGE em PL/pgSQL)
     - SEM função: db/dw/query.sql, seção "SEM FUNÇÃO" (comandos sqlc)

   Fluxo de carga (orquestrado pelo programa Go, numa única transação):
     1. pg_advisory_xact_lock      -> impede duas cargas simultâneas
     2. TRUNCATE staging
     3. COPY da origem para staging (em lotes, protocolo binário do pgx)
     4. Guardas (origem vazia, % de exclusões)
     5. Regras SCD2, conforme -strategy:
          funcao     -> dw.scd2_merge_sales_territory() (um MERGE)
          sem-funcao -> ExpireChanged, CloseDeleted, InsertNewVersions
     6. COMMIT (a restrição de "uma versão atual" é checada aqui)
   ===================================================================== */

CREATE SCHEMA IF NOT EXISTS dw;

-- ---------------------------------------------------------------------
-- Staging: UNLOGGED (não gera WAL, carga mais rápida). O conteúdo é
-- descartável: é truncado a cada execução.
-- row_hash é calculado pelo próprio banco na chegada da linha, então a
-- detecção de mudança compara 1 coluna em vez de N.
-- ---------------------------------------------------------------------
CREATE UNLOGGED TABLE IF NOT EXISTS dw.stg_sales_territory (
    sales_territory_id INT         NOT NULL PRIMARY KEY, -- PK: chave duplicada falha no COPY
    region             VARCHAR(50),
    country            VARCHAR(50),
    "group"            VARCHAR(50),
    row_hash           BYTEA       NOT NULL GENERATED ALWAYS AS (
        decode(md5(
            coalesce(region,  E'\\N') || E'\x1f' ||
            coalesce(country, E'\\N') || E'\x1f' ||
            coalesce("group", E'\\N')
        ), 'hex')
    ) STORED
);

-- ---------------------------------------------------------------------
-- Dimensão SCD2
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.dim_sales_territory (
    sales_territory_key BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- chave substituta
    sales_territory_id  INT         NOT NULL,                                 -- chave natural
    region              VARCHAR(50),
    country             VARCHAR(50),
    "group"             VARCHAR(50),
    row_hash            BYTEA       NOT NULL,
    valid_from          TIMESTAMPTZ NOT NULL,
    valid_to            TIMESTAMPTZ NOT NULL DEFAULT '9999-12-31 00:00:00+00',
    is_current          BOOLEAN     NOT NULL DEFAULT TRUE,
    is_deleted          BOOLEAN     NOT NULL DEFAULT FALSE,

    CONSTRAINT ck_dim_st_vigencia CHECK (valid_to > valid_from),

    -- No máximo UMA versão atual por chave natural.
    -- DEFERRABLE: dentro do MERGE o INSERT da nova versão pode ser
    -- processado antes do UPDATE que fecha a antiga; checa no COMMIT.
    -- A estratégia sem função não precisaria disso (fecha antes de
    -- inserir), mas funciona igual com a restrição adiada.
    -- O INCLUDE (row_hash) deixa o índice cobrir a comparação de hash.
    CONSTRAINT ex_dim_st_uma_versao_atual
        EXCLUDE USING btree (sales_territory_id WITH =)
        INCLUDE (row_hash)
        WHERE (is_current)
        DEFERRABLE INITIALLY DEFERRED
);

-- Lookup da versão vigente numa data (carga das fatos)
CREATE INDEX IF NOT EXISTS ix_dim_st_nk_vigencia
    ON dw.dim_sales_territory (sales_territory_id, valid_from)
    INCLUDE (valid_to, sales_territory_key);

-- ---------------------------------------------------------------------
-- Log das cargas
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dw.etl_log (
    id            BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    processo      TEXT        NOT NULL,
    inicio        TIMESTAMPTZ NOT NULL,
    fim           TIMESTAMPTZ NOT NULL,
    linhas_origem INT         NOT NULL,
    novas         INT         NOT NULL,
    novas_versoes INT         NOT NULL,
    reativadas    INT         NOT NULL,
    expiradas     INT         NOT NULL,
    excluidas     INT         NOT NULL
);
