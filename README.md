# scd2-go — SCD Tipo 2 com Go, sqlc e pgx/v5 (PostgreSQL 18)

Carrega `dw.dim_sales_territory` a partir do AdventureWorks mantendo o histórico
completo (Slowly Changing Dimension Tipo 2).

As regras SCD2 têm duas implementações, escolhidas por `-strategy`:

- **`funcao`** (padrão): um MERGE dentro da função PL/pgSQL
  `dw.scd2_merge_sales_territory()`.
- **`sem-funcao`**: três comandos set-based (UPDATE, UPDATE, INSERT) gerados
  pelo sqlc e chamados pelo Go, sem função no banco. Veio do projeto
  `scd2-go-sem-funcao`.

Todo o resto é comum às duas: extração, COPY, staging, guardas, transação e log.

## Estrutura

```
cmd/scd2/main.go                   CLI: flags, pools, migrations embutidas, log
internal/scd2/loader.go            Orquestração comum (transação, pipeline, guardas)
internal/scd2/com_funcao.go        >>> COM FUNÇÃO <<<  chama a função MERGE
internal/scd2/sem_funcao.go        >>> SEM FUNÇÃO <<<  orquestra os 3 comandos sqlc
internal/dw/                       Código gerado pelo sqlc (DW)
internal/source/                   Código gerado pelo sqlc (origem)
db/embed.go                        Migrations embutidas e db.Migrate
db/migrations/001_dw.sql           Dimensão, staging UNLOGGED e etl_log (só DDL, comum)
db/migrations/002_scd2_funcao.sql  >>> COM FUNÇÃO <<<  a função MERGE em PL/pgSQL
db/dw/query.sql                    Queries sqlc do DW, em seções: comum / COM / SEM FUNÇÃO
db/source/query.sql                Query sqlc da origem (keyset)
db/source/schema.sql               Recorte do schema da origem, só para o sqlc
internal/scd2/loader_test.go       Testes de integração, rodados para as duas estratégias
internal/scd2/bench_test.go        Benchmark comparando as estratégias
internal/scd2/testdata/seed.sql    Origem mínima para teste
docs/scd2-comparacao.html          Comparação lado a lado e resultados do benchmark
docs/bench.txt                     Saída bruta do benchmark usado no documento
```

## Como rodar

```bash
go install github.com/sqlc-dev/sqlc/cmd/sqlc@latest
sqlc generate          # regenera internal/dw e internal/source
go mod tidy

docker compose up -d   # PostgreSQL 18 local
go test ./...          # carga inicial, mudança, exclusão, reativação, guardas

# Em produção
export SOURCE_DATABASE_URL=postgres://user:pass@host/adventureworks
export DW_DATABASE_URL=postgres://user:pass@host/dw      # opcional
go run ./cmd/scd2 -migrate -batch 10000 -max-delete-pct 10
go run ./cmd/scd2 -strategy sem-funcao      # mesma carga, sem função no banco
```

| Flag | Padrão | Descrição |
|---|---|---|
| `-source` | `$SOURCE_DATABASE_URL` | DSN da origem |
| `-dw` | `$DW_DATABASE_URL` ou a origem | DSN do DW |
| `-strategy` | `funcao` | `funcao` (MERGE em PL/pgSQL) ou `sem-funcao` (queries sqlc) |
| `-batch` | 5000 | Linhas por lote de leitura/COPY |
| `-max-delete-pct` | 20 | Aborta se mais que esse % das versões atuais fosse excluído (0 desliga) |
| `-migrate` | false | Aplica as migrations (idempotentes) antes da carga |
| `-dry-run` | false | Executa tudo e faz ROLLBACK, para ver as contagens |
| `-timeout` | 30m | Tempo máximo |

Códigos de saída: `0` sucesso, `1` erro, `3` abortado por guarda de segurança.

## Como a carga funciona

Tudo acontece numa única transação no DW:

1. `pg_advisory_xact_lock`: impede duas cargas simultâneas.
2. `TRUNCATE` da staging.
3. **Extração em pipeline:** uma goroutine lê a origem por keyset
   (`WHERE id > último ORDER BY id LIMIT n`) enquanto a principal grava o lote
   anterior com **COPY binário** (`:copyfrom` do sqlc) numa staging `UNLOGGED`.
4. **Guardas:** origem vazia ou exclusões acima de `-max-delete-pct` abortam.
5. **Regras SCD2**, conforme `-strategy` (detalhes na seção abaixo):
   - `funcao`: `dw.scd2_merge_sales_territory()`, um **único MERGE set-based**,
     fecha as versões alteradas, insere novas versões, novas chaves e reativações,
     marca as exclusões e grava as contagens em `dw.etl_log`.
   - `sem-funcao`: `ANALYZE` da staging, `ExpireChanged`, `CloseDeleted`,
     `InsertNewVersions` e `InsertEtlLog`, nessa ordem.
6. `COMMIT`: a restrição "uma versão atual por chave" (`DEFERRABLE`) é checada.

### Por que é rápido

- **COPY** em vez de INSERT linha a linha.
- **Staging `UNLOGGED`**: não gera WAL; o conteúdo é descartável.
- **Hash de linha** (`row_hash`, coluna gerada na staging): a detecção de
  mudança compara uma coluna, não N. Faz diferença em dimensões largas.
- **Índice parcial coberto** `(sales_territory_id) INCLUDE (row_hash) WHERE is_current`,
  que é o próprio índice da restrição de exclusão.
- **`ANALYZE` da staging** antes do MERGE, para o planner conhecer o volume.
- **Uma ida ao servidor** para todo o MERGE, independentemente do número de linhas.
- **Memória limitada** ao tamanho do lote, e leitura e escrita sobrepostas.

### Por que o MERGE fica numa função SQL

O MERGE com `WHEN NOT MATCHED BY SOURCE` e `RETURNING merge_action()` / `new.*`
usa sintaxe recente (PostgreSQL 17/18). Colocá-lo numa função PL/pgSQL faz o
sqlc ver só uma chamada simples (`SELECT dw.scd2_merge_sales_territory($1)`),
sem depender de o analisador do sqlc entender essa sintaxe.

## COM função × SEM função

O documento `docs/scd2-comparacao.html` (abra no navegador) mostra o código
das duas estratégias lado a lado, com gráficos e tabela do benchmark.

| | `-strategy funcao` | `-strategy sem-funcao` |
|---|---|---|
| Onde está a regra SCD2 | `db/migrations/002_scd2_funcao.sql` (PL/pgSQL) | `db/dw/query.sql`, seção SEM FUNÇÃO |
| Código Go | `internal/scd2/com_funcao.go` | `internal/scd2/sem_funcao.go` |
| Comandos SCD2 no servidor | 1 (`SELECT` da função) + leitura do log | 4 (`ANALYZE`, UPDATE, UPDATE, INSERT) + log |
| Duas ações por mudança | Truque do `UNION ALL` na origem do MERGE | Comandos separados, sem truque |
| Contagens | `RETURNING merge_action()` | `RowsAffected` + CTE no INSERT |
| Tipagem pelo sqlc | Só a chamada da função | Cada comando |
| PostgreSQL mínimo | 17 (`WHEN NOT MATCHED BY SOURCE`) | Qualquer versão suportada |
| Restrição "uma versão atual" | Precisa ser `DEFERRABLE` | Não precisaria (fecha antes de inserir) |

As duas usam o mesmo schema. A restrição `EXCLUDE ... DEFERRABLE` atende as
duas. No projeto `scd2-go-sem-funcao` original ela era um índice único parcial
comum, e o benchmark mede essa variante também (`sem-funcao-indice`).

### Cuidado: subconsulta sobre a dimensão que está sendo gravada

As duas versões originais tinham um `EXISTS` correlacionado sobre
`dw.dim_sales_territory` dentro do comando que grava nessa mesma tabela.
Quando as estatísticas dizem que a dimensão está vazia (por exemplo, depois de
`TRUNCATE` seguido de `VACUUM`/autovacuum), o planner executa esse `EXISTS` como
um Seq Scan repetido para cada linha da staging. Cada repetição percorre também
as páginas que o próprio comando acabou de inserir, e o custo fica quadrático:
uma recarga de 1 milhão de linhas passou de 60 s sem terminar (~3 s esperado).

A correção, aplicada às duas estratégias, lê a dimensão uma única vez numa CTE
`MATERIALIZED` (`historico` e `atuais`) antes de gravar.

## Benchmark

```bash
docker compose up -d
go test ./internal/scd2 -run '^$' -bench . -benchtime 5x -count 3 -timeout 90m | tee bench.txt
benchstat -col /estrategia bench.txt      # go install golang.org/x/perf/cmd/benchstat@latest
SCD2_BENCH_ROWS=10000,100000 go test ./internal/scd2 -run '^$' -bench .   # tamanhos menores
```

Cenários: `inicial` (dimensão truncada, todas as linhas novas), `sem-mudancas`
e `incremental` (5% alteradas, 1% excluídas e 1% reativadas por carga). `ns/op`
é a carga inteira. `ms-scd2/op` mede só a parte que muda entre as estratégias.
O preparo e o `VACUUM ANALYZE` de cada iteração ficam fora do cronômetro.

Resultado em Apple M4 Pro, PostgreSQL 18 em Docker (`docs/bench.txt`; tempo só
das regras SCD2, mediana de 3 execuções):

| Linhas | Cenário | funcao | sem-funcao | sem-funcao + índice único |
|---|---|---:|---:|---:|
| 10 mil | inicial | 42,5 ms | 40,5 ms | 36,1 ms |
| 10 mil | sem mudanças | 26,7 ms | 23,2 ms | 23,4 ms |
| 10 mil | incremental | 32,8 ms | 28,5 ms | 27,6 ms |
| 100 mil | inicial | 324 ms | 298 ms | 249 ms |
| 100 mil | sem mudanças | 149 ms | 115 ms | 113 ms |
| 100 mil | incremental | 227 ms | 165 ms | 163 ms |
| 1 milhão | inicial | 2.711 ms | 2.381 ms | 1.838 ms |
| 1 milhão | sem mudanças | 1.248 ms | 955 ms | 874 ms |
| 1 milhão | incremental | 2.746 ms | 2.033 ms | 2.025 ms |

Na média geométrica, `sem-funcao` leva 17% menos tempo nas regras SCD2 e 9%
menos na carga completa.

## Adaptando para outra dimensão

1. Crie a staging e a dimensão com o mesmo padrão (hash gerado, `EXCLUDE ... WHERE is_current DEFERRABLE`).
2. Copie a função MERGE (com função) ou as queries da seção SEM FUNÇÃO,
   trocando as colunas.
3. Adicione as queries (`:copyfrom`, contagens, chamada da função ou passos) e rode `sqlc generate`.
4. Crie o loader reaproveitando `internal/scd2/loader.go`.

## Carga das fatos

Para achar a chave substituta vigente na data do fato use
`GetCurrentVersionAt` ou, em lote:

```sql
SELECT f.salesorderid, d.sales_territory_key
FROM sales.salesorderheader f
JOIN dw.dim_sales_territory d
  ON d.sales_territory_id = f.territoryid
 AND f.orderdate >= d.valid_from
 AND f.orderdate <  d.valid_to;
```
