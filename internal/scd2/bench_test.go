package scd2_test

// Benchmark das duas estratégias SCD2 sobre uma origem sintética.
//
//	docker compose up -d
//	go test ./internal/scd2 -run '^$' -bench . -benchtime 5x -count 3 -timeout 60m
//
// Variantes de estratégia:
//
//	funcao            MERGE em PL/pgSQL (restrição EXCLUDE DEFERRABLE)
//	sem-funcao        queries sqlc, no mesmo schema (EXCLUDE DEFERRABLE)
//	sem-funcao-indice queries sqlc com o índice único parcial comum, como
//	                  no projeto scd2-go-sem-funcao original
//
// Cenários:
//
//	inicial      dimensão vazia: todas as linhas são novas
//	sem-mudancas reexecução sem nenhuma diferença na origem
//	incremental  por carga: 5% alteradas, 1% excluídas, 1% reativadas
//
// Métricas: ns/op é a carga completa (extração + COPY + SCD2 + COMMIT);
// ms-scd2/op isola a parte que muda entre as estratégias.

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"scd2/internal/scd2"
)

type benchVariant struct {
	name     string
	strategy scd2.Strategy
	setup    string // SQL extra aplicado depois das migrations
}

var benchVariants = []benchVariant{
	{"funcao", scd2.StrategyFunction, ""},
	{"sem-funcao", scd2.StrategySteps, ""},
	{"sem-funcao-indice", scd2.StrategySteps, `
		ALTER TABLE dw.dim_sales_territory DROP CONSTRAINT ex_dim_st_uma_versao_atual;
		CREATE UNIQUE INDEX ux_dim_st_versao_atual
		    ON dw.dim_sales_territory (sales_territory_id)
		    INCLUDE (row_hash)
		    WHERE is_current;`},
}

// benchSizes pode ser trocado por SCD2_BENCH_ROWS=10000,100000.
func benchSizes(b *testing.B) []int {
	env := os.Getenv("SCD2_BENCH_ROWS")
	if env == "" {
		return []int{10_000, 100_000, 1_000_000}
	}
	var sizes []int
	for _, f := range strings.Split(env, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(f))
		if err != nil {
			b.Fatalf("SCD2_BENCH_ROWS: %v", err)
		}
		sizes = append(sizes, n)
	}
	return sizes
}

// seedSource troca a origem de 10 linhas do seed por n linhas sintéticas,
// com bench_deleted guardando as linhas excluídas para reativá-las depois.
func seedSource(b *testing.B, pool *pgxpool.Pool, n int) {
	execSQL(b, pool, fmt.Sprintf(`
		TRUNCATE sales.salesterritory;
		INSERT INTO sales.salesterritory (territoryid, name, countryregioncode, "group")
		SELECT i, 'Territory ' || i,
		       (ARRAY['US','CA','FR','DE','AU','GB'])[1 + i %% 6],
		       (ARRAY['North America','Europe','Pacific'])[1 + i %% 3]
		FROM generate_series(1, %d) AS i;
		CREATE TABLE bench_deleted (LIKE sales.salesterritory);`, n))
	execSQL(b, pool, "VACUUM ANALYZE sales.salesterritory")
}

func BenchmarkLoad(b *testing.B) {
	for _, n := range benchSizes(b) {
		b.Run(fmt.Sprintf("linhas=%d", n), func(b *testing.B) {
			for _, v := range benchVariants {
				b.Run("estrategia="+v.name, func(b *testing.B) {
					// Esta função roda uma vez (só tem sub-benchmarks); o banco
					// é apagado quando todos os cenários da variante terminam.
					pool := newTestDB(b)
					if v.setup != "" {
						execSQL(b, pool, v.setup)
					}
					seedSource(b, pool, n)
					cfg := scd2.Config{Strategy: v.strategy, BatchSize: 10_000, MaxDeletePct: 20}

					b.Run("cenario=inicial", func(b *testing.B) {
						benchLoop(b, pool, cfg, func(int) counts {
							execSQL(b, pool, "TRUNCATE dw.dim_sales_territory, dw.etl_log")
							return counts{Origem: int32(n), Novas: int32(n)}
						})
					})

					b.Run("cenario=sem-mudancas", func(b *testing.B) {
						benchLoop(b, pool, cfg, func(int) counts {
							return counts{Origem: int32(n)}
						})
					})

					// k persiste entre as chamadas do b.Run: cada carga exclui um
					// resto (id % 100) diferente e reativa o da carga anterior.
					k := 0
					b.Run("cenario=incremental", func(b *testing.B) {
						benchLoop(b, pool, cfg, func(int) counts {
							k++
							del := k % 100
							reativar := queryInt(b, pool, "SELECT count(*) FROM bench_deleted")
							execSQL(b, pool, fmt.Sprintf(`
								INSERT INTO sales.salesterritory SELECT * FROM bench_deleted;
								TRUNCATE bench_deleted;
								WITH d AS (DELETE FROM sales.salesterritory
								           WHERE territoryid %% 100 = %d RETURNING *)
								INSERT INTO bench_deleted SELECT * FROM d;
								UPDATE sales.salesterritory
								SET name = CASE WHEN name LIKE '%%*' THEN rtrim(name, '*') ELSE name || '*' END
								WHERE (territoryid - %d + 100) %% 100 BETWEEN 1 AND 5;`, del, del))
							excluidas := queryInt(b, pool, "SELECT count(*) FROM bench_deleted")
							alteradas := 5 * n / 100
							return counts{
								Origem:       int32(n - excluidas),
								NovasVersoes: int32(alteradas),
								Expiradas:    int32(alteradas),
								Reativadas:   int32(reativar),
								Excluidas:    int32(excluidas),
							}
						})
					})
				})
			}
		})
	}
}

// benchLoop prepara cada iteração fora do cronômetro (prepare + VACUUM
// ANALYZE, para as duas estratégias partirem do mesmo estado físico) e
// mede só a carga.
func benchLoop(b *testing.B, pool *pgxpool.Pool, cfg scd2.Config, prepare func(i int) counts) {
	if queryInt(b, pool, "SELECT count(*) FROM dw.dim_sales_territory") == 0 {
		if _, err := load(b, pool, cfg); err != nil { // estado inicial carregado
			b.Fatal(err)
		}
	}
	var scd2Total time.Duration
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		want := prepare(i)
		// Um VACUUM por chamada: vários comandos numa mensagem viram transação.
		execSQL(b, pool, "VACUUM ANALYZE dw.dim_sales_territory")
		execSQL(b, pool, "VACUUM ANALYZE sales.salesterritory")
		b.StartTimer()

		res := mustLoad(b, pool, cfg, want)
		scd2Total += res.SCD2Duration
	}
	b.ReportMetric(float64(scd2Total.Microseconds())/1000/float64(b.N), "ms-scd2/op")
}
