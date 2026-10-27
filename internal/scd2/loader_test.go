package scd2_test

// Testes de integração: precisam de um PostgreSQL 18 (docker compose up -d).
// Cada teste cria um banco próprio a partir de TEST_DATABASE_URL e o apaga
// no fim; sem servidor acessível, os testes são pulados.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"scd2/db"
	"scd2/internal/dw"
	"scd2/internal/scd2"
)

const defaultTestDSN = "postgres://postgres:postgres@localhost:5432/postgres"

// counts são as contagens esperadas em dw.etl_log para uma carga.
type counts struct {
	Origem, Novas, NovasVersoes, Reativadas, Expiradas, Excluidas int32
}

func countsOf(l dw.DwEtlLog) counts {
	return counts{l.LinhasOrigem, l.Novas, l.NovasVersoes, l.Reativadas, l.Expiradas, l.Excluidas}
}

// newTestDB cria um banco vazio, aplica o seed da origem e as migrations do DW.
func newTestDB(t testing.TB) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		dsn = defaultTestDSN
	}
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Skipf("PostgreSQL indisponível em %s: %v", dsn, err)
	}
	defer admin.Close(ctx)

	name := fmt.Sprintf("scd2_test_%s_%d",
		strings.ToLower(strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())), time.Now().UnixNano())
	if len(name) > 63 {
		name = name[len(name)-63:]
	}
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatalf("criar banco de teste: %v", err)
	}

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		c, err := pgx.Connect(ctx, dsn)
		if err != nil {
			t.Logf("apagar banco de teste: %v", err)
			return
		}
		defer c.Close(ctx)
		if _, err := c.Exec(ctx, "DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Logf("apagar banco de teste: %v", err)
		}
	})

	seed, err := os.ReadFile("testdata/seed.sql")
	if err != nil {
		t.Fatal(err)
	}
	execSQL(t, pool, string(seed))
	if _, err := db.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrations: %v", err)
	}
	return pool
}

// execSQL roda um ou mais comandos pelo protocolo simples.
func execSQL(t testing.TB, pool *pgxpool.Pool, sql string) {
	t.Helper()
	ctx := context.Background()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if _, err := conn.Conn().PgConn().Exec(ctx, sql).ReadAll(); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

func load(t testing.TB, pool *pgxpool.Pool, cfg scd2.Config) (scd2.Result, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	return scd2.NewLoader(pool, pool, cfg).Run(ctx)
}

func mustLoad(t testing.TB, pool *pgxpool.Pool, cfg scd2.Config, want counts) scd2.Result {
	t.Helper()
	res, err := load(t, pool, cfg)
	if err != nil {
		t.Fatalf("carga: %v", err)
	}
	if got := countsOf(res.Log); got != want {
		t.Fatalf("contagens = %+v, esperado %+v", got, want)
	}
	if res.Staged != int64(want.Origem) {
		t.Fatalf("staged = %d, esperado %d", res.Staged, want.Origem)
	}
	return res
}

func queryInt(t testing.TB, pool *pgxpool.Pool, sql string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), sql).Scan(&n); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}

var strategies = []scd2.Strategy{scd2.StrategyFunction, scd2.StrategySteps}

// forEachStrategy roda o mesmo teste contra as duas implementações das
// regras SCD2, cada uma num banco próprio: os resultados têm de ser iguais.
func forEachStrategy(t *testing.T, test func(t *testing.T, pool *pgxpool.Pool, st scd2.Strategy)) {
	for _, st := range strategies {
		t.Run(string(st), func(t *testing.T) {
			test(t, newTestDB(t), st)
		})
	}
}

// TestScenario reproduz o ciclo de vida completo da dimensão; os passos
// dependem uns dos outros e rodam em ordem sobre o mesmo banco.
func TestScenario(t *testing.T) {
	forEachStrategy(t, func(t *testing.T, pool *pgxpool.Pool, st scd2.Strategy) {

		steps := []struct {
			name   string
			before string
			want   counts
		}{
			{"carga inicial", "", counts{Origem: 10, Novas: 10}},
			{"reexecução sem mudanças", "", counts{Origem: 10}},
			{"mudança no id 1",
				"UPDATE sales.salesterritory SET name = 'Pacific Northwest' WHERE territoryid = 1",
				counts{Origem: 10, NovasVersoes: 1, Expiradas: 1}},
			{"id 10 some da origem",
				"DELETE FROM sales.salesterritory WHERE territoryid = 10",
				counts{Origem: 9, Excluidas: 1}},
			{"id 10 volta",
				"INSERT INTO sales.salesterritory VALUES (10, 'United Kingdom', 'GB', 'Europe')",
				counts{Origem: 10, Reativadas: 1}},
			{"mudança via país no id 7",
				"UPDATE person.countryregion SET name = 'République française' WHERE countryregioncode = 'FR'",
				counts{Origem: 10, NovasVersoes: 1, Expiradas: 1}},
		}
		for _, s := range steps {
			if !t.Run(s.name, func(t *testing.T) {
				if s.before != "" {
					execSQL(t, pool, s.before)
				}
				mustLoad(t, pool, scd2.Config{Strategy: st, MaxDeletePct: 20}, s.want)
			}) {
				t.FailNow()
			}
		}

		t.Run("histórico", func(t *testing.T) {
			type version struct {
				ID               int32
				Region, Country  string
				Current, Deleted bool
			}
			want := []version{
				{1, "Northwest", "United States", false, false},
				{1, "Pacific Northwest", "United States", true, false},
				{7, "France", "France", false, false},
				{7, "France", "République française", true, false},
				{10, "United Kingdom", "United Kingdom", false, true},
				{10, "United Kingdom", "United Kingdom", true, false},
			}

			rows, err := pool.Query(context.Background(), `
				SELECT sales_territory_id, region, country, is_current, is_deleted, valid_from, valid_to
				FROM dw.dim_sales_territory
				WHERE sales_territory_id IN (1, 7, 10)
				ORDER BY sales_territory_id, valid_from`)
			if err != nil {
				t.Fatal(err)
			}
			var (
				got      []version
				prevID   int32
				prevTo   time.Time
				infinity = time.Date(9000, 1, 1, 0, 0, 0, 0, time.UTC)
			)
			for rows.Next() {
				var v version
				var from, to time.Time
				if err := rows.Scan(&v.ID, &v.Region, &v.Country, &v.Current, &v.Deleted, &from, &to); err != nil {
					t.Fatal(err)
				}
				if v.Current != to.After(infinity) {
					t.Errorf("id %d desde %v: is_current=%v mas valid_to=%v", v.ID, from, v.Current, to)
				}
				if v.ID == prevID && from.Before(prevTo) {
					t.Errorf("id %d: versões sobrepostas (%v < %v)", v.ID, from, prevTo)
				}
				prevID, prevTo = v.ID, to
				got = append(got, v)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if fmt.Sprint(got) != fmt.Sprint(want) {
				t.Fatalf("histórico\n got: %+v\nwant: %+v", got, want)
			}
		})

		t.Run("etl_log tem uma linha por carga", func(t *testing.T) {
			if n := queryInt(t, pool, "SELECT count(*) FROM dw.etl_log"); n != len(steps) {
				t.Fatalf("etl_log tem %d linhas, esperado %d", n, len(steps))
			}
		})
	})
}

func TestGuardMassDelete(t *testing.T) {
	forEachStrategy(t, func(t *testing.T, pool *pgxpool.Pool, st scd2.Strategy) {
		mustLoad(t, pool, scd2.Config{Strategy: st, MaxDeletePct: 20}, counts{Origem: 10, Novas: 10})
		execSQL(t, pool, "DELETE FROM sales.salesterritory WHERE territoryid > 2")

		_, err := load(t, pool, scd2.Config{Strategy: st, MaxDeletePct: 20, DryRun: true})
		if !errors.Is(err, scd2.ErrGuard) {
			t.Fatalf("erro = %v, esperado ErrGuard", err)
		}
		if !strings.Contains(err.Error(), "8 de 10") {
			t.Errorf("mensagem sem as contagens: %v", err)
		}
		if n := queryInt(t, pool, "SELECT count(*) FROM dw.dim_sales_territory WHERE is_current AND NOT is_deleted"); n != 10 {
			t.Errorf("versões atuais = %d após guarda, esperado 10", n)
		}

		// Com a guarda desligada a mesma carga passa.
		mustLoad(t, pool, scd2.Config{Strategy: st, MaxDeletePct: 0}, counts{Origem: 2, Excluidas: 8})
	})
}

func TestGuardEmptySource(t *testing.T) {
	forEachStrategy(t, func(t *testing.T, pool *pgxpool.Pool, st scd2.Strategy) {
		execSQL(t, pool, "DELETE FROM sales.salesterritory")

		_, err := load(t, pool, scd2.Config{Strategy: st, MaxDeletePct: 0})
		if !errors.Is(err, scd2.ErrGuard) {
			t.Fatalf("erro = %v, esperado ErrGuard", err)
		}
	})
}

func TestDryRunRollsBack(t *testing.T) {
	forEachStrategy(t, func(t *testing.T, pool *pgxpool.Pool, st scd2.Strategy) {

		res := mustLoad(t, pool, scd2.Config{Strategy: st, DryRun: true}, counts{Origem: 10, Novas: 10})
		if !res.DryRun {
			t.Error("Result.DryRun = false")
		}
		for _, table := range []string{"dw.dim_sales_territory", "dw.etl_log"} {
			if n := queryInt(t, pool, "SELECT count(*) FROM "+table); n != 0 {
				t.Errorf("%s tem %d linhas após dry-run, esperado 0", table, n)
			}
		}
	})
}

// TestBatchSizes exercita a paginação por keyset e o pipeline com lotes
// menores, iguais e maiores que a origem.
func TestBatchSizes(t *testing.T) {
	forEachStrategy(t, func(t *testing.T, pool *pgxpool.Pool, st scd2.Strategy) {
		for _, size := range []int32{1, 3, 10, 11} {
			t.Run(fmt.Sprint(size), func(t *testing.T) {
				mustLoad(t, pool, scd2.Config{Strategy: st, BatchSize: size, DryRun: true}, counts{Origem: 10, Novas: 10})
			})
		}
	})
}

func TestMigrateIsIdempotent(t *testing.T) {
	// newTestDB já aplicou as migrations uma vez.
	forEachStrategy(t, func(t *testing.T, pool *pgxpool.Pool, st scd2.Strategy) {
		mustLoad(t, pool, scd2.Config{Strategy: st, MaxDeletePct: 20}, counts{Origem: 10, Novas: 10})
		if _, err := db.Migrate(context.Background(), pool); err != nil {
			t.Fatalf("segunda aplicação: %v", err)
		}
		mustLoad(t, pool, scd2.Config{Strategy: st, MaxDeletePct: 20}, counts{Origem: 10})
	})
}

func TestParseStrategy(t *testing.T) {
	for _, st := range strategies {
		if got, err := scd2.ParseStrategy(string(st)); err != nil || got != st {
			t.Errorf("ParseStrategy(%q) = %q, %v", st, got, err)
		}
	}
	if _, err := scd2.ParseStrategy("procedure"); err == nil {
		t.Error("ParseStrategy aceitou um nome inválido")
	}
}
