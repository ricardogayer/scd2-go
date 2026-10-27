// Package db embute as migrations SQL no binário e sabe aplicá-las.
package db

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var Migrations embed.FS

// Migrate executa os arquivos .sql embutidos, em ordem de nome, e devolve
// os arquivos aplicados. Usa o protocolo simples (PgConn.Exec), que aceita
// vários comandos por arquivo; os scripts são idempotentes
// (IF NOT EXISTS / OR REPLACE).
func Migrate(ctx context.Context, pool *pgxpool.Pool) ([]string, error) {
	files, err := fs.Glob(Migrations, "migrations/*.sql")
	if err != nil {
		return nil, err
	}
	sort.Strings(files)

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("migrations: %w", err)
	}
	defer conn.Release()

	for i, f := range files {
		sql, err := fs.ReadFile(Migrations, f)
		if err != nil {
			return files[:i], err
		}
		if _, err := conn.Conn().PgConn().Exec(ctx, string(sql)).ReadAll(); err != nil {
			return files[:i], fmt.Errorf("migration %s: %w", f, err)
		}
	}
	return files, nil
}
