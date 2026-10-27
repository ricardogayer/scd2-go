package scd2

// >>> COM FUNÇÃO <<<  (-strategy funcao)
//
// A regra SCD2 inteira mora no banco, em dw.scd2_merge_sales_territory()
// (db/migrations/002_scd2_funcao.sql): um único MERGE que fecha versões
// alteradas, insere novas chaves/versões/reativações, marca exclusões e
// grava as contagens em dw.etl_log. O Go faz uma chamada e lê o log.
//
// Contrapartida: o sqlc só enxerga "SELECT função($1)", então o SQL do
// MERGE não é tipado nem versionado junto com o Go, e a sintaxe
// (WHEN NOT MATCHED BY SOURCE, merge_action()) exige PostgreSQL 17+.

import (
	"context"
	"fmt"

	"scd2/internal/dw"
)

func applyWithFunction(ctx context.Context, q *dw.Queries) (dw.DwEtlLog, error) {
	logID, err := q.MergeSalesTerritory(ctx, ProcessName)
	if err != nil {
		return dw.DwEtlLog{}, fmt.Errorf("merge scd2: %w", err)
	}
	logRow, err := q.GetEtlLog(ctx, logID)
	if err != nil {
		return dw.DwEtlLog{}, fmt.Errorf("ler etl_log: %w", err)
	}
	return logRow, nil
}
