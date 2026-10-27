package scd2

// >>> SEM FUNÇÃO <<<  (-strategy sem-funcao)
//
// A regra SCD2 fica nas queries sqlc (db/dw/query.sql, seção "SEM FUNÇÃO")
// e é orquestrada aqui, na mesma transação, com três comandos set-based:
//
//	ExpireChanged     (UPDATE ... FROM staging)   fecha versões cujo hash mudou
//	CloseDeleted      (UPDATE ... NOT EXISTS)     fecha e marca chaves que sumiram
//	InsertNewVersions (INSERT ... SELECT)         novas chaves, novas versões e
//	                                              reativações, com as contagens
//
// Contrapartida: 4 idas ao servidor em vez de 1, mas cada comando é tipado
// pelo sqlc, versionado com o Go e roda em qualquer PostgreSQL suportado.

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"scd2/internal/dw"
)

// applyWithoutFunction aplica as regras SCD2 na ordem:
// fechar alterados -> fechar excluídos -> inserir o que ficou sem versão atual.
// A ordem importa: o INSERT depende dos UPDATEs já terem fechado as versões.
func applyWithoutFunction(ctx context.Context, tx pgx.Tx, q *dw.Queries, staged int64) (dw.DwEtlLog, error) {
	// Estatísticas frescas da staging recém-carregada. ANALYZE não é uma
	// query que o sqlc gere, então vai direto pela transação.
	if _, err := tx.Exec(ctx, "ANALYZE dw.stg_sales_territory"); err != nil {
		return dw.DwEtlLog{}, fmt.Errorf("analyze staging: %w", err)
	}

	expiradas, err := q.ExpireChanged(ctx)
	if err != nil {
		return dw.DwEtlLog{}, fmt.Errorf("fechar versões alteradas: %w", err)
	}
	excluidas, err := q.CloseDeleted(ctx)
	if err != nil {
		return dw.DwEtlLog{}, fmt.Errorf("fechar excluídas: %w", err)
	}
	ins, err := q.InsertNewVersions(ctx)
	if err != nil {
		return dw.DwEtlLog{}, fmt.Errorf("inserir versões: %w", err)
	}

	// Invariante: toda versão fechada por mudança ganha exatamente uma nova.
	if int64(ins.NovasVersoes) != expiradas {
		return dw.DwEtlLog{}, fmt.Errorf("inconsistência: %d versões fechadas, %d novas versões",
			expiradas, ins.NovasVersoes)
	}

	logRow, err := q.InsertEtlLog(ctx, dw.InsertEtlLogParams{
		Processo:     ProcessName,
		LinhasOrigem: int32(staged),
		Novas:        ins.Novas,
		NovasVersoes: ins.NovasVersoes,
		Reativadas:   ins.Reativadas,
		Expiradas:    int32(expiradas),
		Excluidas:    int32(excluidas),
	})
	if err != nil {
		return dw.DwEtlLog{}, fmt.Errorf("gravar etl_log: %w", err)
	}
	return logRow, nil
}
