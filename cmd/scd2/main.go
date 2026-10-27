// Comando scd2: carrega dw.dim_sales_territory (SCD Tipo 2) a partir do AdventureWorks.
//
// Uso:
//
//	export SOURCE_DATABASE_URL=postgres://user:pass@host:5432/adventureworks
//	export DW_DATABASE_URL=postgres://user:pass@host:5432/dw   # opcional; padrão = origem
//	scd2 -migrate            # cria/atualiza os objetos do DW e roda a carga
//	scd2 -dry-run            # executa e faz ROLLBACK, só para ver as contagens
//	scd2 -strategy sem-funcao  # regras SCD2 como queries sqlc, sem função no banco
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"scd2/db"
	"scd2/internal/scd2"
)

func main() {
	var (
		sourceDSN    = flag.String("source", os.Getenv("SOURCE_DATABASE_URL"), "DSN da origem (AdventureWorks)")
		dwDSN        = flag.String("dw", os.Getenv("DW_DATABASE_URL"), "DSN do DW (padrão: mesma da origem)")
		batchSize    = flag.Int("batch", 5000, "linhas por lote de extração/COPY")
		maxDeletePct = flag.Float64("max-delete-pct", 20, "aborta se mais que este % das versões atuais for excluído (0 desativa)")
		strategy     = flag.String("strategy", string(scd2.StrategyFunction), "regras SCD2: funcao (MERGE em PL/pgSQL) ou sem-funcao (queries sqlc)")
		migrate      = flag.Bool("migrate", false, "aplica as migrations antes da carga")
		dryRun       = flag.Bool("dry-run", false, "executa a carga e faz ROLLBACK")
		timeout      = flag.Duration("timeout", 30*time.Minute, "tempo máximo da execução")
	)
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	if *sourceDSN == "" {
		logger.Error("informe -source ou SOURCE_DATABASE_URL")
		os.Exit(2)
	}
	st, err := scd2.ParseStrategy(*strategy)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(2)
	}
	if *dwDSN == "" {
		*dwDSN = *sourceDSN
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()

	if err := run(ctx, logger, *sourceDSN, *dwDSN, *migrate, scd2.Config{
		Strategy:     st,
		BatchSize:    int32(*batchSize),
		MaxDeletePct: *maxDeletePct,
		DryRun:       *dryRun,
	}); err != nil {
		logger.Error("falha na carga", "erro", err)
		os.Exit(exitCode(err))
	}
}

// exitCode mapeia o erro da carga para o código de saída do processo:
// 0 sucesso, 1 erro, 3 abortado por guarda de segurança.
func exitCode(err error) int {
	switch {
	case err == nil:
		return 0
	case errors.Is(err, scd2.ErrGuard):
		return 3
	default:
		return 1
	}
}

func run(ctx context.Context, logger *slog.Logger, sourceDSN, dwDSN string, migrate bool, cfg scd2.Config) error {
	srcPool, err := pgxpool.New(ctx, sourceDSN)
	if err != nil {
		return fmt.Errorf("conectar origem: %w", err)
	}
	defer srcPool.Close()

	dwPool := srcPool
	if dwDSN != sourceDSN {
		dwPool, err = pgxpool.New(ctx, dwDSN)
		if err != nil {
			return fmt.Errorf("conectar DW: %w", err)
		}
		defer dwPool.Close()
	}

	if migrate {
		files, err := db.Migrate(ctx, dwPool)
		for _, f := range files {
			logger.Info("migration aplicada", "arquivo", f)
		}
		if err != nil {
			return err
		}
	}

	start := time.Now()
	res, err := scd2.NewLoader(srcPool, dwPool, cfg).Run(ctx)
	if err != nil {
		return err
	}

	l := res.Log
	logger.Info("carga concluída",
		"processo", l.Processo,
		"estrategia", res.Strategy,
		"dry_run", res.DryRun,
		"linhas_origem", l.LinhasOrigem,
		"novas", l.Novas,
		"novas_versoes", l.NovasVersoes,
		"reativadas", l.Reativadas,
		"expiradas", l.Expiradas,
		"excluidas", l.Excluidas,
		"duracao", time.Since(start).Round(time.Millisecond),
		"duracao_scd2", res.SCD2Duration.Round(time.Millisecond),
	)
	return nil
}
