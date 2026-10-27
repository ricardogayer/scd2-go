// Package scd2 orquestra a carga SCD Tipo 2 da dimensão de territórios.
//
// Otimizações:
//   - Extração por keyset em lotes, com memória limitada ao tamanho do lote.
//   - Pipeline: enquanto um lote é gravado via COPY, o próximo já está
//     sendo lido da origem (goroutine produtora + canal com buffer).
//   - COPY (protocolo binário do pgx) para uma staging UNLOGGED.
//   - Detecção de mudança por hash calculado no banco (1 comparação por linha).
//   - Regras SCD2 set-based no servidor, sem idas e voltas por linha, em
//     uma de duas estratégias (Config.Strategy):
//     COM função (com_funcao.go): um MERGE dentro de uma função PL/pgSQL.
//     SEM função (sem_funcao.go): UPDATE, UPDATE e INSERT gerados pelo sqlc.
//   - Tudo numa transação: falha em qualquer etapa não deixa a dimensão
//     pela metade; um advisory lock impede cargas simultâneas.
package scd2

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"scd2/internal/dw"
	"scd2/internal/source"
)

// loadLockKey identifica esta carga no pg_advisory_xact_lock.
const loadLockKey int64 = 0x5CD2_0001

// ProcessName é gravado em dw.etl_log.processo.
const ProcessName = "dim_sales_territory"

// Strategy escolhe onde ficam as regras SCD2.
type Strategy string

const (
	// StrategyFunction chama dw.scd2_merge_sales_territory() (MERGE em PL/pgSQL).
	StrategyFunction Strategy = "funcao"
	// StrategySteps executa os passos SCD2 como queries sqlc, sem função no banco.
	StrategySteps Strategy = "sem-funcao"
)

// ParseStrategy valida o nome vindo da linha de comando.
func ParseStrategy(s string) (Strategy, error) {
	switch st := Strategy(s); st {
	case StrategyFunction, StrategySteps:
		return st, nil
	}
	return "", fmt.Errorf("estratégia desconhecida %q (use %q ou %q)", s, StrategyFunction, StrategySteps)
}

// Config controla o comportamento da carga.
type Config struct {
	// Strategy escolhe a implementação das regras SCD2 (padrão: StrategyFunction).
	Strategy Strategy
	// BatchSize é o número de linhas por lote de extração/COPY.
	BatchSize int32
	// MaxDeletePct aborta a carga se mais que esse percentual das versões
	// atuais fosse marcado como excluído (proteção contra origem parcial).
	// Valor <= 0 desativa a checagem.
	MaxDeletePct float64
	// DryRun executa tudo e faz ROLLBACK no final, para conferir as contagens.
	DryRun bool
}

// Loader executa a carga lendo de src e gravando em dwPool.
// Os dois pools podem apontar para o mesmo banco.
type Loader struct {
	src *pgxpool.Pool
	dw  *pgxpool.Pool
	cfg Config
}

func NewLoader(src, dwPool *pgxpool.Pool, cfg Config) *Loader {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 5000
	}
	if cfg.Strategy == "" {
		cfg.Strategy = StrategyFunction
	}
	return &Loader{src: src, dw: dwPool, cfg: cfg}
}

// Result traz o registro gravado em dw.etl_log.
type Result struct {
	Log      dw.DwEtlLog
	Staged   int64
	DryRun   bool
	Strategy Strategy
	// SCD2Duration é o tempo só da aplicação das regras SCD2 (a parte que
	// muda entre as estratégias), sem extração nem COPY.
	SCD2Duration time.Duration
}

// ErrGuard indica que uma checagem de segurança abortou a carga.
var ErrGuard = errors.New("carga abortada por checagem de segurança")

func (l *Loader) Run(ctx context.Context) (Result, error) {
	tx, err := l.dw.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Result{}, fmt.Errorf("iniciar transação: %w", err)
	}
	// Rollback após Commit é no-op; garante limpeza em qualquer retorno antecipado.
	defer tx.Rollback(context.WithoutCancel(ctx))

	q := dw.New(tx)

	if err := q.AcquireLoadLock(ctx, loadLockKey); err != nil {
		return Result{}, fmt.Errorf("advisory lock: %w", err)
	}
	if err := q.TruncateStaging(ctx); err != nil {
		return Result{}, fmt.Errorf("truncate staging: %w", err)
	}

	staged, err := l.extractToStaging(ctx, q)
	if err != nil {
		return Result{}, err
	}

	if err := l.checkGuards(ctx, q, staged); err != nil {
		return Result{}, err
	}

	var logRow dw.DwEtlLog
	scd2Start := time.Now()
	switch l.cfg.Strategy {
	case StrategySteps:
		logRow, err = applyWithoutFunction(ctx, tx, q, staged)
	default:
		logRow, err = applyWithFunction(ctx, q)
	}
	if err != nil {
		return Result{}, err
	}

	res := Result{
		Log:          logRow,
		Staged:       staged,
		DryRun:       l.cfg.DryRun,
		Strategy:     l.cfg.Strategy,
		SCD2Duration: time.Since(scd2Start),
	}
	if l.cfg.DryRun {
		return res, nil // o defer faz o ROLLBACK
	}

	// A restrição DEFERRABLE (uma versão atual por chave) é verificada aqui,
	// nas duas estratégias.
	if err := tx.Commit(ctx); err != nil {
		return Result{}, fmt.Errorf("commit: %w", err)
	}
	return res, nil
}

// batch é o lote que trafega entre a goroutine leitora e a gravadora.
type batch struct {
	rows []dw.InsertStagingParams
	err  error
}

// extractToStaging lê a origem em lotes e grava cada lote com COPY,
// sobrepondo a leitura do lote N+1 com a gravação do lote N.
func (l *Loader) extractToStaging(ctx context.Context, q *dw.Queries) (int64, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel() // encerra a produtora se a gravação falhar

	batches := make(chan batch, 2)
	go l.produce(ctx, batches)

	var total int64
	for b := range batches {
		if b.err != nil {
			return 0, fmt.Errorf("extrair origem: %w", b.err)
		}
		n, err := q.InsertStaging(ctx, b.rows)
		if err != nil {
			return 0, fmt.Errorf("copy staging: %w", err)
		}
		total += n
	}
	// Se o contexto foi cancelado, a produtora fecha o canal sem erro explícito.
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return total, nil
}

func (l *Loader) produce(ctx context.Context, out chan<- batch) {
	defer close(out)

	sq := source.New(l.src)
	var after int32

	for {
		rows, err := sq.ListTerritoriesAfter(ctx, source.ListTerritoriesAfterParams{
			AfterID:   after,
			BatchSize: l.cfg.BatchSize,
		})
		if err != nil {
			send(ctx, out, batch{err: err})
			return
		}
		if len(rows) == 0 {
			return
		}

		params := make([]dw.InsertStagingParams, len(rows))
		for i, r := range rows {
			params[i] = dw.InsertStagingParams{
				SalesTerritoryID: r.Territoryid,
				Region:           text(r.Region),
				Country:          text(r.Country),
				Group:            text(r.Group),
			}
		}
		if !send(ctx, out, batch{rows: params}) {
			return
		}

		after = rows[len(rows)-1].Territoryid
		if int32(len(rows)) < l.cfg.BatchSize {
			return // último lote
		}
	}
}

func send(ctx context.Context, out chan<- batch, b batch) bool {
	select {
	case out <- b:
		return true
	case <-ctx.Done():
		return false
	}
}

func (l *Loader) checkGuards(ctx context.Context, q *dw.Queries, staged int64) error {
	if staged == 0 {
		return fmt.Errorf("%w: a origem não retornou linhas", ErrGuard)
	}
	if l.cfg.MaxDeletePct <= 0 {
		return nil
	}

	current, err := q.CountCurrent(ctx)
	if err != nil {
		return fmt.Errorf("contar versões atuais: %w", err)
	}
	if current == 0 {
		return nil // primeira carga
	}
	toDelete, err := q.CountToBeDeleted(ctx)
	if err != nil {
		return fmt.Errorf("contar exclusões: %w", err)
	}
	pct := float64(toDelete) * 100 / float64(current)
	if pct > l.cfg.MaxDeletePct {
		return fmt.Errorf("%w: %d de %d versões atuais (%.1f%%) seriam excluídas; limite %.1f%%",
			ErrGuard, toDelete, current, pct, l.cfg.MaxDeletePct)
	}
	return nil
}

func text(s string) pgtype.Text {
	return pgtype.Text{String: s, Valid: true}
}
