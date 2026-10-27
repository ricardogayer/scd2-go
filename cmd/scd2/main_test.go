package main

import (
	"errors"
	"fmt"
	"testing"

	"scd2/internal/scd2"
)

func TestExitCode(t *testing.T) {
	tests := []struct {
		err  error
		want int
	}{
		{nil, 0},
		{errors.New("falhou"), 1},
		{scd2.ErrGuard, 3},
		{fmt.Errorf("%w: origem vazia", scd2.ErrGuard), 3},
	}
	for _, tt := range tests {
		if got := exitCode(tt.err); got != tt.want {
			t.Errorf("exitCode(%v) = %d, esperado %d", tt.err, got, tt.want)
		}
	}
}
