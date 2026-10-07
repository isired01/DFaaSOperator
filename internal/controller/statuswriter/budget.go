/*
Copyright 2026 Isaia Del Rosso.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package statuswriter

import (
	"context"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Budget is one named Retry counter with a ceiling.
//
// A round is Attempt (bump, compare to the Limit), then either a terminal
// Condition and Phase on exhaustion or a progress Condition and a requeue;
// Clear on the next success. When the counter write itself fails, Attempt
// reports CounterUnavailable instead of a count of zero.
//
// Counters stay generation-scoped, "<generation>:<count>" (ADR-0003,
// docs/adr/0003-all-retry-counters-generation-scoped.md); Budget owns only the
// policy on top, and inherits the encoding untouched through Writer.Bump. Per
// ADR-0002 (docs/adr/0002-status-writer-never-decides-requeue.md) Attempt
// returns no ctrl.Result: each caller keeps its own requeue and its own
// terminal Phase.
type Budget[T client.Object, P ~string] struct {
	Writer  Writer[T, P]
	Counter string
	Limit   int
}

// Outcome is the only thing a caller branches on.
type Outcome struct {
	// Count is the number of consecutive failed rounds including this one, or
	// 0 when the counter could not be written.
	Count int
	// First is true on the first ever observation for this generation. Callers
	// that distinguish "not checked yet" from "failing" stamp Unknown here.
	First bool
	// Exhausted is true once the budget is spent: the caller goes terminal.
	Exhausted bool
	// CounterUnavailable is true when the counter write failed. The round still
	// happened and the caller must still surface it -- it just cannot be
	// counted, so the budget does not advance and nothing goes terminal on the
	// strength of an unknown count.
	CounterUnavailable bool
}

// Attempt records one failed round.
//
// The error return is for a caller that wants to propagate it; every current
// caller treats a counter write failure as non-fatal and branches on
// CounterUnavailable instead.
func (b Budget[T, P]) Attempt(ctx context.Context, obj T) (Outcome, error) {
	count, err := b.Writer.Bump(ctx, obj, b.Counter)
	if err != nil {
		return Outcome{CounterUnavailable: true}, err
	}
	return Outcome{
		Count:     count,
		First:     count == 1,
		Exhausted: b.Limit > 0 && count >= b.Limit,
	}, nil
}

// Clear resets the counter after a successful round. A no-op when it is
// already zero, so a steady state does not churn the object.
func (b Budget[T, P]) Clear(ctx context.Context, obj T) error {
	return b.Writer.Reset(ctx, obj, b.Counter)
}

// Attempts renders the attempt count for a Condition message: "2/3", or "?/3"
// when the counter could not be written, never "0/3", which would read as a
// real measurement of zero.
func (b Budget[T, P]) Attempts(o Outcome) string {
	if o.CounterUnavailable {
		return fmt.Sprintf("?/%d", b.Limit)
	}
	return fmt.Sprintf("%d/%d", o.Count, b.Limit)
}
