package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/uptrace/bun"

	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
)

const (
	// indexShipEvery is how often this shard's changed sessions are shipped, and so how far
	// behind the shared database's homes and lists can be.
	indexShipEvery = 5 * time.Second
	// indexOverlap is how far before its mark a ship reads again. A row's updated_at is set by
	// the router writing it before the write commits, so a slow commit can land behind a mark
	// already past it; shipping a row twice changes nothing.
	indexOverlap = time.Minute
)

// indexShipped is how far a shard has shipped one kind of row (Store.ShipIndex).
type indexShipped struct {
	bun.BaseModel `bun:"table:shard_index_shipped"`

	Shard          string    `bun:"shard,pk"`
	Kind           string    `bun:"kind,pk"`
	ShippedThrough time.Time `bun:"shipped_through"`
}

// ShipIndex writes into the shared database what it keeps of this shard's sessions changed
// since the last ship: where they live (SessionHomes). A shard that never shipped ships every
// session it has first, statsShipBatch at a time, and carries on in the next ship when this one
// runs out of time.
func (s *Store) ShipIndex(ctx context.Context, _ time.Time) error {
	shard := s.ShardName()
	var mark indexShipped
	err := s.global.NewSelect().Model(&mark).Where("shard = ? AND kind = ?", shard, "sessions").Scan(ctx)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return stack.Wrap(fmt.Errorf("store: ship index: %w", err))
	}
	from := time.Time{}
	if !mark.ShippedThrough.IsZero() {
		from = mark.ShippedThrough.Add(-indexOverlap)
	}
	type key struct {
		updated  time.Time
		customer string
		id       string
	}
	var after *key
	for {
		var rows []AgentSession
		q := s.shard.NewSelect().Model(&rows).Column("customer_id", "id", "created_at", "updated_at").
			Where("updated_at >= ?", from)
		if after != nil {
			q = q.Where("(updated_at, customer_id, id) > (?, ?, ?)", after.updated, after.customer, after.id)
		}
		if err := q.Order("updated_at", "customer_id", "id").Limit(statsShipBatch).Scan(ctx); err != nil {
			return stack.Wrap(fmt.Errorf("store: ship index: read sessions: %w", err))
		}
		if len(rows) == 0 {
			return nil
		}
		homes := make([]SessionHome, len(rows))
		for i, row := range rows {
			homes[i] = SessionHome{CustomerID: row.CustomerID, SessionID: row.ID, OpenedAt: row.CreatedAt}
		}
		if err := s.SessionHomes.record(ctx, homes); err != nil {
			return err
		}
		last := rows[len(rows)-1]
		_, err := s.global.NewInsert().Model(&indexShipped{Shard: shard, Kind: "sessions", ShippedThrough: last.UpdatedAt}).
			On("CONFLICT (shard, kind) DO UPDATE").
			Set("shipped_through = GREATEST(?TableAlias.shipped_through, EXCLUDED.shipped_through)").
			Set("shipped_at = now()").
			Exec(ctx)
		if err != nil {
			return stack.Wrap(fmt.Errorf("store: ship index: mark: %w", err))
		}
		if len(rows) < statsShipBatch {
			return nil
		}
		after = &key{updated: last.UpdatedAt, customer: last.CustomerID, id: last.ID}
	}
}
