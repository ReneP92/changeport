package cdc

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// SlotState is a replication slot as pg_replication_slots shows it.
type SlotState struct {
	// Exists is false once the slot is gone, or before the connector made it.
	Exists bool
	// Active reports whether a connection is reading from it.
	Active bool
	// RetainedBytes is the WAL the database keeps for the slot.
	RetainedBytes int64
	// WALStatus is reserved, extended, unreserved or lost.
	WALStatus string
	// SafeWALSize is how much more WAL the slot may hold back before the
	// database invalidates it; nil without max_slot_wal_keep_size.
	SafeWALSize *int64
}

// ErrSlotActive means a connection still reads from the slot.
var ErrSlotActive = errors.New("the replication slot is in use")

// Slot reads a replication slot of the database.
func (c Checker) Slot(ctx context.Context, connection Connection, slot string) (SlotState, error) {
	conn, err := c.open(ctx, connection)
	if err != nil {
		return SlotState{}, err
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	var state SlotState
	err = conn.QueryRow(ctx, `
SELECT active,
       coalesce(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0)::bigint,
       coalesce(wal_status, ''),
       safe_wal_size
FROM pg_replication_slots
WHERE slot_name = $1`, slot).Scan(&state.Active, &state.RetainedBytes, &state.WALStatus, &state.SafeWALSize)
	if errors.Is(err, pgx.ErrNoRows) {
		return SlotState{}, nil
	}
	if err != nil {
		return SlotState{}, fmt.Errorf("reading replication slot %s: %s", slot, rootCause(err))
	}
	state.Exists = true
	return state, nil
}

// DropSlot drops a replication slot of the database, unless a connection
// still reads from it (ErrSlotActive). It reports whether the slot existed.
func (c Checker) DropSlot(ctx context.Context, connection Connection, slot string) (bool, error) {
	conn, err := c.open(ctx, connection)
	if err != nil {
		return false, err
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	var active bool
	err = conn.QueryRow(ctx, `SELECT active FROM pg_replication_slots WHERE slot_name = $1`, slot).Scan(&active)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("reading replication slot %s: %s", slot, rootCause(err))
	case active:
		return true, ErrSlotActive
	}
	if _, err := conn.Exec(ctx, `SELECT pg_drop_replication_slot($1)`, slot); err != nil {
		return true, fmt.Errorf("dropping replication slot %s: %s", slot, rootCause(err))
	}
	return true, nil
}

// open logs in, or returns why it could not, in the checks' words.
func (c Checker) open(ctx context.Context, connection Connection) (*pgx.Conn, error) {
	if err := connection.Validate(); err != nil {
		return nil, err
	}
	conn, results := c.connect(ctx, connection, false)
	if conn != nil {
		return conn, nil
	}
	for _, result := range results {
		if result.Status == Fail {
			return nil, errors.New(result.Message)
		}
	}
	return nil, errors.New("connecting failed")
}
