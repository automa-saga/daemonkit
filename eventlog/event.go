// SPDX-License-Identifier: Apache-2.0

package eventlog

import (
	"errors"
	"time"
)

// Level is the severity of a lifecycle milestone — mirrors HIP-defined values.
// Three levels are defined: INFO for a milestone that happened, ERROR for one
// that failed terminally, and WARN for a milestone that completed in a degraded
// or policy-notable way — an optional artifact skipped, or a safety check
// deliberately bypassed. WARN is not for retries, backoff, or transient errors;
// those, like all operational states, belong in journald, not here.
// DEBUG has no place in a sparse milestone audit trail.
type Level string

const (
	LevelInfo  Level = "INFO"
	LevelWarn  Level = "WARN"
	LevelError Level = "ERROR"
)

// Event is a single lifecycle milestone written to a JSONL file.
// All fields except Metadata are required; Log rejects an Event with any
// required zero value. Metadata carries optional structured key-value pairs
// that do not fit the fixed schema (e.g. scheduled_time, artifact_hash).
type Event struct {
	Ts          time.Time         `json:"ts"`
	Level       Level             `json:"level"`
	Reason      string            `json:"reason"`
	Msg         string            `json:"msg"`
	OperationID string            `json:"operationId"`
	NodeID      string            `json:"nodeId"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

func (e Event) validate() error {
	var errs []error
	if e.Ts.IsZero() {
		errs = append(errs, ErrInvalidEvent.New("Ts is required"))
	}
	if e.Level == "" {
		errs = append(errs, ErrInvalidEvent.New("Level is required"))
	}
	if e.Reason == "" {
		errs = append(errs, ErrInvalidEvent.New("Reason is required"))
	}
	if e.Msg == "" {
		errs = append(errs, ErrInvalidEvent.New("Msg is required"))
	}
	if e.OperationID == "" {
		errs = append(errs, ErrInvalidEvent.New("OperationID is required"))
	}
	if e.NodeID == "" {
		errs = append(errs, ErrInvalidEvent.New("NodeID is required"))
	}
	return errors.Join(errs...)
}
