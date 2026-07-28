package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// FieldChange records a changed field between two snapshots.
type FieldChange struct {
	Field    string
	OldValue string
	NewValue string
}

// DiffSnapshots compares two JSON object snapshots (top-level keys only) and
// returns fields that changed, excluding the specified target field.
func DiffSnapshots(before, after []byte, targetField string) ([]FieldChange, error) {
	return diffTopLevel(before, after, map[string]bool{targetField: true})
}

// DiffSnapshotsExcluding compares two JSON object snapshots (top-level keys
// only) and returns fields that changed, excluding any field named in
// exclude. Used by the post-create convergence check to allow known-dynamic
// fields (timestamps, counters) to be excluded from the stability assertion
// via --ignore-fields.
func DiffSnapshotsExcluding(before, after []byte, exclude []string) ([]FieldChange, error) {
	excludeSet := make(map[string]bool, len(exclude))
	for _, f := range exclude {
		f = strings.TrimSpace(f)
		if f != "" {
			excludeSet[f] = true
		}
	}
	return diffTopLevel(before, after, excludeSet)
}

// diffTopLevel compares two JSON object snapshots at the top level, skipping
// any field present in exclude.
func diffTopLevel(before, after []byte, exclude map[string]bool) ([]FieldChange, error) {
	var beforeMap, afterMap map[string]json.RawMessage
	if err := json.Unmarshal(before, &beforeMap); err != nil {
		// If before is empty or null, treat as empty object.
		if len(before) == 0 || string(before) == "null" {
			beforeMap = make(map[string]json.RawMessage)
		} else {
			return nil, fmt.Errorf("parsing before snapshot: %w", err)
		}
	}
	if err := json.Unmarshal(after, &afterMap); err != nil {
		if len(after) == 0 || string(after) == "null" {
			afterMap = make(map[string]json.RawMessage)
		} else {
			return nil, fmt.Errorf("parsing after snapshot: %w", err)
		}
	}

	// Collect all keys from both maps.
	keys := make(map[string]bool)
	for k := range beforeMap {
		keys[k] = true
	}
	for k := range afterMap {
		keys[k] = true
	}

	var changes []FieldChange
	for k := range keys {
		if exclude[k] {
			continue
		}
		oldVal := string(beforeMap[k])
		newVal := string(afterMap[k])
		if oldVal != newVal {
			changes = append(changes, FieldChange{
				Field:    k,
				OldValue: oldVal,
				NewValue: newVal,
			})
		}
	}

	sort.Slice(changes, func(i, j int) bool {
		return changes[i].Field < changes[j].Field
	})
	return changes, nil
}

// FormatChanges returns a human-readable summary of field changes.
func FormatChanges(changes []FieldChange) string {
	if len(changes) == 0 {
		return "all non-target fields stable ✓"
	}
	var lines []string
	for _, c := range changes {
		lines = append(lines, fmt.Sprintf("%s: %s → %s", c.Field, c.OldValue, c.NewValue))
	}
	return strings.Join(lines, ", ")
}
