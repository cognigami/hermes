package agent

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
)

const maxPersistedJournalBytes = 16 << 20

// MarshalJSON checks the same recovery bound before durable creates or replaces
// any file. Large cumulative manifests must fail operationally, not become
// acknowledged local state that a restarted agent cannot decode.
func (j journal) MarshalJSON() ([]byte, error) {
	type plainJournal journal
	b, e := json.Marshal(plainJournal(j))
	if e != nil {
		return nil, e
	}
	if len(b) > maxPersistedJournalBytes {
		return nil, errors.New("persisted journal exceeds metadata bound")
	}
	return b, nil
}

// A persisted proof is not a wire Work envelope. Missing/null lifecycle facts
// must not become Go's false zero values and authorize a fresh execution.
func (j *journal) UnmarshalJSON(b []byte) error {
	if len(b) > maxPersistedJournalBytes {
		return errors.New("persisted journal exceeds metadata bound")
	}
	d := json.NewDecoder(bytes.NewReader(b))
	token, e := d.Token()
	if e != nil || token != json.Delim('{') {
		return errors.New("persisted journal must be an object")
	}
	fields := map[string]json.RawMessage{}
	seen := map[string]bool{}
	for d.More() {
		token, e = d.Token()
		if e != nil {
			return e
		}
		key, ok := token.(string)
		if !ok {
			return errors.New("invalid journal key")
		}
		folded := strings.ToLower(strings.ToUpper(key))
		if seen[folded] {
			return errors.New("duplicate journal lifecycle/metadata key")
		}
		seen[folded] = true
		var raw json.RawMessage
		if e = d.Decode(&raw); e != nil {
			return e
		}
		fields[key] = raw
	}
	if _, e = d.Token(); e != nil {
		return e
	}
	var trailing any
	if e = d.Decode(&trailing); e != io.EOF {
		return errors.New("trailing persisted journal data")
	}
	for _, key := range []string{"process_started", "quiescent", "uncertain", "final_sent"} {
		raw, ok := fields[key]
		if !ok {
			return errors.New("missing persisted lifecycle proof: " + key)
		}
		var value *bool
		if e = json.Unmarshal(raw, &value); e != nil || value == nil {
			return errors.New("invalid/null persisted lifecycle proof: " + key)
		}
	}
	type plainJournal journal
	var decoded plainJournal
	strict := json.NewDecoder(bytes.NewReader(b))
	strict.DisallowUnknownFields()
	if e = strict.Decode(&decoded); e != nil {
		return e
	}
	value := journal(decoded)
	if value.Work.Assignment == nil {
		return errors.New("persisted journal has no assignment")
	}
	a := value.Work.Assignment
	if (!value.ProcessStarted && len(value.Started) > 0) || (value.Uncertain && value.Quiescent) || (value.FinalSent && (!value.Quiescent || value.Uncertain)) || len(value.Started) > len(a.Units) || value.Delivered < 0 || value.Delivered > len(value.Candidates) {
		return errors.New("contradictory persisted lifecycle proof")
	}
	for i, id := range value.Started {
		if a.Units[i].UnitID != id {
			return errors.New("persisted grant inventory is not assignment prefix")
		}
	}
	if e := validateFiniteJournal(value); e != nil {
		return e
	}
	*j = value
	return nil
}
