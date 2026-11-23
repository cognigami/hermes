// Package agent runs one explicitly configured local package profile. It does
// not qualify general process-tree containment or restart adoption (S3).
package agent

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"hermes/internal/artifacts"
	"hermes/internal/contracts"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

type Config struct {
	Root, WorkerID, SessionID, Runtime, Profile string
	RuntimeArgv                                 []string
	Client                                      *Client
	MaxArtifactBytes                            int64
}
type journal struct {
	Work           contracts.Work        `json:"work"`
	ProcessStarted bool                  `json:"process_started"`
	Started        []string              `json:"started"`
	Candidates     []contracts.Candidate `json:"candidates"`
	Delivered      int                   `json:"delivered"`
	Quiescent      bool                  `json:"quiescent"`
	FinalSent      bool                  `json:"final_sent"`
}
type runner struct {
	cfg   Config
	mu    sync.Mutex
	j     journal
	store *artifacts.Store
}

func ID() string {
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		panic(e)
	}
	return hex.EncodeToString(b[:])
}
func durable(path string, value any) error {
	b, e := json.Marshal(value)
	if e != nil {
		return e
	}
	f, e := os.CreateTemp(filepath.Dir(path), ".journal-")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	if _, e = f.Write(b); e != nil {
		f.Close()
		return e
	}
	if e = f.Sync(); e != nil {
		f.Close()
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	if e = os.Rename(f.Name(), path); e != nil {
		return e
	}
	d, e := os.Open(filepath.Dir(path))
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}
func (r *runner) save() error { return durable(filepath.Join(r.cfg.Root, "journal.json"), r.j) }
func Run(ctx context.Context, cfg Config) error {
	if cfg.Root == "" || cfg.Client == nil || cfg.WorkerID == "" || cfg.SessionID == "" || cfg.Runtime == "" || cfg.Profile == "" || len(cfg.RuntimeArgv) == 0 || cfg.MaxArtifactBytes <= 0 {
		return errors.New("incomplete agent config")
	}
	if !filepath.IsAbs(cfg.RuntimeArgv[0]) {
		return errors.New("runtime executable must be absolute")
	}
	absolute, e := filepath.Abs(cfg.Root)
	if e != nil {
		return e
	}
	cfg.Root = absolute
	if e := os.MkdirAll(cfg.Root, 0700); e != nil {
		return e
	}
	lock, e := os.OpenFile(filepath.Join(cfg.Root, "agent.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return e
	}
	defer lock.Close()
	if e = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); e != nil {
		return e
	}
	identity := struct{ Worker, Session string }{cfg.WorkerID, cfg.SessionID}
	path := filepath.Join(cfg.Root, "identity.json")
	if b, e := os.ReadFile(path); e == nil {
		var prior struct{ Worker, Session string }
		if e = json.Unmarshal(b, &prior); e != nil {
			return e
		}
		if prior != identity {
			return errors.New("persisted worker/session identity mismatch")
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return e
	} else if e = durable(path, identity); e != nil {
		return e
	}
	store, e := artifacts.New(filepath.Join(cfg.Root, "blobs"), cfg.MaxArtifactBytes)
	if e != nil {
		return e
	}
	r := &runner{cfg: cfg, store: store}
	if b, e := os.ReadFile(filepath.Join(cfg.Root, "journal.json")); e == nil {
		if e = json.Unmarshal(b, &r.j); e != nil {
			return e
		}
		if e = validateWork(r.j.Work); e != nil {
			return e
		}
		a := r.j.Work.Assignment
		if a == nil {
			return errors.New("retained journal has no assignment")
		}
		if !r.j.FinalSent && (a.WorkerID != cfg.WorkerID || a.SessionID != cfg.SessionID || a.Runtime != cfg.Runtime || a.Profile != cfg.Profile) {
			return errors.New("retained assignment incompatible with configured worker")
		}
		if (r.j.ProcessStarted || len(r.j.Started) > 0) && !r.j.Quiescent {
			return errors.New("started unfinished assignment: ownership uncertain, S3 reconciliation required")
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return e
	}
	for {
		if e = ctx.Err(); e != nil {
			return e
		}
		if r.j.Work.Assignment != nil && !r.j.FinalSent {
			if e = r.assignment(ctx); e != nil {
				return e
			}
		}
		request := contracts.PollRequest{SchemaVersion: 1, RequestID: ID(), WorkerID: cfg.WorkerID, SessionID: cfg.SessionID}
		var work contracts.Work
		for {
			e = cfg.Client.Do(ctx, "POST", "/v1/poll", request, &work)
			if e == nil {
				break
			}
			if e = retry(ctx, e); e != nil {
				return e
			}
		}
		if e = validateWork(work); e != nil {
			return e
		}
		if work.Assignment == nil {
			if e = wait(ctx); e != nil {
				return e
			}
			continue
		}
		a := work.Assignment
		if a.WorkerID != cfg.WorkerID || a.SessionID != cfg.SessionID || a.Runtime != cfg.Runtime || a.Profile != cfg.Profile {
			return errors.New("incompatible assignment")
		}
		if r.j.Work.Assignment != nil && r.j.Work.Assignment.AssignmentID == a.AssignmentID {
			return errors.New("controller reissued terminal assignment")
		}
		r.j = journal{Work: work}
		if e = r.save(); e != nil {
			return e
		}
	}
}
func retry(ctx context.Context, err error) error {
	if errors.Is(err, ErrProtocol) {
		return err
	}
	var response *HTTPError
	if errors.As(err, &response) && response.Status >= 400 && response.Status < 500 {
		return err
	}
	return wait(ctx)
}
func wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(100 * time.Millisecond):
		return nil
	}
}
func (r *runner) receipt(suffix string) contracts.Receipt {
	a := r.j.Work.Assignment
	return contracts.Receipt{SchemaVersion: 1, RequestID: a.AssignmentID + ":" + suffix, AssignmentID: a.AssignmentID, WorkerID: a.WorkerID, SessionID: a.SessionID}
}
func (r *runner) assignment(ctx context.Context) error {
	// Delivery runs independently of grant progress. Local publication is the only
	// acknowledgment needed before the next issued unit; outage retains bytes.
	deliveryCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	deliveryResult := make(chan error, 1)
	go func() { deliveryResult <- r.deliver(deliveryCtx) }()
	if !r.j.Quiescent {
		if e := r.execute(ctx); e != nil {
			cancel()
			<-deliveryResult
			return e
		}
	}
	if e := <-deliveryResult; e != nil {
		return e
	}
	final := contracts.FinalReport{Receipt: r.receipt("final")}
	for _, c := range r.j.Candidates {
		final.Completed = append(final.Completed, c.UnitID)
	}
	for _, u := range r.j.Work.Assignment.Units {
		found := false
		for _, s := range r.j.Started {
			if s == u.UnitID {
				found = true
			}
		}
		if !found {
			final.NeverStarted = append(final.NeverStarted, u.UnitID)
		}
	}
	for {
		e := r.cfg.Client.Do(ctx, "POST", "/v1/final", final, nil)
		if e == nil {
			break
		}
		if e = retry(ctx, e); e != nil {
			return e
		}
	}
	r.j.FinalSent = true
	return r.save()
}
func (r *runner) deliver(ctx context.Context) error {
	// Receipt reporting is retryable delivery, not permission to execute the
	// finite assignment already reserved and journaled locally.
	for {
		e := r.cfg.Client.Do(ctx, "POST", "/v1/receipt", r.receipt("receipt"), nil)
		if e == nil {
			break
		}
		if e = retry(ctx, e); e != nil {
			return e
		}
	}
	for {
		r.mu.Lock()
		index := r.j.Delivered
		done := r.j.Quiescent
		var c contracts.Candidate
		if index < len(r.j.Candidates) {
			c = r.j.Candidates[index]
		}
		r.mu.Unlock()
		if c.UnitID == "" {
			if done {
				return nil
			}
			if e := wait(ctx); e != nil {
				return e
			}
			continue
		}
		var failure error
		for _, f := range c.Files {
			src, e := r.store.Open(f.Artifact.Digest)
			if e != nil {
				return e
			}
			failure = r.cfg.Client.Publish(ctx, f.Artifact.Digest, f.Artifact.Size, c.AssignmentID, src)
			src.Close()
			if failure != nil {
				break
			}
		}
		if failure == nil {
			report := contracts.CandidateReport{Receipt: r.receipt("candidate:" + contracts.Hash([]byte(c.UnitID))), Candidate: c}
			var ack contracts.Acknowledgment
			failure = r.cfg.Client.Do(ctx, "POST", "/v1/candidate", report, &ack)
			expected, _ := contracts.Digest(c)
			if failure == nil && (ack.SchemaVersion != 1 || ack.RawPublished != expected) {
				failure = fmt.Errorf("%w: candidate acknowledgment identity mismatch", ErrProtocol)
			}
		}
		if failure != nil {
			if e := retry(ctx, failure); e != nil {
				return e
			}
			continue
		}
		r.mu.Lock()
		r.j.Delivered++
		e := r.save()
		r.mu.Unlock()
		if e != nil {
			return e
		}
	}
}

type logBuffer struct{ bytes.Buffer }

func (b *logBuffer) Write(p []byte) (int, error) {
	if b.Len()+len(p) > contracts.MaxMessageBytes {
		return 0, errors.New("application log limit")
	}
	return b.Buffer.Write(p)
}
func (r *runner) execute(ctx context.Context) error {
	a := r.j.Work.Assignment
	dir := filepath.Join(r.cfg.Root, "assignment-"+contracts.Hash([]byte(a.AssignmentID)))
	if e := os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	output := filepath.Join(dir, "output")
	if e := os.MkdirAll(output, 0700); e != nil {
		return e
	}
	manifest := filepath.Join(dir, "manifest.json")
	localManifest := struct {
		SchemaVersion int                   `json:"schema_version"`
		Assignment    *contracts.Assignment `json:"assignment"`
		Units         []contracts.Unit      `json:"units"`
	}{1, a, r.j.Work.Units}
	if e := durable(manifest, localManifest); e != nil {
		return e
	}
	if e := durable(filepath.Join(dir, "package.json"), r.j.Work.Package); e != nil {
		return e
	}
	if e := r.stage(ctx, dir); e != nil {
		return e
	}
	bounded, cancel := context.WithDeadline(ctx, a.TerminateAt)
	defer cancel()
	args := append([]string{}, r.cfg.RuntimeArgv[1:]...)
	args = append(args, "--manifest", manifest, "--output", output)
	cmd := exec.CommandContext(bounded, r.cfg.RuntimeArgv[0], args...)
	cmd.Dir = dir
	stdin, e := cmd.StdinPipe()
	if e != nil {
		return e
	}
	defer stdin.Close()
	stdout, e := cmd.StdoutPipe()
	if e != nil {
		return e
	}
	logs := &logBuffer{}
	cmd.Stderr = logs
	r.mu.Lock()
	r.j.ProcessStarted = true
	e = r.save()
	r.mu.Unlock()
	if e != nil {
		return e
	}
	if e = cmd.Start(); e != nil {
		return e
	}
	finished := false
	defer func() {
		if !finished {
			cmd.Process.Kill()
			cmd.Wait()
		}
	}()
	reader := contracts.MessageReader(stdout)
	m, e := contracts.ReadMessage(reader)
	if e != nil || m.Type != "ready" {
		return fmt.Errorf("application ready: %v", e)
	}
	for _, u := range a.Units {
		if err := bounded.Err(); err != nil {
			return err
		}
		if !time.Now().Before(a.StopNewUnitsAt) {
			break
		}
		r.mu.Lock()
		r.j.Started = append(r.j.Started, u.UnitID)
		e = r.save()
		r.mu.Unlock()
		if e != nil {
			return e
		}
		if e = contracts.WriteMessage(stdin, contracts.Message{SchemaVersion: 1, Type: "execute_unit", UnitID: u.UnitID}); e != nil {
			return e
		}
		m, e = contracts.ReadMessage(reader)
		if e != nil {
			return e
		}
		if m.Type != "unit_result" || m.UnitID != u.UnitID {
			return errors.New("unexpected unit result")
		}
		root, e := os.OpenRoot(output)
		if e != nil {
			return e
		}
		f, e := openRegular(root, m.ManifestPath)
		if e != nil {
			root.Close()
			return e
		}
		var candidate contracts.Candidate
		e = contracts.Decode(f, &candidate)
		f.Close()
		if e != nil {
			root.Close()
			return e
		}
		if e = candidate.Validate(); e != nil {
			root.Close()
			return e
		}
		if candidate.UnitID != u.UnitID || candidate.SpecDigest != u.SpecDigest || candidate.AssignmentID != a.AssignmentID || candidate.AttemptID != a.AssignmentID {
			root.Close()
			return errors.New("unbound candidate")
		}
		for _, ref := range candidate.Files {
			src, e := openRegular(root, ref.Path)
			if e != nil {
				root.Close()
				return e
			}
			e = r.store.Publish(ctx, ref.Artifact.Digest, ref.Artifact.Size, src)
			src.Close()
			if e != nil {
				root.Close()
				return e
			}
		}
		root.Close()
		r.mu.Lock()
		r.j.Candidates = append(r.j.Candidates, candidate)
		e = r.save()
		r.mu.Unlock()
		if e != nil {
			return e
		}
		if e = contracts.WriteMessage(stdin, contracts.Message{SchemaVersion: 1, Type: "published", UnitID: u.UnitID}); e != nil {
			return e
		}
	}
	if e = contracts.WriteMessage(stdin, contracts.Message{SchemaVersion: 1, Type: "finish"}); e != nil {
		return e
	}
	m, e = contracts.ReadMessage(reader)
	if e != nil || m.Type != "finished" {
		return errors.New("missing finished")
	}
	if e = cmd.Wait(); e != nil {
		return e
	}
	finished = true
	r.mu.Lock()
	r.j.Quiescent = true
	e = r.save()
	r.mu.Unlock()
	return e
}
