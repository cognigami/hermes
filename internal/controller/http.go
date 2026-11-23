package controller

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"hermes/internal/contracts"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func (c *Controller) Handler() http.Handler { return http.HandlerFunc(c.serve) }

// HandlerWithWorkflows enables workflow metadata routes after InstallWorkflows.
// Approval is controller-local configuration; nil disables registration/execution,
// but existing workflow status and control remain available.
func (c *Controller) HandlerWithWorkflows(approval *WorkflowAdapterConfig) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.serveHTTP(w, r, true, approval)
	})
}

func (c *Controller) serve(w http.ResponseWriter, r *http.Request) {
	c.serveHTTP(w, r, false, nil)
}
func (c *Controller) serveHTTP(w http.ResponseWriter, r *http.Request, workflows bool, approval *WorkflowAdapterConfig) {
	if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.PeerCertificates) == 0 {
		http.Error(w, "client certificate required", 401)
		return
	}
	principal := r.TLS.PeerCertificates[0].Subject.CommonName
	operator := principal == "operator"
	workerID := strings.TrimPrefix(principal, "worker:")
	worker := strings.HasPrefix(principal, "worker:") && c.allowedWorker(workerID)
	if !operator && !worker {
		http.Error(w, "unknown principal", 403)
		return
	}
	if workflows && c.serveWorkflowHTTP(w, r, principal, approval) {
		return
	}
	fail := func(e error) { http.Error(w, e.Error(), 400) }
	w.Header().Set("Content-Type", "application/json")
	if strings.HasPrefix(r.URL.Path, "/v1/artifacts/") {
		digest := strings.TrimPrefix(r.URL.Path, "/v1/artifacts/")
		if len(digest) != 64 {
			fail(errors.New("invalid digest"))
			return
		}
		if worker {
			var owner string
			if e := c.db.QueryRow("SELECT worker FROM assignments WHERE id=?", r.URL.Query().Get("assignment")).Scan(&owner); e != nil || owner != workerID {
				http.Error(w, "assignment required", 403)
				return
			}
		}
		if r.Method == "PUT" {
			size, e := strconv.ParseInt(r.Header.Get("X-Artifact-Size"), 10, 64)
			if e != nil {
				fail(e)
				return
			}
			assignment := ""
			if worker {
				assignment = r.URL.Query().Get("assignment")
			}
			if e = c.PublishArtifact(r.Context(), digest, size, assignment, r.Body); e != nil {
				status := http.StatusInternalServerError
				if errors.Is(e, ErrControllerCapacity) {
					status = http.StatusServiceUnavailable
				} else if errors.Is(e, ErrInvalidArtifact) {
					status = http.StatusBadRequest
				}
				http.Error(w, e.Error(), status)
				return
			}
			json.NewEncoder(w).Encode(contracts.Acknowledgment{SchemaVersion: 1, Published: true})
			return
		}
		if r.Method == "GET" && worker {
			tx, e := c.db.BeginTx(r.Context(), nil)
			if e != nil {
				fail(e)
				return
			}
			var manifest []byte
			e = tx.QueryRow("SELECT manifest FROM assignments WHERE id=?", r.URL.Query().Get("assignment")).Scan(&manifest)
			var a contracts.Assignment
			if e == nil {
				e = json.Unmarshal(manifest, &a)
			}
			var work contracts.Work
			if e == nil {
				work, e = c.work(tx, a)
			}
			tx.Rollback()
			if e != nil {
				fail(e)
				return
			}
			allowed := false
			refs := append([]contracts.Artifact{}, work.Package.Preparation...)
			for _, u := range work.Units {
				refs = append(refs, u.Inputs...)
			}
			for _, ref := range refs {
				if ref.Digest == digest {
					allowed = true
				}
			}
			if !allowed {
				http.Error(w, "artifact not assigned", 403)
				return
			}
		}
		if r.Method == "GET" && (operator || worker) {
			assignment := ""
			if worker {
				assignment = r.URL.Query().Get("assignment")
			}
			transferCtx, release, e := c.BeginArtifactTransfer(r.Context(), assignment)
			if e != nil {
				status := http.StatusInternalServerError
				if errors.Is(e, ErrControllerCapacity) {
					status = http.StatusServiceUnavailable
				}
				http.Error(w, e.Error(), status)
				return
			}
			defer release()
			stopIO := context.AfterFunc(transferCtx, func() {
				if e := http.NewResponseController(w).SetWriteDeadline(time.Now()); e != nil && !errors.Is(e, http.ErrNotSupported) {
					log.Printf("cancel artifact response: %v", e)
				}
			})
			defer stopIO()
			f, e := c.store.Open(digest)
			if e != nil {
				fail(e)
				return
			}
			defer f.Close()
			info, e := f.Stat()
			if e != nil {
				fail(e)
				return
			}
			http.ServeContent(w, r.WithContext(transferCtx), digest, info.ModTime(), f)
			return
		}
		http.Error(w, "forbidden", 403)
		return
	}
	if r.Method == "GET" && r.URL.Path == "/v1/resources" && operator {
		status, e := c.ResourceStatus(r.Context(), r.URL.Query().Get("host"))
		if e != nil {
			fail(e)
			return
		}
		json.NewEncoder(w).Encode(status)
		return
	}
	if r.Method == "GET" && r.URL.Path == "/v1/status" && operator {
		s, e := c.Status(r.Context(), r.URL.Query().Get("run"))
		if e != nil {
			fail(e)
			return
		}
		json.NewEncoder(w).Encode(s)
		return
	}
	if r.Method == "GET" && r.URL.Path == "/v1/control/status" && operator {
		s, e := c.ControlStatus(r.Context(), r.URL.Query().Get("kind"), r.URL.Query().Get("target"))
		if e != nil {
			fail(e)
			return
		}
		json.NewEncoder(w).Encode(s)
		return
	}
	if r.Method != "POST" {
		http.Error(w, "not found", 404)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, contracts.MaxMetadataBytes)
	if worker {
		b, e := io.ReadAll(r.Body)
		if e != nil {
			fail(e)
			return
		}
		var identity struct {
			WorkerID string `json:"worker_id"`
		}
		if e = json.Unmarshal(b, &identity); e != nil {
			fail(e)
			return
		}
		if identity.WorkerID != workerID {
			http.Error(w, "worker certificate/body mismatch", 403)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(b))
	}
	if c.controlRoute(w, r, principal, operator, worker) {
		return
	}
	var result []byte
	var e error
	switch r.URL.Path {
	case "/v1/runs":
		if !operator {
			http.Error(w, "operator required", 403)
			return
		}
		var s contracts.Submission
		if e = contracts.Decode(r.Body, &s); e == nil {
			result, e = c.mutate(r.Context(), principal, s.RequestID, s, func(tx *sql.Tx) (any, error) { return c.submit(tx, s) })
		}
	case "/v1/resource-policy":
		if !operator {
			http.Error(w, "operator required", 403)
			return
		}
		var policy contracts.PolicyRequest
		if e = contracts.Decode(r.Body, &policy); e == nil {
			result, e = c.mutate(r.Context(), principal, policy.RequestID, policy, func(tx *sql.Tx) (any, error) { return c.updateResourcePolicy(tx, policy) })
		}
	case "/v1/resource-stop":
		if !worker {
			http.Error(w, "worker required", 403)
			return
		}
		var stop contracts.ResourceStop
		if e = contracts.Decode(r.Body, &stop); e == nil {
			result, e = c.mutate(r.Context(), principal, stop.RequestID, stop, func(tx *sql.Tx) (any, error) { return c.resourceStop(tx, stop) })
		}
	case "/v1/register":
		if !worker {
			http.Error(w, "worker required", 403)
			return
		}
		var registration contracts.WorkerRegistration
		if e = contracts.Decode(r.Body, &registration); e == nil {
			result, e = c.mutate(r.Context(), principal, registration.RequestID, registration, func(tx *sql.Tx) (any, error) { return c.registerHost(tx, registration) })
		}
	case "/v1/poll":
		if !worker {
			http.Error(w, "worker required", 403)
			return
		}
		var p contracts.PollRequest
		if e = contracts.Decode(r.Body, &p); e == nil {
			if p.WorkerID != workerID {
				fail(errors.New("worker mismatch"))
				return
			}
			result, e = c.mutate(r.Context(), principal, p.RequestID, p, func(tx *sql.Tx) (any, error) { return c.poll(tx, p) })
		}
	case "/v1/receipt":
		if !worker {
			http.Error(w, "worker required", 403)
			return
		}
		var p contracts.Receipt
		if e = contracts.Decode(r.Body, &p); e == nil {
			result, e = c.mutate(r.Context(), principal, p.RequestID, p, func(tx *sql.Tx) (any, error) {
				if _, e := c.bound(tx, p); e != nil {
					return nil, e
				}
				_, e := tx.Exec("UPDATE assignments SET receipt=1 WHERE id=?", p.AssignmentID)
				return contracts.Acknowledgment{SchemaVersion: 1, Received: true}, e
			})
		}
	case "/v1/candidate":
		if !worker {
			http.Error(w, "worker required", 403)
			return
		}
		var p contracts.CandidateReport
		if e = contracts.Decode(r.Body, &p); e == nil {
			result, e = c.mutate(r.Context(), principal, p.RequestID, p, func(tx *sql.Tx) (any, error) { return c.candidate(tx, p) })
		}
	case "/v1/final":
		if !worker {
			http.Error(w, "worker required", 403)
			return
		}
		var p contracts.FinalReport
		if e = contracts.Decode(r.Body, &p); e == nil {
			result, e = c.mutate(r.Context(), principal, p.RequestID, p, func(tx *sql.Tx) (any, error) { return c.final(tx, p) })
		}
	default:
		http.Error(w, "not found", 404)
		return
	}
	if e != nil {
		fail(e)
		return
	}
	w.Write(result)
}
