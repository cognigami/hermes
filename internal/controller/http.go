package controller

import (
	"database/sql"
	"encoding/json"
	"errors"
	"hermes/internal/contracts"
	"net/http"
	"strconv"
	"strings"
)

func (c *Controller) Handler() http.Handler { return http.HandlerFunc(c.serve) }
func (c *Controller) serve(w http.ResponseWriter, r *http.Request) {
	if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.PeerCertificates) == 0 {
		http.Error(w, "client certificate required", 401)
		return
	}
	principal := r.TLS.PeerCertificates[0].Subject.CommonName
	operator := principal == "operator"
	worker := principal == "worker:"+c.cfg.WorkerID
	if !operator && !worker {
		http.Error(w, "unknown principal", 403)
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
			if e := c.db.QueryRow("SELECT worker FROM assignments WHERE id=?", r.URL.Query().Get("assignment")).Scan(&owner); e != nil || owner != c.cfg.WorkerID {
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
			if e = c.store.Publish(r.Context(), digest, size, r.Body); e != nil {
				fail(e)
				return
			}
			if _, e = c.db.ExecContext(r.Context(), "INSERT OR IGNORE INTO artifacts VALUES(?,?)", digest, size); e != nil {
				http.Error(w, e.Error(), 500)
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
			http.ServeContent(w, r, digest, info.ModTime(), f)
			return
		}
		http.Error(w, "forbidden", 403)
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
	if r.Method != "POST" {
		http.Error(w, "not found", 404)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, contracts.MaxMetadataBytes)
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
	case "/v1/poll":
		if !worker {
			http.Error(w, "worker required", 403)
			return
		}
		var p contracts.PollRequest
		if e = contracts.Decode(r.Body, &p); e == nil {
			if p.WorkerID != c.cfg.WorkerID {
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
