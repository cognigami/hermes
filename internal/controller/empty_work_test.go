package controller

import (
	"context"
	"errors"
	"hermes/internal/agent"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAgentRejectsMalformedIdleWork(t *testing.T) {
	f := setup(t)
	for _, body := range []string{`null`, `{}`, `{"schema_version":2}`, `{"schema_version":1,"package":{}}`, `{"schema_version":1,"units":[]}`} {
		t.Run(body, func(t *testing.T) {
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte(body))
			}))
			server.TLS = f.server.TLS.Clone()
			server.StartTLS()
			defer server.Close()
			client := *f.worker
			client.URL = server.URL
			root := filepath.Join(t.TempDir(), "spool")
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			e := agent.Run(ctx, agent.Config{Root: root, WorkerID: "local", SessionID: "session", Runtime: "synthetic-v1", Profile: "synthetic-local", RuntimeArgv: []string{f.app}, Client: &client, MaxArtifactBytes: 1 << 20})
			if e == nil || errors.Is(e, context.DeadlineExceeded) {
				t.Fatal("malformed idle response was not rejected promptly", e)
			}
			if _, e = os.Stat(filepath.Join(root, "journal.json")); !errors.Is(e, os.ErrNotExist) {
				t.Fatal("malformed work was journaled", e)
			}
		})
	}
}
