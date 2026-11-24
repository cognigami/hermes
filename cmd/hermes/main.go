package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"hermes/internal/agent"
	"hermes/internal/contracts"
	"io"
	"net/url"
	"os"
	"strings"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	endpoint := flag.String("url", "https://localhost:8443", "controller URL")
	ca := flag.String("ca", "", "server CA")
	cert := flag.String("cert", "", "operator certificate")
	key := flag.String("key", "", "operator key")
	flag.Parse()
	args := flag.Args()
	if len(args) != 2 && !(len(args) == 3 && args[0] == "finite-export") {
		return fmt.Errorf("usage: hermes [TLS flags] artifact-put <file> | submit <manifest> | status <run> | control <json> | control-status <kind:id> | lost-spool <json> | workflow-register <json> | workflow-control <json> | workflow-status <id> | finite-export <request.json> <directory> | finite-import <directory>")
	}
	client, e := agent.NewClient(*endpoint, *ca, *cert, *key)
	if e != nil {
		return e
	}
	ctx := context.Background()
	if handled, err := runFiniteCommand(ctx, client, args, os.Stdout); handled {
		return err
	}
	if handled, err := runWorkflowCommand(ctx, client, args, os.Stdout); handled {
		return err
	}
	switch args[0] {
	case "artifact-put":
		f, e := os.Open(args[1])
		if e != nil {
			return e
		}
		defer f.Close()
		info, e := f.Stat()
		if e != nil {
			return e
		}
		h := sha256.New()
		if _, e = io.Copy(h, f); e != nil {
			return e
		}
		digest := hex.EncodeToString(h.Sum(nil))
		if _, e = f.Seek(0, 0); e != nil {
			return e
		}
		if e = client.Publish(ctx, digest, info.Size(), "", f); e != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(contracts.Artifact{Digest: digest, Size: info.Size(), MediaType: "application/octet-stream"})
	case "submit":
		f, e := os.Open(args[1])
		if e != nil {
			return e
		}
		defer f.Close()
		var s contracts.Submission
		if e = contracts.Decode(f, &s); e != nil {
			return e
		}
		var outcome contracts.Acknowledgment
		if e = client.Do(ctx, "POST", "/v1/runs", s, &outcome); e != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(outcome)
	case "control", "lost-spool":
		f, e := os.Open(args[1])
		if e != nil {
			return e
		}
		defer f.Close()
		if args[0] == "control" {
			var request contracts.ControlRequest
			if e = contracts.Decode(f, &request); e != nil {
				return e
			}
			var outcome contracts.ControlStatus
			if e = client.Do(ctx, "POST", "/v1/control", request, &outcome); e != nil {
				return e
			}
			return json.NewEncoder(os.Stdout).Encode(outcome)
		}
		var request contracts.LostSpoolResolution
		if e = contracts.Decode(f, &request); e != nil {
			return e
		}
		if e = client.Do(ctx, "POST", "/v1/lost-spool", request, nil); e != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(contracts.Acknowledgment{SchemaVersion: 1, Received: true})
	case "control-status":
		parts := strings.SplitN(args[1], ":", 2)
		if len(parts) != 2 {
			return fmt.Errorf("control-status requires run:<id> or worker:<id>")
		}
		var status contracts.ControlStatus
		if e = client.Do(ctx, "GET", "/v1/control/status?kind="+url.QueryEscape(parts[0])+"&target="+url.QueryEscape(parts[1]), nil, &status); e != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(status)
	case "status":
		var s contracts.Status
		if e = client.Do(ctx, "GET", "/v1/status?run="+url.QueryEscape(args[1]), nil, &s); e != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(s)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}
