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
	if len(args) != 2 {
		return fmt.Errorf("usage: hermes [TLS flags] artifact-put <file> | submit <manifest> | status <run>")
	}
	client, e := agent.NewClient(*endpoint, *ca, *cert, *key)
	if e != nil {
		return e
	}
	ctx := context.Background()
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
