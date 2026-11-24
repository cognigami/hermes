package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"golang.org/x/sys/unix"
	"hermes/internal/agent"
	"hermes/internal/contracts"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
)

func decodeFiniteFile(path string, value any) error {
	if path == "" {
		return errors.New("finite metadata path required")
	}
	root, e := os.OpenRoot(filepath.Dir(path))
	if e != nil {
		return e
	}
	defer root.Close()
	file, e := root.OpenFile(filepath.Base(path), os.O_RDONLY|unix.O_NONBLOCK, 0)
	if e != nil {
		return e
	}
	defer file.Close()
	info, e := file.Stat()
	if e != nil {
		return e
	}
	if !info.Mode().IsRegular() {
		return errors.New("finite metadata must be regular file")
	}
	return contracts.Decode(file, value)
}
func runFiniteCommand(command string, args []string) error {
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	root := flags.String("root", "", "absolute host state root")
	ackPath := flags.String("ack", "", "genuine controller import acknowledgment JSON")
	bundle := flags.String("bundle", "", "immutable input bundle directory")
	approved := flags.String("approved-runtimes", "", "local deployment-approved runtime bindings JSON")
	worker := flags.String("worker", "", "provisioned worker identity")
	session := flags.String("session", "", "provisioned execution session")
	host := flags.String("host-id", "", "approved host identity (defaults to worker)")
	url := flags.String("url", "https://localhost:8443", "controller URL for real registration only")
	ca := flags.String("ca", "", "server CA")
	cert := flags.String("cert", "", "worker certificate")
	key := flags.String("key", "", "worker key")
	maximum := flags.Int64("max-artifact-bytes", 1<<20, "maximum individual artifact bytes")
	concurrent := flags.Int("max-concurrent", 64, "local application process bound")
	synthetic := flags.Bool("synthetic-backend", false, "enable independently approved synthetic infrastructure events")
	memory := flags.Int64("synthetic-memory-bytes", 0, "synthetic memory demand only")
	phase := flags.String("synthetic-stop-phase", "unit", "synthetic event phase prepare or unit")
	if e := flags.Parse(args); e != nil {
		return e
	}
	if *maximum <= 0 || *maximum == 1<<63-1 {
		return errors.New("max-artifact-bytes must be positive and leave room for overflow-safe length checking")
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected finite positional arguments")
	}
	if !filepath.IsAbs(*root) {
		return errors.New("absolute host root required")
	}
	if command == "finite-ack" {
		var ack contracts.FiniteImportAck
		if e := decodeFiniteFile(*ackPath, &ack); e != nil {
			return e
		}
		return agent.ApplyFiniteImportAck(*root, ack, *ackPath)
	}
	if *worker == "" || *session == "" {
		return errors.New("independently provisioned worker/session required")
	}
	if *host == "" {
		*host = *worker
	}
	cfg := agent.Config{Root: *root, WorkerID: *worker, SessionID: *session, HostID: *host, MaxArtifactBytes: *maximum, MaxConcurrent: *concurrent, SyntheticBackend: *synthetic, SyntheticMemoryBytes: *memory, SyntheticPhase: *phase}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if command == "register" {
		client, e := agent.NewClient(*url, *ca, *cert, *key)
		if e != nil {
			return e
		}
		cfg.Client = client
		budget, e := agent.RegisterFiniteHost(ctx, cfg)
		if e != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(budget)
	}
	if !filepath.IsAbs(*bundle) {
		return errors.New("absolute input bundle directory required")
	}
	var envelope contracts.FiniteBundleEnvelope
	if e := decodeFiniteFile(filepath.Join(*bundle, "bundle.json"), &envelope); e != nil {
		return e
	}
	var bindings []agent.FiniteRuntimeBinding
	if e := decodeFiniteFile(*approved, &bindings); e != nil {
		return e
	}
	return agent.RunFinite(ctx, agent.FiniteConfig{Config: cfg, BundleDirectory: *bundle, Runtimes: bindings}, envelope)
}
