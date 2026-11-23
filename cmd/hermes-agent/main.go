package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hermes/internal/agent"
	"hermes/internal/platform"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	if len(os.Args) > 1 && os.Args[1] == "check-quiescence" {
		flags := flag.NewFlagSet("check-quiescence", flag.ContinueOnError)
		root := flags.String("root", "", "absolute state root")
		if err := flags.Parse(os.Args[2:]); err != nil {
			return err
		}
		result := agent.CheckLocalQuiescence(*root)
		if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
			return err
		}
		if result.State != "quiescent" {
			return fmt.Errorf("local quiescence %s: %s", result.State, result.Reason)
		}
		return nil
	}
	version := flag.Bool("version", false, "report build and supported capability information")
	root := flag.String("root", "", "spool root")
	worker := flag.String("worker", "synthetic-local", "stable worker ID")
	host := flag.String("host-id", "", "approved stable host ID (defaults to worker ID)")
	synthetic := flag.Bool("synthetic-backend", false, "enable explicitly approved synthetic infrastructure events (no native enforcement qualification)")
	syntheticMemory := flag.Int64("synthetic-memory-bytes", 0, "synthetic backend memory demand in bytes, zero disables simulation")
	syntheticPhase := flag.String("synthetic-stop-phase", "unit", "synthetic memory-stop phase: prepare or unit")
	session := flag.String("session", "", "provisioned stable execution session")
	runtime := flag.String("runtime", "", "absolute synthetic executable")
	url := flag.String("url", "https://localhost:8443", "controller URL")
	ca := flag.String("ca", "", "server CA")
	cert := flag.String("cert", "", "worker certificate")
	key := flag.String("key", "", "worker key")
	var runtimeArgs []string
	flag.Func("runtime-arg", "one allowlisted runtime argv value (repeatable)", func(s string) error { runtimeArgs = append(runtimeArgs, s); return nil })
	flag.Parse()
	if *version {
		return json.NewEncoder(os.Stdout).Encode(platform.Current())
	}
	client, e := agent.NewClient(*url, *ca, *cert, *key)
	if e != nil {
		return e
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	e = agent.Run(ctx, agent.Config{Root: *root, HostID: *host, SyntheticBackend: *synthetic, SyntheticMemoryBytes: *syntheticMemory, SyntheticPhase: *syntheticPhase, WorkerID: *worker, SessionID: *session, Runtime: "synthetic-v1", Profile: "synthetic-local", RuntimeArgv: append([]string{*runtime}, runtimeArgs...), Client: client, MaxArtifactBytes: 1 << 20})
	if errors.Is(e, context.Canceled) {
		return nil
	}
	return e
}
