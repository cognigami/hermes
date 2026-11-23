package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"hermes/internal/agent"
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
	root := flag.String("root", "", "spool root")
	worker := flag.String("worker", "synthetic-local", "stable worker ID")
	session := flag.String("session", "", "provisioned stable execution session")
	runtime := flag.String("runtime", "", "absolute synthetic executable")
	url := flag.String("url", "https://localhost:8443", "controller URL")
	ca := flag.String("ca", "", "server CA")
	cert := flag.String("cert", "", "worker certificate")
	key := flag.String("key", "", "worker key")
	var runtimeArgs []string
	flag.Func("runtime-arg", "one allowlisted runtime argv value (repeatable)", func(s string) error { runtimeArgs = append(runtimeArgs, s); return nil })
	flag.Parse()
	client, e := agent.NewClient(*url, *ca, *cert, *key)
	if e != nil {
		return e
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	e = agent.Run(ctx, agent.Config{Root: *root, WorkerID: *worker, SessionID: *session, Runtime: "synthetic-v1", Profile: "synthetic-local", RuntimeArgv: append([]string{*runtime}, runtimeArgs...), Client: client, MaxArtifactBytes: 1 << 20})
	if errors.Is(e, context.Canceled) {
		return nil
	}
	return e
}
