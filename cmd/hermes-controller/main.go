package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hermes/internal/contracts"
	"hermes/internal/controller"
	"hermes/internal/platform"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	if len(os.Args) > 1 && os.Args[1] == "check-quiescence" {
		return runQuiescenceInspection(os.Args[2:], os.Stdout)
	}
	version := flag.Bool("version", false, "report build and supported capability information")
	root := flag.String("root", "", "state root")
	listen := flag.String("listen", "127.0.0.1:8443", "listen address")
	ca := flag.String("ca", "", "client CA")
	cert := flag.String("cert", "", "server certificate")
	key := flag.String("key", "", "server key")
	worker := flag.String("workers", "synthetic-local", "comma-separated permitted workers")
	validator := flag.String("validator", "", "absolute synthetic validator executable")
	policyFile := flag.String("resource-policy", "", "approved versioned resource policy JSON file")
	workflowFile := flag.String("workflow-adapter-config", "", "optional controller-local workflow adapter approval JSON")
	finiteFile := flag.String("finite-runtime-config", "", "optional controller-local finite runtime approval JSON")
	maxArtifactBytes := flag.Int64("max-artifact-bytes", 1<<20, "maximum bytes per artifact (positive int64 below MaxInt64; resource policy still applies)")
	flag.Parse()
	if *version {
		return json.NewEncoder(os.Stdout).Encode(platform.Current())
	}
	if e := validateMaxArtifactBytes(*maxArtifactBytes); e != nil {
		return e
	}
	var approval *controller.WorkflowAdapterConfig
	if *workflowFile != "" {
		source, err := os.Open(*workflowFile)
		if err != nil {
			return err
		}
		cfg, err := controller.LoadWorkflowAdapterConfig(source)
		source.Close()
		if err != nil {
			return err
		}
		approval = &cfg
	}
	policySource, e := os.Open(*policyFile)
	if e != nil {
		return e
	}
	var policy contracts.ResourcePolicy
	e = contracts.Decode(policySource, &policy)
	policySource.Close()
	if e != nil {
		return e
	}
	b, e := os.ReadFile(*ca)
	if e != nil {
		return e
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(b) {
		return fmt.Errorf("invalid CA")
	}
	c, e := controller.Open(controller.Config{Root: *root, ResourcePolicy: &policy, Workers: strings.Split(*worker, ","), Runtime: "synthetic-v1", Validator: "synthetic-v1", Profile: "synthetic-local", ValidatorArgv: []string{*validator}, MaxArtifactBytes: *maxArtifactBytes})
	if e != nil {
		return e
	}
	defer c.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if e = configureFiniteRuntimes(ctx, c, *finiteFile); e != nil {
		return e
	}
	if e = c.InstallWorkflows(ctx); e != nil {
		return e
	}
	server := &http.Server{Addr: *listen, Handler: c.HandlerWithWorkflows(approval), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, MaxHeaderBytes: 16 << 10, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS13, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert}}
	validation := make(chan error, 1)
	go func() { validation <- c.RunValidators(ctx) }()
	var workflows <-chan error
	if approval != nil {
		done := make(chan error, 1)
		workflows = done
		go func() { done <- c.RunWorkflows(ctx, *approval) }()
	}
	served := make(chan error, 1)
	go func() { served <- server.ListenAndServeTLS(*cert, *key) }()
	validationDone, workflowsDone, serverDone := false, approval == nil, false
	select {
	case e = <-validation:
		validationDone = true
	case e = <-workflows:
		workflowsDone = true
	case e = <-served:
		serverDone = true
	case <-ctx.Done():
		e = nil
	}
	stop()
	if errors.Is(e, context.Canceled) || errors.Is(e, http.ErrServerClosed) {
		e = nil
	}
	record := func(err error) {
		if e == nil && err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, http.ErrServerClosed) {
			e = err
		}
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if x := server.Shutdown(shutdown); x != nil {
		record(x)
		// Interrupt active connections, but still join both service loops before
		// deferred controller Close releases database and state-root authority.
		record(server.Close())
	}
	if !validationDone {
		record(<-validation)
	}
	if !workflowsDone {
		record(<-workflows)
	}
	if !serverDone {
		record(<-served)
	}
	return e
}
