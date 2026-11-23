package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"hermes/internal/controller"
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
	root := flag.String("root", "", "state root")
	listen := flag.String("listen", "127.0.0.1:8443", "listen address")
	ca := flag.String("ca", "", "client CA")
	cert := flag.String("cert", "", "server certificate")
	key := flag.String("key", "", "server key")
	worker := flag.String("workers", "synthetic-local", "comma-separated permitted workers")
	validator := flag.String("validator", "", "absolute synthetic validator executable")
	flag.Parse()
	b, e := os.ReadFile(*ca)
	if e != nil {
		return e
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(b) {
		return fmt.Errorf("invalid CA")
	}
	c, e := controller.Open(controller.Config{Root: *root, Workers: strings.Split(*worker, ","), Runtime: "synthetic-v1", Validator: "synthetic-v1", Profile: "synthetic-local", ValidatorArgv: []string{*validator}, MaxArtifactBytes: 1 << 20})
	if e != nil {
		return e
	}
	defer c.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	server := &http.Server{Addr: *listen, Handler: c.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, MaxHeaderBytes: 16 << 10, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS13, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert}}
	validation := make(chan error, 1)
	go func() { validation <- c.RunValidators(ctx) }()
	served := make(chan error, 1)
	go func() { served <- server.ListenAndServeTLS(*cert, *key) }()
	validationDone, serverDone := false, false
	select {
	case e = <-validation:
		validationDone = true
	case e = <-served:
		serverDone = true
	case <-ctx.Done():
		e = nil
	}
	stop()
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if x := server.Shutdown(shutdown); x != nil {
		return x
	}
	if !validationDone {
		if x := <-validation; e == nil && !errors.Is(x, context.Canceled) {
			e = x
		}
	}
	if !serverDone {
		if x := <-served; e == nil && !errors.Is(x, http.ErrServerClosed) {
			e = x
		}
	}
	if errors.Is(e, context.Canceled) || errors.Is(e, http.ErrServerClosed) {
		return nil
	}
	return e
}
