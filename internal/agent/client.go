package agent

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"hermes/internal/contracts"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

var ErrProtocol = errors.New("controller protocol failure")

type HTTPError struct {
	Status int
	Body   string
}

func (e *HTTPError) Error() string { return fmt.Sprintf("HTTP %d: %s", e.Status, e.Body) }

type Client struct {
	URL  string
	HTTP *http.Client
}

func NewClient(url, ca, cert, key string) (*Client, error) {
	if !strings.HasPrefix(url, "https://") {
		return nil, errors.New("HTTPS required")
	}
	b, e := os.ReadFile(ca)
	if e != nil {
		return nil, e
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(b) {
		return nil, errors.New("invalid CA")
	}
	identity, e := tls.LoadX509KeyPair(cert, key)
	if e != nil {
		return nil, e
	}
	return &Client{URL: strings.TrimRight(url, "/"), HTTP: &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, Certificates: []tls.Certificate{identity}}}}}, nil
}
func (c *Client) Do(ctx context.Context, method, path string, body any, dst any) error {
	var reader io.Reader
	if body != nil {
		b, e := json.Marshal(body)
		if e != nil {
			return e
		}
		reader = bytes.NewReader(b)
	}
	req, e := http.NewRequestWithContext(ctx, method, c.URL+path, reader)
	if e != nil {
		return e
	}
	req.Header.Set("Content-Type", "application/json")
	return c.send(req, dst)
}
func (c *Client) send(req *http.Request, dst any) error {
	r, e := c.HTTP.Do(req)
	if e != nil {
		return e
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		b, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
		return &HTTPError{Status: r.StatusCode, Body: string(b)}
	}
	if dst == nil {
		var a contracts.Acknowledgment
		if e = contracts.Decode(r.Body, &a); e != nil {
			return fmt.Errorf("%w: %v", ErrProtocol, e)
		}
		if a.SchemaVersion != 1 {
			return fmt.Errorf("%w: unsupported acknowledgment version", ErrProtocol)
		}
		valid := false
		switch req.URL.Path {
		case "/v1/runs":
			valid = a.RunID != ""
		case "/v1/receipt", "/v1/control/ack", "/v1/lost-spool":
			valid = a.Received
		case "/v1/candidate":
			valid = len(a.RawPublished) == 64
		case "/v1/final":
			valid = a.Quiescent
		default:
			valid = req.Method == "PUT" && strings.HasPrefix(req.URL.Path, "/v1/artifacts/") && a.Published
		}
		if !valid {
			return fmt.Errorf("%w: missing explicit operation acknowledgment", ErrProtocol)
		}
		return nil
	}
	if e = contracts.Decode(r.Body, dst); e != nil {
		return fmt.Errorf("%w: %v", ErrProtocol, e)
	}
	return nil
}
func (c *Client) Publish(ctx context.Context, digest string, size int64, assignment string, reader io.Reader) error {
	path := "/v1/artifacts/" + digest
	if assignment != "" {
		path += "?assignment=" + assignment
	}
	r, e := http.NewRequestWithContext(ctx, "PUT", c.URL+path, reader)
	if e != nil {
		return e
	}
	r.Header.Set("X-Artifact-Size", fmt.Sprint(size))
	return c.send(r, nil)
}
