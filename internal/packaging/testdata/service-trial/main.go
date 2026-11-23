// Test-only bounded native service exercise. Never invoked by ordinary tests in
// privileged modes. No accounts, product APIs or production daemons are added.
package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"golang.org/x/sys/unix"
	"hermes/internal/contracts"
	"hermes/testdata/synthetic"
	"io"
	"math/big"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type config struct {
	Approved            bool     `json:"approved"`
	ControllerExtraArgs []string `json:"controller_extra_args"`
	AgentExtraArgs      []string `json:"agent_extra_args"`
}
type trial struct {
	Root, Account, Endpoint string
	Config                  config
}
type journal struct {
	Work       contracts.Work        `json:"work"`
	Started    []string              `json:"started"`
	Candidates []contracts.Candidate `json:"candidates"`
	Quiescent  bool                  `json:"quiescent"`
}

func main() {
	ctx := context.Background()
	stop := func() {}
	// Validators retain ordinary signal termination; orchestration cancels.
	if len(os.Args) > 1 && !strings.HasPrefix(os.Args[1], "--") {
		ctx, stop = signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	}
	defer stop()
	if e := run(ctx); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run(parent context.Context) error {
	args := os.Args[1:]
	if len(args) > 0 && strings.HasPrefix(args[0], "--") {
		return validator(args)
	}
	if len(args) == 0 {
		return errors.New("usage: trial-helper create BUNDLE ACCOUNT | exercise ROOT | cleanup ROOT | prepare-inputs ROOT | hold-lock ROOT READY")
	}
	switch args[0] {
	case "prepare-inputs":
		if len(args) != 2 {
			return errors.New("prepare-inputs requires root")
		}
		return prepareInputs(args[1])
	case "create":
		if len(args) != 3 {
			return errors.New("create requires bundle and existing account")
		}
		return create(parent, args[1], args[2])
	case "exercise", "cleanup":
		if len(args) != 2 {
			return errors.New("root required")
		}
		if os.Geteuid() != 0 {
			return errors.New("operator root invocation required")
		}
		var t trial
		if e := readJSON(filepath.Join(args[1], "trial.json"), &t); e != nil {
			return e
		}
		if t.Root != args[1] || !strings.HasPrefix(t.Root, "/Library/HermesServiceTrial-") {
			return errors.New("not an owned trial root")
		}
		duration := 180 * time.Second
		if args[0] == "cleanup" {
			duration = 30 * time.Second
		}
		ctx, cancel := context.WithTimeout(parent, duration)
		defer cancel()
		if args[0] == "cleanup" {
			return t.cleanup(ctx)
		}
		return t.exercise(ctx)
	case "hold-lock":
		if len(args) != 3 {
			return errors.New("hold-lock requires root and ready file")
		}
		return holdLock(parent, args[1], args[2])
	case "test-boundary":
		if len(args) != 3 {
			return errors.New("test-boundary requires root and timeout millis")
		}
		ms, e := strconv.Atoi(args[2])
		if e != nil {
			return e
		}
		ctx, cancel := context.WithTimeout(parent, time.Duration(ms)*time.Millisecond)
		defer cancel()
		exe, e := os.Executable()
		if e != nil {
			return e
		}
		_, e = command(ctx, "sh", "-c", `"$1" test-command-child "$2"; result=$?; exit "$result"`, "trial-test", exe, args[1])
		return e
	case "test-command-child":
		if len(args) != 2 {
			return errors.New("child root required")
		}
		return commandChild(args[1])
	case "test-cleanup-proof":
		return lateStopProof(parent, true)
	case "test-cleanup-noop":
		return lateStopProof(parent, false)
	case "test-busy-proof":
		if len(args) != 2 {
			return errors.New("capture file required")
		}
		b, e := os.ReadFile(args[1])
		if e != nil {
			return e
		}
		_, failure := command(parent, "sh", "-c", "exit 1")
		return requireBusyGate(b, failure)
	default:
		return errors.New("unknown fixture mode")
	}
}
func writeJSON(path string, v any) error {
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	return os.WriteFile(path, b, 0600)
}
func readJSON(path string, v any) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()
	return contracts.Decode(f, v)
}
func command(ctx context.Context, name string, args ...string) ([]byte, error) {
	c := exec.CommandContext(ctx, name, args...)
	// Only our synchronous scripts/foreground utilities use this owned group;
	// no detached/setsid child contract or product containment is implied.
	c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	c.WaitDelay = 250 * time.Millisecond
	c.Cancel = func() error {
		err := unix.Kill(-c.Process.Pid, unix.SIGKILL)
		if errors.Is(err, unix.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	b, e := c.CombinedOutput()
	if e != nil {
		return b, fmt.Errorf("%s: %w: %s", name, e, b)
	}
	return b, nil
}
func wait(ctx context.Context, description string, fn func() (bool, error)) error {
	for {
		if e := ctx.Err(); e != nil {
			return fmt.Errorf("%s: %w", description, e)
		}
		ok, e := fn()
		if e != nil {
			return fmt.Errorf("%s: %w", description, e)
		}
		if e := ctx.Err(); e != nil {
			return fmt.Errorf("%s: %w", description, e)
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: %w", description, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}
func proveLeafCleanup(parent context.Context, window time.Duration, submit func(context.Context) error, held func() (bool, error), stop func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(parent, window)
	defer cancel()
	if e := submit(ctx); e != nil {
		return e
	}
	if e := wait(ctx, "validator lock held within evidence window", held); e != nil {
		return e
	}
	if e := stop(ctx); e != nil {
		return e
	}
	return wait(ctx, "validator release within pre-timeout evidence window", func() (bool, error) { locked, e := held(); return !locked, e })
}
func requireBusyGate(output []byte, failure error) error {
	var exited *exec.ExitError
	if !errors.As(failure, &exited) || exited.ExitCode() != 1 {
		return errors.New("uninstall did not exit through the expected failure gate")
	}
	text := string(output)
	if !strings.Contains(text, "local quiescence busy: host authority is active") || !strings.Contains(text, "quiescence unknown/busy; uninstall incomplete") {
		return errors.New("uninstall failed before the expected inspector busy gate")
	}
	for _, line := range strings.Split(text, "\n") {
		var record struct {
			SchemaVersion int    `json:"schema_version"`
			State         string `json:"state"`
			Reason        string `json:"reason"`
		}
		if json.Unmarshal([]byte(line), &record) == nil && record.SchemaVersion == 1 && record.State == "busy" && record.Reason == "host authority is active" {
			return nil
		}
	}
	return errors.New("missing version-1 inspector busy record")
}
func (t trial) recoveryDigests() (map[string]string, error) {
	digests, e := t.stateDigests()
	if e != nil {
		return nil, e
	}
	for _, p := range []string{t.path("agent-install", "hermes-agent"), t.path("agent-install", "credentials", "ca.pem"), t.path("agent-install", "credentials", "cert.pem"), t.path("agent-install", "credentials", "key.pem"), t.path("agent-install", "state-root"), "/Library/LaunchDaemons/org.hermes.agent.plist"} {
		b, e := os.ReadFile(p)
		if e != nil {
			return nil, e
		}
		digests[p] = contracts.Hash(b)
	}
	return digests, nil
}

// Synthetic command child: no descendants or signals of its own. Its globally
// rooted lock closes only on process exit, including group cancellation.
func commandChild(root string) error {
	f, e := os.OpenFile(filepath.Join(root, "child.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return e
	}
	validatorLifetimeLock = f
	if e = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); e != nil {
		return e
	}
	if e = os.WriteFile(filepath.Join(root, "child-ready"), nil, 0600); e != nil {
		return e
	}
	time.Sleep(3 * time.Second)
	return os.WriteFile(filepath.Join(root, "child-continued"), nil, 0600)
}
func lateStopProof(ctx context.Context, slow bool) error {
	started := time.Now()
	return proveLeafCleanup(ctx, 30*time.Millisecond, func(context.Context) error { return nil }, func() (bool, error) { return time.Since(started) < 60*time.Millisecond, nil }, func(context.Context) error {
		if slow {
			time.Sleep(90 * time.Millisecond)
		}
		return nil
	})
}
func progress(s string) { fmt.Fprintln(os.Stderr, "service-trial:", s) }

func certificate(dir, name, cn string, ca *x509.Certificate, signer *ecdsa.PrivateKey) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if e != nil {
		return nil, nil, e
	}
	serial, e := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if e != nil {
		return nil, nil, e
	}
	c := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: cn}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(2 * time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}}
	if ca == nil {
		c.IsCA = true
		c.BasicConstraintsValid = true
		c.KeyUsage |= x509.KeyUsageCertSign
		ca = c
		signer = key
	}
	if name == "server" {
		c.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
		c.DNSNames = []string{"localhost"}
	}
	der, e := x509.CreateCertificate(rand.Reader, c, ca, &key.PublicKey, signer)
	if e != nil {
		return nil, nil, e
	}
	parsed, e := x509.ParseCertificate(der)
	if e != nil {
		return nil, nil, e
	}
	kb, e := x509.MarshalECPrivateKey(key)
	if e != nil {
		return nil, nil, e
	}
	if e = os.WriteFile(filepath.Join(dir, name+".pem"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); e != nil {
		return nil, nil, e
	}
	e = os.WriteFile(filepath.Join(dir, name+"-key.pem"), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}), 0600)
	return parsed, key, e
}
func prepareInputs(root string) error {
	dir := filepath.Join(root, "pki")
	if e := os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	ca, key, e := certificate(dir, "ca", "trial-ca", nil, nil)
	if e != nil {
		return e
	}
	for _, c := range []struct{ name, cn string }{{"server", "server"}, {"operator", "operator"}, {"worker", "worker:service-trial"}} {
		if _, _, e = certificate(dir, c.name, c.cn, ca, key); e != nil {
			return e
		}
	}
	return writeJSON(filepath.Join(root, "submission.json"), submission("initial", true))
}
func submission(run string, dependent bool) contracts.Submission {
	makeUnit := func(id string) contracts.Unit {
		return contracts.Unit{SchemaVersion: contracts.Version, UnitID: run + "-" + id, SpecDigest: contracts.Hash([]byte(run + "-" + id)), Operation: "synthetic-v1", Runtime: "synthetic-v1", Validator: "synthetic-v1"}
	}
	a := makeUnit("a")
	units := []contracts.Unit{a}
	p := contracts.Package{SchemaVersion: contracts.Version, PackageID: run + "-first", Units: []contracts.UnitRef{{UnitID: a.UnitID, SpecDigest: a.SpecDigest}}}
	pkgs := []contracts.Package{p}
	if dependent {
		b, c := makeUnit("b"), makeUnit("c")
		c.Dependencies = []string{a.UnitID, b.UnitID}
		units = append(units, b, c)
		pkgs[0].Units = append(pkgs[0].Units, contracts.UnitRef{UnitID: b.UnitID, SpecDigest: b.SpecDigest})
		pkgs = append(pkgs, contracts.Package{SchemaVersion: contracts.Version, PackageID: run + "-dependent", Units: []contracts.UnitRef{{UnitID: c.UnitID, SpecDigest: c.SpecDigest}}})
	}
	return contracts.Submission{SchemaVersion: contracts.Version, RequestID: "submit-" + run, RunID: run, Units: units, Packages: pkgs}
}

// Reuse the package's qualified classifier, not another launchd error parser.
func registered(ctx context.Context, serviceScript, kind string) (bool, error) {
	b, e := command(ctx, "sh", "-c", `. "$1"; hermes_role "$2"; if hermes_registered; then printf registered; else printf absent; fi`, "service-trial", serviceScript, kind)
	if e != nil {
		return false, e
	}
	switch string(b) {
	case "registered":
		return true, nil
	case "absent":
		return false, nil
	default:
		return false, errors.New("invalid package registration response")
	}
}
func copyFile(src, dst string, mode os.FileMode) error {
	st, e := os.Lstat(src)
	if e != nil {
		return e
	}
	if !st.Mode().IsRegular() {
		return fmt.Errorf("not regular: %s", src)
	}
	in, e := os.Open(src)
	if e != nil {
		return e
	}
	defer in.Close()
	out, e := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
	if e != nil {
		return e
	}
	if e = out.Chmod(mode); e != nil {
		out.Close()
		return e
	}
	_, e = io.Copy(out, in)
	closeErr := out.Close()
	if e != nil {
		return e
	}
	return closeErr
}
func verifyPackageBinary(dir, name string) error {
	b, e := os.ReadFile(filepath.Join(dir, "SHA256SUMS"))
	if e != nil {
		return e
	}
	fields := strings.Fields(string(b))
	if len(fields) != 2 || fields[1] != name {
		return errors.New("invalid package checksum manifest")
	}
	f, e := os.Open(filepath.Join(dir, name))
	if e != nil {
		return e
	}
	defer f.Close()
	h := sha256.New()
	if _, e = io.Copy(h, f); e != nil {
		return e
	}
	if hex.EncodeToString(h.Sum(nil)) != fields[0] {
		return errors.New("package binary checksum mismatch")
	}
	return nil
}
func create(parent context.Context, bundle, account string) error {
	if os.Geteuid() != 0 {
		return errors.New("operator root invocation required")
	}
	if account == "" || strings.ContainsAny(account, "/\x00\r\n") || strings.HasPrefix(account, "-") {
		return errors.New("invalid existing account")
	}
	ctx, cancel := context.WithTimeout(parent, 15*time.Second)
	defer cancel()
	uidBytes, e := command(ctx, "id", "-u", account)
	if e != nil {
		return e
	}
	uid, e := strconv.Atoi(strings.TrimSpace(string(uidBytes)))
	if e != nil || uid == 0 {
		return errors.New("existing nonroot account required")
	}
	var cfg config
	if e = readJSON(filepath.Join(bundle, "trial-config.json"), &cfg); e != nil {
		return e
	}
	if !cfg.Approved {
		return errors.New("trial config not approved: inspector/S4 integration remains gated")
	}
	for _, kind := range []string{"agent", "controller"} {
		present, e := registered(ctx, filepath.Join(bundle, "agent", "service.sh"), kind)
		if e != nil {
			return e
		}
		if present {
			return errors.New("refusing pre-existing Hermes service")
		}
		if _, e = os.Lstat("/Library/LaunchDaemons/org.hermes." + kind + ".plist"); !errors.Is(e, os.ErrNotExist) {
			return errors.New("refusing existing or unknown Hermes plist")
		}
	}
	var nonce [12]byte
	if _, e = rand.Read(nonce[:]); e != nil {
		return e
	}
	root := "/Library/HermesServiceTrial-" + hex.EncodeToString(nonce[:])
	if e = os.Mkdir(root, 0755); e != nil {
		return e
	}
	if e = os.Chmod(root, 0755); e != nil {
		return e
	}
	// On failure preserve the dedicated root for inspection, never delete unknown files.
	for _, name := range []string{"bin", "agent-package", "controller-package", "config"} {
		if e = os.Mkdir(filepath.Join(root, name), 0755); e != nil {
			return e
		}
		if e = os.Chmod(filepath.Join(root, name), 0755); e != nil {
			return e
		}
	}
	for _, kind := range []string{"agent", "controller"} {
		for _, name := range []string{"hermes-" + kind, "install.sh", "uninstall.sh", "lifecycle.sh", "service.plist", "service.sh", "SHA256SUMS", "build-info.txt"} {
			mode := os.FileMode(0644)
			if name == "hermes-"+kind {
				mode = 0755
			}
			if e = copyFile(filepath.Join(bundle, kind, name), filepath.Join(root, kind+"-package", name), mode); e != nil {
				return e
			}
		}
	}
	for _, name := range []string{"hermes", "hermes-synthetic"} {
		if e = copyFile(filepath.Join(bundle, "bin", name), filepath.Join(root, "bin", name), 0755); e != nil {
			return e
		}
	}
	exe, e := os.Executable()
	if e != nil {
		return e
	}
	if e = copyFile(exe, filepath.Join(root, "bin", "trial-validator"), 0755); e != nil {
		return e
	}
	// Policy files are staged application-neutral metadata, never discovered host data.
	entries, e := os.ReadDir(filepath.Join(bundle, "config"))
	if e != nil && !errors.Is(e, os.ErrNotExist) {
		return e
	}
	if len(entries) > 16 {
		return errors.New("too many staged config files")
	}
	for _, entry := range entries {
		src := filepath.Join(bundle, "config", entry.Name())
		st, e := os.Lstat(src)
		if e != nil {
			return e
		}
		if !st.Mode().IsRegular() || st.Size() > contracts.MaxMetadataBytes {
			return errors.New("invalid staged metadata")
		}
		b, e := os.ReadFile(src)
		if e != nil {
			return e
		}
		b = bytes.ReplaceAll(b, []byte("@ROOT@"), []byte(root))
		destination := filepath.Join(root, "config", entry.Name())
		if e = os.WriteFile(destination, b, 0644); e != nil {
			return e
		}
		// These protected, nonsecret metadata files must be service-readable
		// even when the invoking operator's umask is 077.
		if e = os.Chmod(destination, 0644); e != nil {
			return e
		}
	}
	for _, kind := range []string{"agent", "controller"} {
		if e = verifyPackageBinary(filepath.Join(root, kind+"-package"), "hermes-"+kind); e != nil {
			return e
		}
	}
	if e = prepareInputs(root); e != nil {
		return e
	}
	if e = os.Chmod(filepath.Join(root, "pki"), 0755); e != nil {
		return e
	}
	for _, name := range []string{"ca.pem", "server.pem", "server-key.pem", "worker.pem", "worker-key.pem"} {
		if e = os.Chown(filepath.Join(root, "pki", name), uid, -1); e != nil {
			return e
		}
	}
	for _, name := range []string{"validator.lock", "validator-delay-ms"} {
		path := filepath.Join(root, "bin", name)
		if e = os.WriteFile(path, []byte("100"), 0600); e != nil {
			return e
		}
		if e = os.Chown(path, uid, -1); e != nil {
			return e
		}
	}
	// Reserve a loopback port while preparing; a bind race later is a failed trial,
	// never permission to kill another listener.
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		return e
	}
	endpoint := "https://" + listener.Addr().String()
	listener.Close()
	t := trial{Root: root, Account: account, Endpoint: endpoint, Config: cfg}
	if e = writeJSON(filepath.Join(root, "trial.json"), t); e != nil {
		return e
	}
	fmt.Println(root)
	return nil
}
func (t trial) path(parts ...string) string {
	return filepath.Join(append([]string{t.Root}, parts...)...)
}
func (t trial) cli(ctx context.Context, args ...string) ([]byte, error) {
	base := []string{"--url", t.Endpoint, "--ca", t.path("pki", "ca.pem"), "--cert", t.path("pki", "operator.pem"), "--key", t.path("pki", "operator-key.pem")}
	return command(ctx, t.path("bin", "hermes"), append(base, args...)...)
}
func (t trial) lifecycle(ctx context.Context, kind, operation string) error {
	_, e := t.lifecycleOutput(ctx, kind, operation)
	return e
}
func (t trial) lifecycleOutput(ctx context.Context, kind, operation string) ([]byte, error) {
	script := "lifecycle.sh"
	if operation == "uninstall" {
		script = "uninstall.sh"
	}
	args := []string{t.path(kind+"-package", script)}
	if script == "lifecycle.sh" {
		args = append(args, operation)
	}
	args = append(args, t.path(kind+"-install"), t.path(kind+"-state"))
	return command(ctx, "sh", args...)
}
func (t trial) install(ctx context.Context, kind string) error {
	args := []string{t.path(kind+"-package", "install.sh"), t.Account, t.path(kind + "-install"), t.path(kind + "-state"), t.path("pki", "ca.pem")}
	extra := t.Config.AgentExtraArgs
	if kind == "controller" {
		args = append(args, t.path("pki", "server.pem"), t.path("pki", "server-key.pem"), "--listen", strings.TrimPrefix(t.Endpoint, "https://"), "--workers", "service-trial", "--validator", t.path("bin", "trial-validator"))
		extra = t.Config.ControllerExtraArgs
	} else {
		args = append(args, t.path("pki", "worker.pem"), t.path("pki", "worker-key.pem"), "--worker", "service-trial", "--session", "service-trial-session", "--url", t.Endpoint, "--runtime", t.path("bin", "hermes-synthetic"), "--runtime-arg=--delay-millis", "--runtime-arg=1500")
	}
	for _, a := range extra {
		flagName := strings.TrimLeft(strings.SplitN(a, "=", 2)[0], "-")
		switch flagName {
		case "root", "ca", "cert", "key", "listen", "workers", "worker", "session", "runtime", "runtime-arg", "url":
			return errors.New("staged extra args override fixture identity/loopback/runtime")
		}
		args = append(args, strings.ReplaceAll(a, "@ROOT@", t.Root))
	}
	if e := os.WriteFile(t.path("attempted-"+kind), nil, 0600); e != nil {
		return e
	}
	_, e := command(ctx, "sh", args...)
	return e
}
func (t trial) status(ctx context.Context, run string) (contracts.Status, error) {
	var s contracts.Status
	b, e := t.cli(ctx, "status", run)
	if e != nil {
		return s, e
	}
	e = json.Unmarshal(b, &s)
	return s, e
}
func (t trial) submit(ctx context.Context, run string, dependent bool) error {
	path := t.path("submit-" + run + ".json")
	if e := writeJSON(path, submission(run, dependent)); e != nil {
		return e
	}
	_, e := t.cli(ctx, "submit", path)
	return e
}
func (t trial) control(ctx context.Context, action string, revision int64) error {
	r := contracts.ControlRequest{SchemaVersion: contracts.Version, RequestID: fmt.Sprintf("trial-%s-%d", action, revision), TargetKind: "worker", TargetID: "service-trial", ExpectedRevision: revision, Action: action}
	if action == "drain" {
		r.TerminateAt = time.Now().Add(10 * time.Second)
	}
	path := t.path("control-" + action + ".json")
	if e := writeJSON(path, r); e != nil {
		return e
	}
	_, e := t.cli(ctx, "control", path)
	return e
}
func (t trial) journals() ([]journal, error) {
	byAssignment := map[string]journal{}
	for _, pattern := range []string{"journal.json", "active/*/journal.json", "completed/*/journal.json"} {
		paths, e := filepath.Glob(t.path("agent-state", pattern))
		if e != nil {
			return nil, e
		}
		for _, path := range paths {
			var j journal
			b, e := os.ReadFile(path)
			if errors.Is(e, os.ErrNotExist) {
				continue
			} // Concurrent atomic retirement.
			if e != nil {
				return nil, e
			}
			if e = json.Unmarshal(b, &j); e != nil {
				return nil, e
			}
			if j.Work.Assignment == nil {
				return nil, errors.New("fixture journal has no assignment")
			}
			byAssignment[j.Work.Assignment.AssignmentID] = j
		}
	}
	out := make([]journal, 0, len(byAssignment))
	for _, j := range byAssignment {
		out = append(out, j)
	}
	return out, nil
}
func (t trial) waitAccepted(ctx context.Context, run string, n int) error {
	return wait(ctx, "acceptance "+run, func() (bool, error) {
		s, e := t.status(ctx, run)
		if e != nil {
			return false, nil
		}
		return s.Accepted == n && s.Quiescent, nil
	})
}
func (t trial) check(ctx context.Context) error {
	_, e := command(ctx, t.path("agent-install", "hermes-agent"), "check-quiescence", "--root", t.path("agent-state"))
	return e
}
func (t trial) stateDigests() (map[string]string, error) {
	out := map[string]string{}
	e := filepath.WalkDir(t.path("agent-state"), func(path string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() {
			return nil
		}
		if !d.Type().IsRegular() {
			return errors.New("unexpected nonregular trial state")
		}
		b, e := os.ReadFile(path)
		if e != nil {
			return e
		}
		out[path] = contracts.Hash(b)
		return nil
	})
	return out, e
}
func sameDigests(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for p, d := range a {
		if b[p] != d {
			return false
		}
	}
	return true
}
func (t trial) exercise(ctx context.Context) error {
	progress("install real separate controller and agent services")
	if e := t.install(ctx, "controller"); e != nil {
		return e
	}
	if e := t.install(ctx, "agent"); e != nil {
		return e
	}
	for _, kind := range []string{"controller", "agent"} {
		if e := t.install(ctx, kind); e != nil {
			return e
		}
	}
	if e := wait(ctx, "controller ready", func() (bool, error) {
		_, e := t.cli(ctx, "status", "not-yet-submitted")
		return e != nil && strings.Contains(e.Error(), "unknown run"), nil
	}); e != nil {
		return e
	}
	if e := t.submit(ctx, "initial", true); e != nil {
		return e
	}
	if e := wait(ctx, "first grant", func() (bool, error) {
		js, e := t.journals()
		if e != nil {
			return false, e
		}
		for _, j := range js {
			if len(j.Started) > 0 {
				return true, nil
			}
		}
		return false, nil
	}); e != nil {
		return e
	}
	progress("crash controller only; agent must continue issued work")
	if _, e := command(ctx, "launchctl", "kill", "SIGKILL", "system/org.hermes.controller"); e != nil {
		return e
	}
	if e := wait(ctx, "controller outage observed", func() (bool, error) { _, e := t.status(ctx, "initial"); return e != nil, nil }); e != nil {
		return e
	}
	if e := wait(ctx, "two locally retained issued candidates", func() (bool, error) {
		js, e := t.journals()
		if e != nil {
			return false, e
		}
		for _, j := range js {
			if len(j.Candidates) >= 2 {
				return true, nil
			}
		}
		return false, nil
	}); e != nil {
		return e
	}
	if e := t.waitAccepted(ctx, "initial", 3); e != nil {
		return e
	}
	counts := map[string]int{}
	js, e := t.journals()
	if e != nil {
		return e
	}
	for _, j := range js {
		for _, id := range j.Started {
			counts[id]++
		}
	}
	for _, u := range submission("initial", true).Units {
		if counts[u.UnitID] != 1 {
			return errors.New("accepted prefix replayed or missing grant")
		}
	}
	progress("stop controller while exact leaf validator holds its lifetime lock")
	if e = os.WriteFile(t.path("bin", "validator-delay-ms"), []byte("5000"), 0600); e != nil {
		return e
	}
	// The entire submission/lock/stop/release window is < the validator's
	// natural 5s delay AND the existing controller validator 5s timeout.
	if e = proveLeafCleanup(ctx, 4*time.Second,
		func(proof context.Context) error { return t.submit(proof, "validator-cleanup", false) },
		func() (bool, error) { return lockHeld(t.path("bin", "validator.lock")) },
		func(proof context.Context) error { return t.lifecycle(proof, "controller", "stop") }); e != nil {
		return e
	}
	if e = os.WriteFile(t.path("bin", "validator-delay-ms"), []byte("100"), 0600); e != nil {
		return e
	}
	if e = t.lifecycle(ctx, "controller", "start"); e != nil {
		return e
	}
	if e = t.waitAccepted(ctx, "validator-cleanup", 1); e != nil {
		return e
	}
	progress("drain, persistent stop, explicit start without clearing pause")
	if e = t.control(ctx, "drain", 0); e != nil {
		return e
	}
	if e = wait(ctx, "drain complete", func() (bool, error) {
		b, e := t.cli(ctx, "control-status", "worker:service-trial")
		if e != nil {
			return false, nil
		}
		var s contracts.ControlStatus
		if e = json.Unmarshal(b, &s); e != nil {
			return false, e
		}
		return s.State == "complete", nil
	}); e != nil {
		return e
	}
	if e = t.lifecycle(ctx, "agent", "stop"); e != nil {
		return e
	}
	if e = t.check(ctx); e != nil {
		return e
	}
	stableUntil := time.Now().Add(12 * time.Second)
	if e = wait(ctx, "disabled service remains absent", func() (bool, error) {
		present, e := registered(ctx, t.path("agent-package", "service.sh"), "agent")
		if e != nil {
			return false, e
		}
		if present {
			return false, errors.New("stopped service respawned")
		}
		return time.Now().After(stableUntil), nil
	}); e != nil {
		return e
	}
	if e = t.lifecycle(ctx, "agent", "stop"); e != nil {
		return e
	}
	if e = t.submit(ctx, "paused", false); e != nil {
		return e
	}
	if e = t.lifecycle(ctx, "agent", "start"); e != nil {
		return e
	}
	pauseUntil := time.Now().Add(2 * time.Second)
	if e = wait(ctx, "start retains issuance pause", func() (bool, error) {
		s, e := t.status(ctx, "paused")
		if e != nil {
			return false, e
		}
		if !s.Quiescent || s.Accepted != 0 {
			return false, errors.New("service start cleared persisted pause")
		}
		return time.Now().After(pauseUntil), nil
	}); e != nil {
		return e
	}
	if e = t.control(ctx, "resume", 1); e != nil {
		return e
	}
	if e = t.waitAccepted(ctx, "paused", 1); e != nil {
		return e
	}
	if e = t.control(ctx, "drain", 2); e != nil {
		return e
	}
	if e = wait(ctx, "second drain complete", func() (bool, error) {
		b, e := t.cli(ctx, "control-status", "worker:service-trial")
		if e != nil {
			return false, nil
		}
		var s contracts.ControlStatus
		e = json.Unmarshal(b, &s)
		return s.State == "complete", e
	}); e != nil {
		return e
	}
	if e = t.lifecycle(ctx, "agent", "stop"); e != nil {
		return e
	}
	progress("nondestructive busy-root uninstall refusal, then positive cleanup")
	if e = t.check(ctx); e != nil {
		return e
	}
	exe, e := os.Executable()
	if e != nil {
		return e
	}
	ready := t.path("lock-ready")
	child := exec.CommandContext(ctx, exe, "hold-lock", t.path("agent-state"), ready)
	if e = child.Start(); e != nil {
		return e
	}
	defer func() {
		if child.ProcessState == nil {
			child.Process.Kill()
			child.Wait()
		}
	}()
	if e = wait(ctx, "fixture root lock held", func() (bool, error) { _, e := os.Stat(ready); return e == nil, nil }); e != nil {
		return e
	}
	if held, err := lockHeld(t.path("agent-state", "agent.lock")); err != nil || !held {
		return errors.New("fixture root lock not held before refused uninstall")
	}
	refusalBefore, e := t.recoveryDigests()
	if e != nil {
		return e
	}
	capture, refusalErr := t.lifecycleOutput(ctx, "agent", "uninstall")
	if e = requireBusyGate(capture, refusalErr); e != nil {
		return e
	}
	if held, err := lockHeld(t.path("agent-state", "agent.lock")); err != nil || !held {
		return errors.New("fixture root lock was not held through refused uninstall")
	}
	refusalAfter, e := t.recoveryDigests()
	if e != nil {
		return e
	}
	if !sameDigests(refusalBefore, refusalAfter) {
		return errors.New("busy refusal changed retained recovery bytes")
	}
	if e = os.WriteFile(ready+".release", nil, 0600); e != nil {
		return e
	}
	if e = child.Wait(); e != nil {
		return e
	}
	if e = t.check(ctx); e != nil {
		return e
	}
	before, e := t.stateDigests()
	if e != nil {
		return e
	}
	for _, kind := range []string{"agent", "controller"} {
		if e = t.lifecycle(ctx, kind, "uninstall"); e != nil {
			return e
		}
		if e = t.lifecycle(ctx, kind, "uninstall"); e != nil {
			return e
		}
	}
	after, e := t.stateDigests()
	if e != nil {
		return e
	}
	if !sameDigests(before, after) {
		return errors.New("uninstall changed retained state/results")
	}
	for _, kind := range []string{"agent", "controller"} {
		for _, p := range []string{t.path(kind+"-install", "hermes-"+kind), t.path(kind+"-install", "credentials", "key.pem"), "/Library/LaunchDaemons/org.hermes." + kind + ".plist"} {
			if _, e = os.Lstat(p); !errors.Is(e, os.ErrNotExist) {
				return errors.New("known installed file survived successful uninstall")
			}
		}
		present, e := registered(ctx, t.path("agent-package", "service.sh"), kind)
		if e != nil {
			return e
		}
		if present {
			return errors.New("service remained registered after uninstall")
		}
	}
	progress("reinstall preserved roots; do not replay accepted work")
	if e = t.install(ctx, "controller"); e != nil {
		return e
	}
	if e = t.install(ctx, "agent"); e != nil {
		return e
	}
	if e = t.waitAccepted(ctx, "initial", 3); e != nil {
		return e
	}
	if e = t.lifecycle(ctx, "agent", "stop"); e != nil {
		return e
	}
	if e = t.check(ctx); e != nil {
		return e
	}
	for _, kind := range []string{"agent", "controller"} {
		if e = t.lifecycle(ctx, kind, "uninstall"); e != nil {
			return e
		}
	}
	js, e = t.journals()
	if e != nil {
		return e
	}
	counts = map[string]int{}
	for _, j := range js {
		for _, id := range j.Started {
			counts[id]++
		}
	}
	for _, u := range submission("initial", true).Units {
		if counts[u.UnitID] != 1 {
			return errors.New("reinstall replayed accepted prefix")
		}
	}
	after, e = t.stateDigests()
	if e != nil {
		return e
	}
	identity := t.path("agent-state", "identity.json")
	if after[identity] != before[identity] {
		return errors.New("reinstall changed stable identity")
	}
	progress("PASS: leaf-only phases 1–4; no agent crash, reboot or login transition exercised")
	return os.WriteFile(t.path("PASS"), []byte("leaf-only service trial; no reboot/login/agent-crash qualification\n"), 0600)
}
func (t trial) cleanup(ctx context.Context) error {
	var result error
	for _, kind := range []string{"agent", "controller"} {
		if _, e := os.Stat(t.path("attempted-" + kind)); errors.Is(e, os.ErrNotExist) {
			continue
		}
		plist := "/Library/LaunchDaemons/org.hermes." + kind + ".plist"
		if _, e := os.Lstat(plist); errors.Is(e, os.ErrNotExist) {
			present, q := registered(ctx, t.path("agent-package", "service.sh"), kind)
			if q != nil || present {
				result = errors.New("cleanup registration unknown/foreign; preserve files")
			}
			continue
		}
		b, e := command(ctx, "plutil", "-extract", "ProgramArguments.0", "raw", "-o", "-", plist)
		if e != nil || strings.TrimSpace(string(b)) != t.path(kind+"-install", "hermes-"+kind) {
			result = errors.New("cleanup refused foreign/unknown plist")
			continue
		}
		if e = t.lifecycle(ctx, kind, "stop"); e != nil {
			result = e
		}
	}
	return result
}
func lockHeld(path string) (bool, error) {
	f, e := os.OpenFile(path, os.O_RDWR, 0)
	if e != nil {
		return false, e
	}
	defer f.Close()
	e = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if errors.Is(e, unix.EWOULDBLOCK) || errors.Is(e, unix.EAGAIN) {
		return true, nil
	}
	if e != nil {
		return false, e
	}
	return false, unix.Flock(int(f.Fd()), unix.LOCK_UN)
}
func holdLock(parent context.Context, root, ready string) error {
	f, e := os.OpenFile(filepath.Join(root, "agent.lock"), os.O_RDWR, 0)
	if e != nil {
		return e
	}
	defer f.Close()
	if e = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); e != nil {
		return e
	}
	if e = os.WriteFile(ready, nil, 0600); e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(parent, 15*time.Second)
	defer cancel()
	return wait(ctx, "lock release", func() (bool, error) { _, e := os.Stat(ready + ".release"); return e == nil, nil })
}

// Intentionally held by a global GC root and never explicitly closed: only
// this fixture leaf's process exit may release its validator lifetime lock.
var validatorLifetimeLock *os.File

func validator(args []string) error {
	flags := flag.NewFlagSet("trial-validator", flag.ContinueOnError)
	manifest := flags.String("manifest", "", "candidate")
	root := flags.String("root", "", "result root")
	unit := flags.String("unit", "", "pinned unit")
	if e := flags.Parse(args); e != nil {
		return e
	}
	var c contracts.Candidate
	var u contracts.Unit
	if e := readJSON(*manifest, &c); e != nil {
		return e
	}
	if e := c.Validate(); e != nil {
		return e
	}
	if e := readJSON(*unit, &u); e != nil {
		return e
	}
	if e := u.Validate(); e != nil {
		return e
	}
	exe, e := os.Executable()
	if e != nil {
		return e
	}
	dir := filepath.Dir(exe)
	f, e := os.OpenFile(filepath.Join(dir, "validator.lock"), os.O_RDWR, 0)
	if e != nil {
		return e
	}
	validatorLifetimeLock = f
	if e = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); e != nil {
		return e
	}
	delayBytes, e := os.ReadFile(filepath.Join(dir, "validator-delay-ms"))
	if e != nil {
		return e
	}
	delay, e := strconv.Atoi(string(delayBytes))
	if e != nil || delay < 0 || delay > 5000 {
		return errors.New("invalid bounded fixture delay")
	}
	time.Sleep(time.Duration(delay) * time.Millisecond)
	valid := c.UnitID == u.UnitID && c.SpecDigest == u.SpecDigest && c.Outcome == "complete" && len(c.Files) == 1
	for _, ref := range c.Files {
		b, e := synthetic.ReadFile(*root, ref.Path)
		if e != nil {
			return e
		}
		sum := sha256.Sum256(b)
		if hex.EncodeToString(sum[:]) != ref.Artifact.Digest || int64(len(b)) != ref.Artifact.Size {
			return errors.New("candidate bytes mismatch")
		}
		var r synthetic.Result
		if e = contracts.Decode(bytes.NewReader(b), &r); e != nil {
			return e
		}
		valid = valid && r.UnitID == u.UnitID && r.SpecDigest == u.SpecDigest && r.Value == synthetic.Expected(u.UnitID, u.SpecDigest)
	}
	return json.NewEncoder(os.Stdout).Encode(contracts.Verdict{SchemaVersion: contracts.Version, Valid: valid, Reason: "isolated synthetic service trial"})
}
