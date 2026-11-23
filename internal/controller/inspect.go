package controller

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hermes/internal/contracts"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

// LocalQuiescence describes a read-only snapshot, not a process-control action.
// Lifecycle operations must exclude manual starts after this snapshot returns.
type LocalQuiescence struct {
	SchemaVersion      int    `json:"schema_version"`
	State              string `json:"state"`
	Reason             string `json:"reason,omitempty"`
	UnresolvedChildren int    `json:"unresolved_children"`
}

// CheckLocalQuiescence never opens Controller authority, initializes SQLite or
// infers child exit from absent PIDs, expired deadlines or the parent lock.
func CheckLocalQuiescence(root string) (result LocalQuiescence, resultErr error) {
	result = LocalQuiescence{SchemaVersion: 1, State: "uncertain"}
	fail := func(reason string) (LocalQuiescence, error) {
		result.Reason = reason
		return result, errors.New(reason)
	}
	if !filepath.IsAbs(root) {
		return fail("absolute controller root required")
	}
	info, e := os.Lstat(root)
	if e != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fail("controller root missing or not a real directory")
	}
	fs, e := os.OpenRoot(root)
	if e != nil {
		return fail("controller root unavailable")
	}
	defer fs.Close()
	for _, name := range []string{"authority.lock", "metadata.sqlite", "metadata.sqlite-wal", "metadata.sqlite-shm", "metadata.sqlite-journal"} {
		info, e := fs.Lstat(name)
		optional := strings.HasSuffix(name, "-wal") || strings.HasSuffix(name, "-shm") || strings.HasSuffix(name, "-journal")
		if optional && errors.Is(e, os.ErrNotExist) {
			continue
		}
		if e != nil || !info.Mode().IsRegular() {
			return fail("controller metadata missing/nonregular: " + name)
		}
	}
	lock, e := fs.OpenFile("authority.lock", os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if e != nil {
		return fail("controller authority lock unavailable")
	}
	defer lock.Close()
	if info, e = lock.Stat(); e != nil || !info.Mode().IsRegular() {
		return fail("controller authority lock is not regular")
	}
	if e = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); e != nil {
		if errors.Is(e, unix.EWOULDBLOCK) || errors.Is(e, unix.EAGAIN) {
			result.State = "busy"
			return fail("controller authority is active")
		}
		return fail("controller inspection authority unavailable")
	}
	// With authority held, absent/empty WAL proves the main DB is the committed
	// snapshot. Only that checkpointed case may use immutable (no sidecar IO).
	// Nonempty WAL MUST be read with mode=ro/readonly_shm, never ignored; inability
	// to initialize a WAL index without writes remains uncertainty.
	uri := url.URL{Scheme: "file", Path: filepath.Join(root, "metadata.sqlite")}
	query := url.Values{"mode": {"ro"}, "readonly_shm": {"1"}, "_pragma": {"query_only(1)", "temp_store(MEMORY)", "busy_timeout(250)"}}
	if journal, err := fs.Lstat("metadata.sqlite-journal"); err == nil {
		if !journal.Mode().IsRegular() || journal.Size() != 0 {
			return fail("rollback journal requires recovery before inspection")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fail("rollback journal state unavailable")
	}
	wal, err := fs.Lstat("metadata.sqlite-wal")
	if errors.Is(err, os.ErrNotExist) {
		query.Set("immutable", "1")
	} else if err != nil || !wal.Mode().IsRegular() {
		return fail("WAL metadata unavailable")
	} else if wal.Size() == 0 {
		query.Set("immutable", "1")
	}
	uri.RawQuery = query.Encode()
	db, e := sql.Open("sqlite", uri.String())
	if e != nil {
		return fail("controller metadata unavailable read-only")
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	tx, e := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if e != nil {
		return fail("controller committed snapshot unavailable: " + e.Error())
	}
	defer tx.Rollback()
	var version int
	if e = tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); e != nil {
		return fail("committed controller metadata unreadable: " + e.Error())
	}
	if version != 0 {
		return fail("unknown controller metadata version")
	}
	for _, schema := range []struct {
		name    string
		columns []string
	}{
		{"validator_claims", []string{"id:TEXT:0:1", "started:INTEGER:1:0", "exited:INTEGER:1:0", "resources:BLOB:1:0", "instance:TEXT:1:0"}},
		{"resource_head", []string{"id:INTEGER:0:1", "revision:INTEGER:1:0"}},
		{"resource_policies", []string{"revision:INTEGER:0:1", "digest:TEXT:1:0", "manifest:BLOB:1:0"}},
		{"controller_retained", []string{"id:INTEGER:0:1", "bytes:INTEGER:1:0"}},
		{"controller_storage_totals", []string{"id:INTEGER:0:1", "bytes:INTEGER:1:0"}},
		{"controller_storage_init", []string{"id:INTEGER:0:1"}},
	} {
		if e = inspectionColumns(ctx, tx, schema.name, schema.columns); e != nil {
			return fail(e.Error())
		}
	}
	for _, statement := range []string{
		"SELECT bytes FROM controller_retained WHERE id=1",
		"SELECT bytes FROM controller_storage_totals WHERE id=1",
	} {
		var value int64
		if e = tx.QueryRowContext(ctx, statement).Scan(&value); e != nil || value < 0 {
			return fail("controller bootstrap accounting incomplete")
		}
	}
	var marker int
	if e = tx.QueryRowContext(ctx, "SELECT id FROM controller_storage_init WHERE id=1").Scan(&marker); e != nil || marker != 1 {
		return fail("controller tracking bootstrap incomplete")
	}
	var revision, length int64
	var digest string
	if e = tx.QueryRowContext(ctx, "SELECT p.revision,p.digest,length(p.manifest) FROM resource_head h JOIN resource_policies p ON p.revision=h.revision WHERE h.id=1").Scan(&revision, &digest, &length); e != nil || revision < 1 || length <= 0 || length > contracts.MaxMetadataBytes {
		return fail("controller policy bootstrap incomplete")
	}
	var policyBytes []byte
	if e = tx.QueryRowContext(ctx, "SELECT manifest FROM resource_policies WHERE revision=?", revision).Scan(&policyBytes); e != nil {
		return fail("controller policy metadata unavailable")
	}
	var policy contracts.ResourcePolicy
	if e = contracts.Decode(bytes.NewReader(policyBytes), &policy); e != nil || policy.Validate() != nil || policy.Revision != revision {
		return fail("unknown/invalid controller policy metadata")
	}
	for _, host := range policy.Hosts {
		if host.MaxAssignments < 1 || host.MaxAssignments > 64 {
			return fail("incomplete controller policy host bounds")
		}
	}
	actual, e := contracts.Digest(policy)
	if e != nil || actual != digest {
		return fail("controller policy identity mismatch")
	}
	// Index range seeks detect unknown exit flags without scanning exited history.
	for _, predicate := range []string{"exited IS NULL", "exited<0", "exited>1", "exited>0 AND exited<1"} {
		var found int
		e = tx.QueryRowContext(ctx, "SELECT 1 FROM validator_claims INDEXED BY validator_live WHERE "+predicate+" LIMIT 1").Scan(&found)
		if e == nil {
			return fail("unknown child exit proof flag")
		}
		if !errors.Is(e, sql.ErrNoRows) {
			return fail("child exit index unavailable")
		}
	}
	rows, e := tx.QueryContext(ctx, "SELECT id,started,instance,CASE WHEN length(resources)<=65536 THEN resources ELSE NULL END FROM validator_claims INDEXED BY validator_live WHERE exited=0 LIMIT 65")
	if e != nil {
		return fail("child tracking metadata unavailable")
	}
	count := 0
	for rows.Next() {
		count++
		if count > 64 {
			rows.Close()
			return fail("unresolved child inventory exceeds inspection bound")
		}
		var id, instance string
		var started int
		var encoded []byte
		if e = rows.Scan(&id, &started, &instance, &encoded); e != nil {
			rows.Close()
			return fail("invalid child tracking fields")
		}
		if id == "" || len(id) > 256 || instance == "" || len(instance) > 256 || (started != 0 && started != 1) {
			rows.Close()
			return fail("incomplete child tracking identity/proof")
		}
		if e = inspectionClaim(encoded); e != nil {
			rows.Close()
			return fail(e.Error())
		}
		if started == 1 {
			result.UnresolvedChildren++
		}
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return fail("committed child inventory unreadable")
	}
	if result.UnresolvedChildren > 0 {
		return fail("controller child exit remains unproven")
	}
	result.State = "quiescent"
	return result, nil
}

func inspectionColumns(ctx context.Context, tx *sql.Tx, table string, expected []string) error {
	var kind string
	if e := tx.QueryRowContext(ctx, "SELECT type FROM sqlite_schema WHERE name=?", table).Scan(&kind); e != nil || kind != "table" {
		return errors.New("controller tracking table missing/unknown: " + table)
	}
	rows, e := tx.QueryContext(ctx, "PRAGMA table_info("+table+")")
	if e != nil {
		return e
	}
	defer rows.Close()
	index := 0
	for rows.Next() {
		var cid, notnull, pk int
		var name, kind string
		var def sql.NullString
		if e = rows.Scan(&cid, &name, &kind, &notnull, &def, &pk); e != nil {
			return e
		}
		if index >= len(expected) || cid != index || fmt.Sprintf("%s:%s:%d:%d", name, strings.ToUpper(kind), notnull, pk) != expected[index] {
			return errors.New("unknown controller child tracking schema")
		}
		if (name == "started" || name == "exited") && (!def.Valid || def.String != "0") {
			return errors.New("unknown child proof defaults")
		}
		index++
	}
	if e = rows.Err(); e != nil {
		return e
	}
	if index != len(expected) {
		return errors.New("controller child tracking schema missing")
	}
	return nil
}
func inspectionClaim(encoded []byte) error {
	var record struct {
		Role      *string
		Reference *string
		Resources *struct {
			CPUMilli    *int64 `json:"cpu_milli"`
			MemoryBytes *int64 `json:"memory_bytes"`
			SpoolBytes  *int64 `json:"spool_bytes"`
		}
	}
	if e := contracts.Decode(bytes.NewReader(encoded), &record); e != nil {
		return errors.New("unknown/malformed child resource fields")
	}
	if record.Role == nil || (*record.Role != "validator" && *record.Role != "continuation") || record.Reference == nil || len(*record.Reference) > 256 || record.Resources == nil || record.Resources.CPUMilli == nil || record.Resources.MemoryBytes == nil || record.Resources.SpoolBytes == nil {
		return errors.New("incomplete child resource tracking record")
	}
	v := contracts.ResourceVector{CPUMilli: *record.Resources.CPUMilli, MemoryBytes: *record.Resources.MemoryBytes, SpoolBytes: *record.Resources.SpoolBytes}
	if e := v.Validate(); e != nil {
		return e
	}
	return nil
}
