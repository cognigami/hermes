package controller

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"hermes/internal/contracts"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// PublishFiniteArtifact authorizes ONLY an original sealed finite member before
// delegating to ordinary ingress. No caller-selected online/unknown assignment
// acquires output credit or issued transport rights through this operator API.
func (c *Controller) PublishFiniteArtifact(ctx context.Context, bundleDigest, memberDigest, assignmentID, digest string, size int64, body io.Reader) error {
	if !ingressDigest(bundleDigest) || !ingressDigest(memberDigest) {
		return fmt.Errorf("%w: finite artifact scope", ErrFiniteInvalid)
	}
	var original string
	e := c.db.QueryRowContext(ctx, `SELECT m.assignment_id FROM finite_members m JOIN finite_exports e ON e.revision=m.export_revision WHERE e.digest=? AND m.assignment_digest=? AND m.assignment_id=?`, bundleDigest, memberDigest, assignmentID).Scan(&original)
	if errors.Is(e, sql.ErrNoRows) {
		return fmt.Errorf("%w: not an original sealed finite member", ErrFiniteInvalid)
	}
	if e != nil {
		return e
	}
	return c.PublishArtifact(ctx, digest, size, original, body)
}

func (c *Controller) finiteAssignment(tx *sql.Tx, bundleDigest string, receipt contracts.Receipt) (contracts.Assignment, string, error) {
	var a contracts.Assignment
	var digest string
	if !ingressDigest(bundleDigest) {
		return a, digest, fmt.Errorf("%w: bundle digest", ErrFiniteInvalid)
	}
	e := tx.QueryRow(`SELECT m.assignment_digest FROM finite_members m JOIN finite_exports e ON e.revision=m.export_revision WHERE e.digest=? AND m.assignment_id=?`, bundleDigest, receipt.AssignmentID).Scan(&digest)
	if errors.Is(e, sql.ErrNoRows) {
		return a, digest, fmt.Errorf("%w: not an original exported member", ErrFiniteInvalid)
	}
	if e != nil {
		return a, digest, e
	}
	a, e = c.bound(tx, receipt)
	if e != nil {
		return a, digest, e
	}
	return a, digest, nil
}
func finiteAckFromBytes(body []byte) (contracts.FiniteImportAck, error) {
	var ack contracts.FiniteImportAck
	if e := contracts.Decode(bytes.NewReader(body), &ack); e != nil {
		return ack, e
	}
	return ack, ack.Validate()
}
func (c *Controller) FiniteAcknowledgment(ctx context.Context, bundleDigest string) (contracts.FiniteImportAck, error) {
	tx, e := c.db.BeginTx(ctx, nil)
	if e != nil {
		return contracts.FiniteImportAck{}, e
	}
	defer tx.Rollback()
	return finiteAck(tx, bundleDigest)
}
func finiteAck(tx *sql.Tx, bundleDigest string) (contracts.FiniteImportAck, error) {
	ack := contracts.FiniteImportAck{SchemaVersion: contracts.Version, BundleDigest: bundleDigest}
	if !ingressDigest(bundleDigest) {
		return ack, fmt.Errorf("%w: bundle digest", ErrFiniteInvalid)
	}
	var revision int64
	if e := tx.QueryRow("SELECT revision,bundle_id FROM finite_exports WHERE digest=?", bundleDigest).Scan(&revision, &ack.BundleID); e != nil {
		if errors.Is(e, sql.ErrNoRows) {
			return ack, fmt.Errorf("%w: unknown sealed bundle", ErrFiniteInvalid)
		}
		return ack, e
	}
	rows, e := tx.Query(`SELECT m.assignment_id,m.assignment_digest,a.reconciliation_revision,a.final_digest,a.resource_stop_ack FROM finite_members m JOIN finite_member_ack a ON a.assignment_id=m.assignment_id WHERE m.export_revision=? ORDER BY m.assignment_id`, revision)
	if e != nil {
		return ack, e
	}
	type member struct {
		id  string
		ack contracts.FiniteMemberAck
	}
	var members []member
	for rows.Next() {
		var m member
		var stop int
		if e = rows.Scan(&m.id, &m.ack.AssignmentDigest, &m.ack.ReconciliationRevision, &m.ack.FinalDigest, &stop); e != nil {
			rows.Close()
			return ack, e
		}
		m.ack.ResourceStopAcknowledged = stop == 1
		members = append(members, m)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return ack, e
	}
	for _, m := range members {
		rows, e := tx.Query("SELECT candidate_digest FROM finite_raw_ack WHERE assignment_id=? ORDER BY candidate_digest", m.id)
		if e != nil {
			return ack, e
		}
		for rows.Next() {
			var d string
			if e = rows.Scan(&d); e != nil {
				rows.Close()
				return ack, e
			}
			m.ack.RawPublished = append(m.ack.RawPublished, d)
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return ack, e
		}
		ack.Members = append(ack.Members, m.ack)
	}
	return ack, ack.Validate()
}

// ImportFiniteCandidate records raw publication only. It never manufactures a
// running reconciliation, execution receipt or application-valid acceptance.
func (c *Controller) ImportFiniteCandidate(ctx context.Context, bundleDigest string, report contracts.CandidateReport) (contracts.FiniteImportAck, error) {
	if e := report.Candidate.Validate(); e != nil {
		return contracts.FiniteImportAck{}, fmt.Errorf("%w: %v", ErrFiniteInvalid, e)
	}
	body, e := c.mutate(ctx, "finite:"+bundleDigest, report.RequestID, report, func(tx *sql.Tx) (any, error) {
		a, e := c.finiteCandidateScope(tx, bundleDigest, report)
		if e != nil {
			return nil, e
		}
		candidate := report.Candidate
		for _, file := range candidate.Files {
			var size int64
			e := tx.QueryRow("SELECT size FROM artifacts WHERE digest=?", file.Artifact.Digest).Scan(&size)
			if errors.Is(e, sql.ErrNoRows) {
				return nil, fmt.Errorf("%w: candidate bytes not durably published", ErrFiniteUnavailable)
			}
			if e != nil {
				return nil, e
			}
			if size != file.Artifact.Size {
				return nil, fmt.Errorf("%w: candidate artifact size mismatch", ErrFiniteInvalid)
			}
		}
		if _, e = c.candidate(tx, report); e != nil {
			return nil, e
		}
		digest, e := contracts.Digest(candidate)
		if e != nil {
			return nil, e
		}
		if _, e = tx.Exec("INSERT OR IGNORE INTO finite_raw_ack VALUES(?,?)", a.AssignmentID, digest); e != nil {
			return nil, e
		}
		return finiteAck(tx, bundleDigest)
	})
	if e != nil {
		return contracts.FiniteImportAck{}, e
	}
	return finiteAckFromBytes(body)
}

// ImportFiniteFinal imports only terminal/quiescent or explicit uncertainty
// evidence, never routine active progress. Stop+reconciliation+final commit in
// one transaction so no old inadequate retry can race the typed resource cause.
func (c *Controller) ImportFiniteFinal(ctx context.Context, inventory contracts.FiniteFinalInventory) (contracts.FiniteImportAck, error) {
	if e := inventory.Validate(); e != nil {
		return contracts.FiniteImportAck{}, fmt.Errorf("%w: %v", ErrFiniteInvalid, e)
	}
	if !inventory.Uncertain && !inventory.Reconciliation.Quiescent {
		return contracts.FiniteImportAck{}, fmt.Errorf("%w: active progress is not finite final evidence", ErrFiniteInvalid)
	}
	r := inventory.Reconciliation
	body, e := c.mutate(ctx, "finite:"+inventory.BundleDigest, r.RequestID, inventory, func(tx *sql.Tx) (any, error) {
		a, digest, e := c.finiteAssignment(tx, inventory.BundleDigest, r.Receipt)
		if e != nil {
			return nil, e
		}
		if digest != inventory.AssignmentDigest {
			return nil, fmt.Errorf("%w: original assignment digest", ErrFiniteInvalid)
		}
		if e = inventory.ValidateForAssignment(a); e != nil {
			return nil, fmt.Errorf("%w: %v", ErrFiniteInvalid, e)
		}
		completed := map[string]bool{}
		if inventory.Final != nil {
			for _, id := range inventory.Final.Completed {
				completed[id] = true
			}
		}
		delivered := map[string]bool{}
		if inventory.Final != nil {
			for _, candidateDigest := range r.CandidateDigests {
				var unit string
				e = tx.QueryRow("SELECT unit_id FROM candidates WHERE digest=? AND assignment_id=?", candidateDigest, a.AssignmentID).Scan(&unit)
				if errors.Is(e, sql.ErrNoRows) {
					return nil, fmt.Errorf("%w: retained raw inventory still pending", ErrFiniteUnavailable)
				}
				if e != nil {
					return nil, e
				}
				if inventory.Final != nil && !completed[unit] {
					return nil, fmt.Errorf("%w: raw result contradicts final partition", ErrFiniteInvalid)
				}
				delivered[unit] = true
			}
		}
		if inventory.Final != nil {
			for id := range completed {
				if !delivered[id] {
					return nil, fmt.Errorf("%w: completed unit lacks matching raw inventory", ErrFiniteUnavailable)
				}
			}
		}
		if inventory.ResourceStop != nil {
			if _, e = c.resourceStop(tx, *inventory.ResourceStop); e != nil {
				return nil, e
			}
		}
		if _, e = c.reconcile(tx, r); e != nil {
			return nil, e
		}
		// A stale/terminal short-circuit cannot acknowledge a new report it did not
		// actually persist. Exact original retry is handled by mutate before here.
		var actual []byte
		if e = tx.QueryRow("SELECT manifest FROM execution_reports WHERE assignment_id=?", a.AssignmentID).Scan(&actual); e != nil {
			return nil, e
		}
		if !bytes.Equal(actual, encoded(r)) {
			return nil, fmt.Errorf("%w: stale finite reconciliation", ErrFiniteInvalid)
		}
		if inventory.Uncertain {
			if _, e = tx.Exec("UPDATE execution_reports SET state='uncertain' WHERE assignment_id=?", a.AssignmentID); e != nil {
				return nil, e
			}
		}
		finalDigest := ""
		if inventory.Final != nil {
			if _, e = c.final(tx, *inventory.Final); e != nil {
				return nil, e
			}
			finalDigest, e = contracts.Digest(*inventory.Final)
			if e != nil {
				return nil, e
			}
		}
		if _, e = tx.Exec("UPDATE finite_member_ack SET reconciliation_revision=max(reconciliation_revision,?),final_digest=CASE WHEN ?!='' THEN ? ELSE final_digest END,resource_stop_ack=max(resource_stop_ack,?) WHERE assignment_id=?", r.Revision, finalDigest, finalDigest, inventory.ResourceStop != nil, a.AssignmentID); e != nil {
			return nil, e
		}
		return finiteAck(tx, inventory.BundleDigest)
	})
	if e != nil {
		return contracts.FiniteImportAck{}, e
	}
	return finiteAckFromBytes(body)
}

func (c *Controller) finiteCandidateScope(tx *sql.Tx, bundleDigest string, report contracts.CandidateReport) (contracts.Assignment, error) {
	a, _, e := c.finiteAssignment(tx, bundleDigest, report.Receipt)
	if e != nil {
		return a, e
	}
	v := report.Candidate
	if e = v.Validate(); e != nil {
		return a, fmt.Errorf("%w: %v", ErrFiniteInvalid, e)
	}
	match := false
	for _, ref := range a.Units {
		if ref.UnitID == v.UnitID && ref.SpecDigest == v.SpecDigest {
			match = true
		}
	}
	if !match || v.AssignmentID != a.AssignmentID || v.AttemptID != a.AssignmentID {
		return a, fmt.Errorf("%w: candidate outside original member", ErrFiniteInvalid)
	}
	return a, nil
}
func finiteOpenRegular(root *os.Root, path string) (*os.File, error) {
	f, e := root.OpenFile(path, os.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW, 0)
	if e != nil {
		return nil, e
	}
	info, e := f.Stat()
	if e != nil || !info.Mode().IsRegular() {
		f.Close()
		return nil, fmt.Errorf("%w: finite input must be regular", ErrFiniteInvalid)
	}
	return f, nil
}
func finiteReadMetadata(root *os.Root, path string, dst any) error {
	f, e := finiteOpenRegular(root, path)
	if e != nil {
		return e
	}
	defer f.Close()
	return contracts.Decode(f, dst)
}
func finiteRecordFiles(root *os.Root, directory string) ([]string, error) {
	f, e := root.OpenFile(directory, os.O_RDONLY|unix.O_NONBLOCK|unix.O_DIRECTORY|unix.O_NOFOLLOW, 0)
	if os.IsNotExist(e) {
		return nil, nil
	}
	if e != nil {
		return nil, e
	}
	defer f.Close()
	entries, e := f.ReadDir(contracts.MaxFiniteMembers*contracts.MaxUnits + 1)
	if e != nil && !errors.Is(e, io.EOF) {
		return nil, e
	}
	if len(entries) > contracts.MaxFiniteMembers*contracts.MaxUnits {
		return nil, fmt.Errorf("%w: excessive finite records", ErrFiniteInvalid)
	}
	var paths []string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		name := strings.TrimSuffix(entry.Name(), ".json")
		if entry.IsDir() || name == entry.Name() || !ingressDigest(name) {
			return nil, fmt.Errorf("%w: invalid finite record filename", ErrFiniteInvalid)
		}
		paths = append(paths, filepath.Join(directory, entry.Name()))
	}
	sort.Strings(paths)
	return paths, nil
}

// ImportFiniteDirectory is a local file-transport convenience over the same
// durable ingress/candidate/lifecycle APIs. Each committed raw/final ACK remains
// recoverable even if a later file is absent/corrupt. Caller must preserve and
// return the partial ACK on error; this method never writes the supplied tree.
func (c *Controller) ImportFiniteDirectory(ctx context.Context, path string) (contracts.FiniteImportAck, error) {
	var ack contracts.FiniteImportAck
	root, e := os.OpenRoot(path)
	if e != nil {
		return ack, e
	}
	defer root.Close()
	var envelope contracts.FiniteBundleEnvelope
	if e = finiteReadMetadata(root, "bundle.json", &envelope); e != nil {
		return ack, e
	}
	if e = envelope.Validate(); e != nil {
		return ack, e
	}
	ack, e = c.FiniteAcknowledgment(ctx, envelope.BundleDigest)
	if e != nil {
		return ack, e
	}
	candidates, e := finiteRecordFiles(root, "candidates")
	if e != nil {
		return ack, e
	}
	for _, path := range candidates {
		var report contracts.CandidateReport
		if e = finiteReadMetadata(root, path, &report); e != nil {
			return ack, e
		}
		digest, e := contracts.Digest(report.Candidate)
		if e != nil {
			return ack, e
		}
		if filepath.Base(path) != digest+".json" {
			return ack, fmt.Errorf("%w: candidate filename/digest mismatch", ErrFiniteInvalid)
		}
		tx, e := c.db.BeginTx(ctx, nil)
		if e != nil {
			return ack, e
		}
		_, e = c.finiteCandidateScope(tx, envelope.BundleDigest, report)
		tx.Rollback()
		if e != nil {
			return ack, e
		}
		for _, file := range report.Candidate.Files {
			src, e := finiteOpenRegular(root, filepath.Join("blobs", file.Artifact.Digest))
			if e != nil {
				return ack, e
			}
			memberDigest := ""
			for _, m := range envelope.Bundle.Members {
				if m.Work.Assignment.AssignmentID == report.AssignmentID {
					memberDigest = m.AssignmentDigest
					break
				}
			}
			e = c.PublishFiniteArtifact(ctx, envelope.BundleDigest, memberDigest, report.AssignmentID, file.Artifact.Digest, file.Artifact.Size, src)
			src.Close()
			if e != nil {
				return ack, e
			}
		}
		_, e = c.ImportFiniteCandidate(ctx, envelope.BundleDigest, report)
		if e != nil {
			return ack, e
		}
		current, err := c.FiniteAcknowledgment(ctx, envelope.BundleDigest)
		if err != nil {
			return ack, err
		}
		ack = current
	}
	finals, e := finiteRecordFiles(root, "finals")
	if e != nil {
		return ack, e
	}
	for _, path := range finals {
		var inventory contracts.FiniteFinalInventory
		if e = finiteReadMetadata(root, path, &inventory); e != nil {
			return ack, e
		}
		if inventory.BundleDigest != envelope.BundleDigest || filepath.Base(path) != inventory.AssignmentDigest+".json" {
			return ack, fmt.Errorf("%w: final filename/bundle mismatch", ErrFiniteInvalid)
		}
		_, e = c.ImportFiniteFinal(ctx, inventory)
		if e != nil {
			return ack, e
		}
		current, err := c.FiniteAcknowledgment(ctx, envelope.BundleDigest)
		if err != nil {
			return ack, err
		}
		ack = current
	}
	return ack, nil
}
