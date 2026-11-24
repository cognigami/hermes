package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hermes/internal/agent"
	"hermes/internal/artifacts"
	"hermes/internal/contracts"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/sys/unix"
)

func runFiniteCommand(ctx context.Context, client *agent.Client, args []string, out io.Writer) (bool, error) {
	if len(args) == 0 {
		return false, nil
	}
	switch args[0] {
	case "finite-export":
		if len(args) != 3 {
			return true, errors.New("usage: finite-export REQUEST.json OPERATOR_INPUT_DIRECTORY (operator-provisioned transport staging, no managed-host quota)")
		}
		input, e := os.OpenRoot(filepath.Dir(args[1]))
		if e != nil {
			return true, e
		}
		var request contracts.FiniteExportRequest
		e = finiteCLIMetadata(input, filepath.Base(args[1]), &request)
		input.Close()
		if e != nil {
			return true, e
		}
		if e = request.Validate(); e != nil {
			return true, e
		}
		var envelope contracts.FiniteBundleEnvelope
		if e = client.Do(ctx, "POST", "/v1/finite/export", request, &envelope); e != nil {
			return true, e
		}
		if e = validateFiniteExportReply(request, envelope); e != nil {
			return true, e
		}
		if e = writeFiniteExport(ctx, client, envelope, args[2]); e != nil {
			return true, fmt.Errorf("sealed bundle %s remains reserved; retry SAME request after transfer failure: %w", envelope.Bundle.BundleID, e)
		}
		return true, json.NewEncoder(out).Encode(envelope)
	case "finite-import":
		if len(args) != 2 {
			return true, errors.New("usage: finite-import OUTPUT_DIRECTORY")
		}
		return true, importFiniteOutput(ctx, client, args[1], out)
	}
	return false, nil
}
func validateFiniteExportReply(request contracts.FiniteExportRequest, envelope contracts.FiniteBundleEnvelope) error {
	if e := envelope.Validate(); e != nil {
		return e
	}
	b := envelope.Bundle
	if b.BundleID != request.BundleID || b.WorkerID != request.WorkerID || b.SessionID != request.SessionID || b.HostID != request.HostID || len(b.Members) > request.MemberLimit {
		return errors.New("finite export response/request scope mismatch")
	}
	for _, m := range b.Members {
		a := m.Work.Assignment
		if a.StopNewUnitsAt.After(request.StopNewUnitsAt) || a.TerminateAt.After(request.TerminateAt) || a.HandoffDeadline.After(request.HandoffDeadline) {
			return errors.New("finite export response extended requested deadlines")
		}
	}
	return nil
}

func finiteCLIRegular(root *os.Root, name string) (*os.File, error) {
	f, e := root.OpenFile(name, os.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW, 0)
	if e != nil {
		return nil, e
	}
	info, e := f.Stat()
	if e != nil || !info.Mode().IsRegular() {
		f.Close()
		return nil, errors.New("finite transport file is not regular")
	}
	return f, nil
}
func finiteCLIMetadata(root *os.Root, name string, dst any) error {
	f, e := finiteCLIRegular(root, name)
	if e != nil {
		return e
	}
	defer f.Close()
	return contracts.Decode(f, dst)
}
func finiteCLIWrite(root *os.Root, name string, body []byte, replace bool) error {
	temporary := ".finite-" + agent.ID()
	f, e := root.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	defer root.Remove(temporary)
	if _, e = f.Write(body); e != nil {
		f.Close()
		return e
	}
	if e = f.Sync(); e != nil {
		f.Close()
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	if replace {
		e = root.Rename(temporary, name)
	} else {
		e = root.Link(temporary, name)
		if e == nil {
			e = root.Remove(temporary)
		}
	}
	if e != nil {
		return e
	}
	dir, e := root.Open(".")
	if e != nil {
		return e
	}
	defer dir.Close()
	return dir.Sync()
}
func writeFiniteExport(ctx context.Context, client *agent.Client, envelope contracts.FiniteBundleEnvelope, destination string) error {
	if e := os.MkdirAll(destination, 0700); e != nil {
		return e
	}
	root, e := os.OpenRoot(destination)
	if e != nil {
		return e
	}
	defer root.Close()
	existing := false
	var prior contracts.FiniteBundleEnvelope
	if e = finiteCLIMetadata(root, "bundle.json", &prior); e == nil {
		if e = prior.Validate(); e != nil {
			return e
		}
		if prior.BundleDigest != envelope.BundleDigest {
			return errors.New("destination contains a different sealed bundle")
		}
		existing = true
	} else if !os.IsNotExist(e) {
		return e
	}
	max := int64(1)
	for _, ref := range envelope.Bundle.Blobs {
		if ref.Size > max {
			max = ref.Size
		}
	}
	store, e := artifacts.New(filepath.Join(destination, "blobs"), max)
	if e != nil {
		return e
	}
	for _, ref := range envelope.Bundle.Blobs {
		// A preexisting operator-supplied destination is not a verified cache. Check
		// its bytes at this explicit file-transfer boundary, never infer from stat.
		if old, e := finiteCLIRegular(root, filepath.Join("blobs", ref.Digest)); e == nil {
			h := sha256.New()
			n, err := io.Copy(h, old)
			old.Close()
			if err != nil {
				return err
			}
			if n != ref.Size || hex.EncodeToString(h.Sum(nil)) != ref.Digest {
				return errors.New("existing finite destination blob differs")
			}
		} else if !os.IsNotExist(e) {
			return e
		}
		request, e := http.NewRequestWithContext(ctx, "GET", client.URL+"/v1/artifacts/"+ref.Digest, nil)
		if e != nil {
			return e
		}
		response, e := client.HTTP.Do(request)
		if e != nil {
			return e
		}
		if response.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
			response.Body.Close()
			return fmt.Errorf("finite artifact GET %d: %s", response.StatusCode, b)
		}
		e = store.Publish(ctx, ref.Digest, ref.Size, response.Body)
		response.Body.Close()
		if e != nil {
			return e
		}
	}
	if existing {
		return nil
	}
	body, e := json.Marshal(envelope)
	if e != nil {
		return e
	}
	return finiteCLIWrite(root, "bundle.json", body, false)
}
func finiteCLIRecords(root *os.Root, directory string) ([]string, error) {
	dir, e := root.OpenFile(directory, os.O_RDONLY|unix.O_DIRECTORY|unix.O_NONBLOCK|unix.O_NOFOLLOW, 0)
	if os.IsNotExist(e) {
		return nil, nil
	}
	if e != nil {
		return nil, e
	}
	defer dir.Close()
	entries, e := dir.ReadDir(contracts.MaxFiniteMembers*contracts.MaxUnits + 1)
	if e != nil && !errors.Is(e, io.EOF) {
		return nil, e
	}
	if len(entries) > contracts.MaxFiniteMembers*contracts.MaxUnits {
		return nil, errors.New("too many finite output records")
	}
	paths := []string{}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		name := strings.TrimSuffix(entry.Name(), ".json")
		decoded, e := hex.DecodeString(name)
		if e != nil || len(decoded) != 32 || strings.ToLower(name) != name || name == entry.Name() || entry.IsDir() {
			return nil, errors.New("invalid finite output record filename")
		}
		paths = append(paths, filepath.Join(directory, entry.Name()))
	}
	sort.Strings(paths)
	return paths, nil
}
func mergeFiniteAck(envelope contracts.FiniteBundleEnvelope, old, next contracts.FiniteImportAck) (contracts.FiniteImportAck, error) {
	if e := next.Validate(); e != nil {
		return old, e
	}
	if next.BundleDigest != envelope.BundleDigest || next.BundleID != envelope.Bundle.BundleID {
		return old, errors.New("finite acknowledgment scope mismatch")
	}
	known := map[string]bool{}
	for _, m := range envelope.Bundle.Members {
		known[m.AssignmentDigest] = true
	}
	if old.SchemaVersion == 0 {
		old = contracts.FiniteImportAck{SchemaVersion: contracts.Version, BundleDigest: next.BundleDigest, BundleID: next.BundleID}
	}
	if old.BundleDigest != next.BundleDigest || old.BundleID != next.BundleID {
		return old, errors.New("existing finite acknowledgment scope mismatch")
	}
	byDigest := map[string]contracts.FiniteMemberAck{}
	for _, m := range old.Members {
		if !known[m.AssignmentDigest] {
			return old, errors.New("unknown acknowledged member")
		}
		byDigest[m.AssignmentDigest] = m
	}
	for _, m := range next.Members {
		if !known[m.AssignmentDigest] {
			return old, errors.New("unknown acknowledged member")
		}
		prior := byDigest[m.AssignmentDigest]
		prior.AssignmentDigest = m.AssignmentDigest
		seen := map[string]bool{}
		for _, d := range prior.RawPublished {
			seen[d] = true
		}
		for _, d := range m.RawPublished {
			if !seen[d] {
				prior.RawPublished = append(prior.RawPublished, d)
				seen[d] = true
			}
		}
		sort.Strings(prior.RawPublished)
		if m.ReconciliationRevision > prior.ReconciliationRevision {
			prior.ReconciliationRevision = m.ReconciliationRevision
		}
		if prior.FinalDigest != "" && m.FinalDigest != "" && prior.FinalDigest != m.FinalDigest {
			return old, errors.New("conflicting final acknowledgment")
		}
		if m.FinalDigest != "" {
			prior.FinalDigest = m.FinalDigest
		}
		prior.ResourceStopAcknowledged = prior.ResourceStopAcknowledged || m.ResourceStopAcknowledged
		byDigest[m.AssignmentDigest] = prior
	}
	old.Members = nil
	for _, m := range byDigest {
		old.Members = append(old.Members, m)
	}
	sort.Slice(old.Members, func(i, j int) bool { return old.Members[i].AssignmentDigest < old.Members[j].AssignmentDigest })
	return old, old.Validate()
}

// Finite output uploads carry their immutable export/member authority. Generic
// operator artifact-put deliberately has no assignment-credit semantics.
func publishFiniteCLIArtifact(ctx context.Context, client *agent.Client, bundleDigest, memberDigest, assignmentID string, artifact contracts.Artifact, body io.Reader) error {
	query := url.Values{"bundle": {bundleDigest}, "member": {memberDigest}, "assignment": {assignmentID}}
	request, e := http.NewRequestWithContext(ctx, "PUT", client.URL+"/v1/finite/artifacts/"+artifact.Digest+"?"+query.Encode(), body)
	if e != nil {
		return e
	}
	request.Header.Set("X-Artifact-Size", fmt.Sprint(artifact.Size))
	response, e := client.HTTP.Do(request)
	if e != nil {
		return e
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return &agent.HTTPError{Status: response.StatusCode, Body: string(b)}
	}
	var ack contracts.Acknowledgment
	if e = contracts.Decode(response.Body, &ack); e != nil {
		return fmt.Errorf("%w: %v", agent.ErrProtocol, e)
	}
	if ack.SchemaVersion != contracts.Version || !ack.Published {
		return fmt.Errorf("%w: missing explicit finite artifact publication ACK", agent.ErrProtocol)
	}
	return nil
}

func importFiniteOutput(ctx context.Context, client *agent.Client, path string, out io.Writer) (result error) {
	root, e := os.OpenRoot(path)
	if e != nil {
		return e
	}
	defer root.Close()
	var envelope contracts.FiniteBundleEnvelope
	if e = finiteCLIMetadata(root, "bundle.json", &envelope); e != nil {
		return e
	}
	if e = envelope.Validate(); e != nil {
		return e
	}
	var ack contracts.FiniteImportAck
	if e = finiteCLIMetadata(root, "ack.json", &ack); e != nil && !os.IsNotExist(e) {
		return e
	}
	if ack.SchemaVersion != 0 {
		if e = ack.Validate(); e != nil {
			return e
		}
	}
	persist := func(next contracts.FiniteImportAck) error {
		merged, e := mergeFiniteAck(envelope, ack, next)
		if e != nil {
			return e
		}
		b, e := json.Marshal(merged)
		if e != nil {
			return e
		}
		if e = finiteCLIWrite(root, "ack.json", b, true); e != nil {
			return e
		}
		ack = merged
		return nil
	}
	// Recover committed progress even after a lost previous HTTP reply.
	var current contracts.FiniteImportAck
	if e = client.Do(ctx, "GET", "/v1/finite/ack?bundle="+url.QueryEscape(envelope.BundleDigest), nil, &current); e != nil {
		return e
	}
	if e = persist(current); e != nil {
		return e
	}
	defer func() {
		if ack.Validate() == nil {
			result = errors.Join(result, json.NewEncoder(out).Encode(ack))
		}
	}()
	candidates, e := finiteCLIRecords(root, "candidates")
	if e != nil {
		return e
	}
	members := map[string]contracts.FiniteMember{}
	for _, m := range envelope.Bundle.Members {
		members[m.Work.Assignment.AssignmentID] = m
	}
	for _, name := range candidates {
		var report contracts.CandidateReport
		if e = finiteCLIMetadata(root, name, &report); e != nil {
			return e
		}
		if e = report.Candidate.Validate(); e != nil {
			return e
		}
		digest, e := contracts.Digest(report.Candidate)
		if e != nil {
			return e
		}
		if filepath.Base(name) != digest+".json" {
			return errors.New("candidate filename digest mismatch")
		}
		member, ok := members[report.AssignmentID]
		if !ok || report.WorkerID != envelope.Bundle.WorkerID || report.SessionID != envelope.Bundle.SessionID || report.Candidate.AssignmentID != report.AssignmentID {
			return errors.New("candidate outside sealed bundle")
		}
		match := false
		for _, u := range member.Work.Units {
			if u.UnitID == report.Candidate.UnitID && u.SpecDigest == report.Candidate.SpecDigest {
				match = true
			}
		}
		if !match {
			return errors.New("candidate outside original unit specification")
		}
		for _, file := range report.Candidate.Files {
			src, e := finiteCLIRegular(root, filepath.Join("blobs", file.Artifact.Digest))
			if e != nil {
				return e
			}
			e = publishFiniteCLIArtifact(ctx, client, envelope.BundleDigest, member.AssignmentDigest, report.AssignmentID, file.Artifact, src)
			src.Close()
			if e != nil {
				return e
			}
		}
		var next contracts.FiniteImportAck
		if e = client.Do(ctx, "POST", "/v1/finite/candidate?bundle="+url.QueryEscape(envelope.BundleDigest), report, &next); e != nil {
			return e
		}
		if e = persist(next); e != nil {
			return e
		}
	}
	finals, e := finiteCLIRecords(root, "finals")
	if e != nil {
		return e
	}
	for _, name := range finals {
		var inventory contracts.FiniteFinalInventory
		if e = finiteCLIMetadata(root, name, &inventory); e != nil {
			return e
		}
		if inventory.BundleDigest != envelope.BundleDigest || filepath.Base(name) != inventory.AssignmentDigest+".json" {
			return errors.New("finite lifecycle file scope mismatch")
		}
		member, ok := members[inventory.Reconciliation.AssignmentID]
		if !ok {
			return errors.New("unknown finite lifecycle member")
		}
		if e = inventory.ValidateForAssignment(*member.Work.Assignment); e != nil {
			return e
		}
		var next contracts.FiniteImportAck
		if e = client.Do(ctx, "POST", "/v1/finite/final", inventory, &next); e != nil {
			return e
		}
		if e = persist(next); e != nil {
			return e
		}
	}
	return nil
}
