package controller

import (
	"hermes/internal/contracts"
	"testing"
	"time"
)

func reservationFixture(t *testing.T) (*Controller, string) {
	t.Helper()
	c, _ := ingressFixture(t)
	seedFixtureInventory(t, c)
	s := submission("reservation")
	s.Units = s.Units[:2]
	s.Packages = s.Packages[:1]
	second := s.Packages[0]
	second.PackageID = "second"
	second.Units = append([]contracts.UnitRef(nil), second.Units[1:]...)
	s.Packages[0].Units = s.Packages[0].Units[:1]
	s.Packages = append(s.Packages, second)
	tx, e := c.db.Begin()
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback()
	if _, e = c.submit(tx, s); e != nil {
		t.Fatal(e)
	}
	if e = tx.Commit(); e != nil {
		t.Fatal(e)
	}
	return c, s.RunID
}
func reserveForTest(t *testing.T, c *Controller, key string, bounds *reservationBounds) contracts.Work {
	t.Helper()
	tx, e := c.db.Begin()
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback()
	w, e := c.reserveReadyAssignment(tx, reservationInventory{Worker: "local", Session: "fixture-inventory", Key: key, Bounds: bounds})
	if e != nil {
		t.Fatal(e)
	}
	if e = tx.Commit(); e != nil {
		t.Fatal(e)
	}
	return w
}
func reservationControl(t *testing.T, c *Controller, kind, target, action string, revision int64) {
	t.Helper()
	tx, e := c.db.Begin()
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback()
	r := contracts.ControlRequest{SchemaVersion: 1, TargetKind: kind, TargetID: target, Action: action, ExpectedRevision: revision}
	if action == "drain" {
		r.TerminateAt = time.Now().Add(time.Minute)
	}
	if _, e = c.control(tx, r); e != nil {
		t.Fatal(e)
	}
	if e = tx.Commit(); e != nil {
		t.Fatal(e)
	}
}
func TestCommonFiniteReservationDoesNotInventOnlineReceipt(t *testing.T) {
	c, _ := reservationFixture(t)
	now := time.Now().UTC()
	bounds := &reservationBounds{Stop: now.Add(10 * time.Second), Terminate: now.Add(20 * time.Second), Handoff: now.Add(30 * time.Second)}
	for _, key := range []string{"first", "second"} {
		w := reserveForTest(t, c, key, bounds)
		if w.Assignment == nil {
			t.Fatal("missing independent member")
		}
		a := w.Assignment
		if !a.StopNewUnitsAt.Equal(bounds.Stop) || !a.TerminateAt.Equal(bounds.Terminate) || !a.HandoffDeadline.Equal(bounds.Handoff) {
			t.Fatal("fixed session bounds extended", a)
		}
		var receipt int
		if e := c.db.QueryRow("SELECT receipt FROM assignments WHERE id=?", a.AssignmentID).Scan(&receipt); e != nil || receipt != 0 {
			t.Fatal("finite reservation invented online acknowledgment", receipt, e)
		}
	}
	for _, table := range []string{"assignments", "owners", "allocation_reservations"} {
		if got := ingressCount(t, c, table); got != 2 {
			t.Fatal(table, got)
		}
	}
	var attempts int
	if e := c.db.QueryRow("SELECT sum(attempts) FROM unit_attempts").Scan(&attempts); e != nil || attempts != 2 {
		t.Fatal(attempts, e)
	}
	// The connected path still rejects missing original receipts/inventory.
	tx, e := c.db.Begin()
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback()
	if _, e = c.poll(tx, contracts.PollRequest{SchemaVersion: 1, RequestID: "online", WorkerID: "local", SessionID: "fixture-inventory"}); e == nil {
		t.Fatal("online preconditions accidentally removed")
	}
}
func TestCommonFiniteReservationHonorsWorkerAndRunPause(t *testing.T) {
	c, run := reservationFixture(t)
	reservationControl(t, c, "worker", "local", "drain", 0)
	if w := reserveForTest(t, c, "worker-paused", nil); w.Assignment != nil {
		t.Fatal("finite bypassed worker pause")
	}
	reservationControl(t, c, "worker", "local", "resume", 1)
	reservationControl(t, c, "run", run, "drain", 0)
	if w := reserveForTest(t, c, "run-paused", nil); w.Assignment != nil {
		t.Fatal("finite bypassed run pause")
	}
	if ingressCount(t, c, "owners") != 0 {
		t.Fatal("paused attempt reserved units")
	}
	reservationControl(t, c, "run", run, "resume", 1)
	if w := reserveForTest(t, c, "resumed", nil); w.Assignment == nil {
		t.Fatal("resume did not restore eligibility")
	}
}
func TestCommonFiniteReservationRefusesUncertainOwnership(t *testing.T) {
	c, _ := reservationFixture(t)
	w := reserveForTest(t, c, "owned", nil)
	if _, e := c.db.Exec("INSERT INTO execution_reports VALUES(?,'uncertain',?)", w.Assignment.AssignmentID, []byte(`{}`)); e != nil {
		t.Fatal(e)
	}
	tx, e := c.db.Begin()
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback()
	if _, e = c.reserveReadyAssignment(tx, reservationInventory{Worker: "local", Session: "fixture-inventory", Key: "not-authorized"}); e == nil {
		t.Fatal("finite bypassed uncertain worker ownership")
	}
	var count int
	if e = tx.QueryRow("SELECT count(*) FROM assignments").Scan(&count); e != nil || count != 1 {
		t.Fatal(count, e)
	}
}
