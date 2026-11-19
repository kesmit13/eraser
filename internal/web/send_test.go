package web

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eraser-privacy/eraser/internal/broker"
	"github.com/eraser-privacy/eraser/internal/config"
	"github.com/eraser-privacy/eraser/internal/email"
	"github.com/eraser-privacy/eraser/internal/history"
	emaTemplate "github.com/eraser-privacy/eraser/internal/template"
)

type recordingSender struct {
	mu     sync.Mutex
	sent   []email.Message
	block  chan struct{} // if set, each Send waits for it to close
	onSend func()        // if set, runs during each Send (see setOnSend)
}

// setOnSend sets a hook that runs during each Send, e.g. to break history so
// the send can't be recorded.
func (r *recordingSender) setOnSend(fn func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onSend = fn
}

func (r *recordingSender) Name() string { return "recording" }
func (r *recordingSender) Send(_ context.Context, m email.Message) email.Result {
	if r.block != nil {
		<-r.block
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.onSend != nil {
		r.onSend()
	}
	r.sent = append(r.sent, m)
	return email.Result{Success: true, MessageID: fmt.Sprintf("<%d@example.com>", len(r.sent))}
}

func (r *recordingSender) recipients() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, m := range r.sent {
		out = append(out, m.To)
	}
	sort.Strings(out)
	return out
}

func (r *recordingSender) bodies() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, m := range r.sent {
		out = append(out, m.Body)
	}
	return out
}

type sendFixture struct {
	s      *Server
	h      http.Handler
	sender *recordingSender
	store  *history.Store
}

func newSendFixture(t *testing.T, opts config.Options, brokers []broker.Broker) *sendFixture {
	t.Helper()
	store, err := history.NewStore(filepath.Join(t.TempDir(), "history.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	engine, err := emaTemplate.NewEngine()
	if err != nil {
		t.Fatal(err)
	}
	cfg := testConfig()
	opts.RateLimitMs = 1
	cfg.Options = opts
	s, err := NewServer(testPort, cfg, filepath.Join(t.TempDir(), "config.yaml"),
		&broker.BrokerDatabase{Brokers: brokers}, store, engine)
	if err != nil {
		t.Fatal(err)
	}
	sender := &recordingSender{}
	s.newSender = func(config.EmailConfig) (email.Sender, error) { return sender, nil }
	return &sendFixture{s: s, h: s.setupRouter(), sender: sender, store: store}
}

func sameOrigin(r *http.Request) { r.Header.Set("Sec-Fetch-Site", "same-origin") }

func (f *sendFixture) sendAll(t *testing.T) (map[string]any, int) {
	t.Helper()
	return f.post(t, "/api/send-all", "confirmed=1000")
}

func (f *sendFixture) post(t *testing.T, path, form string) (map[string]any, int) {
	t.Helper()
	rec := do(f.h, http.MethodPost, path, func(r *http.Request) {
		sameOrigin(r)
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		r.Body = readCloser(form)
	})
	var body map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	return body, rec.Code
}

func (f *sendFixture) waitForJob(t *testing.T, id string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		job := f.s.jobManager.Get(id)
		if job == nil {
			t.Fatalf("job %s not found", id)
		}
		if st := job.ToJSON(); st["status"] != JobStatusRunning {
			return st
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("job did not finish")
	return nil
}

func brokersN(ids ...string) []broker.Broker {
	var out []broker.Broker
	for _, id := range ids {
		out = append(out, broker.Broker{ID: id, Name: strings.ToUpper(id), Email: "privacy@" + id + ".example", Region: "us"})
	}
	return out
}

func TestSendAllUsesSharedRules(t *testing.T) {
	bs := brokersN("keep", "excluded", "recent")
	bs = append(bs,
		broker.Broker{ID: "eu", Name: "EU", Email: "privacy@eu.example", Region: "eu"},
		broker.Broker{ID: "form", Name: "FORM", Email: "privacy@form.example", Region: "us", Tags: []string{broker.TagFormOnly}},
	)
	f := newSendFixture(t, config.Options{Template: "ccpa", Regions: []string{"us"}, ExcludedBrokers: []string{"excluded"}}, bs)
	// "recent" was sent successfully yesterday, inside the 30-day window.
	if err := f.store.Add(&history.Record{BrokerID: "recent", BrokerName: "RECENT", Email: "privacy@recent.example",
		Template: "generic", Status: history.StatusSent, SentAt: time.Now().Add(-24 * time.Hour)}); err != nil {
		t.Fatal(err)
	}

	// The broker page's default "pending" filter would already hide "recent";
	// ask for every status so the resend window is what excludes it.
	rec := do(f.h, http.MethodPost, "/api/send-all", func(r *http.Request) {
		sameOrigin(r)
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		r.Body = readCloser("status=all&confirmed=1")
	})
	var body map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %v", rec.Code, body)
	}
	f.waitForJob(t, body["job_id"].(string))

	if got := f.sender.recipients(); strings.Join(got, ",") != "privacy@keep.example" {
		t.Fatalf("sent to %v, want only privacy@keep.example", got)
	}
	if b := f.sender.bodies()[0]; !strings.Contains(b, "California") {
		t.Error("send-all ignored the configured ccpa template")
	}
}

func TestSendOneRefusesWhatSendAllWouldSkip(t *testing.T) {
	f := newSendFixture(t, config.Options{ExcludedBrokers: []string{"excluded"}}, brokersN("ok", "excluded", "recent"))
	if err := f.store.Add(&history.Record{BrokerID: "recent", BrokerName: "RECENT", Email: "privacy@recent.example",
		Template: "generic", Status: history.StatusSent, SentAt: time.Now().Add(-time.Hour)}); err != nil {
		t.Fatal(err)
	}
	for id, want := range map[string]string{"excluded": "excluded", "recent": "already sent"} {
		rec := do(f.h, http.MethodPost, "/api/send/"+id, sameOrigin)
		if rec.Code != http.StatusBadRequest || !strings.Contains(strings.ToLower(rec.Body.String()), want) {
			t.Errorf("%s: got %d %q, want 400 mentioning %q", id, rec.Code, rec.Body.String(), want)
		}
	}
	if rec := do(f.h, http.MethodPost, "/api/send/ok", sameOrigin); rec.Code != http.StatusOK {
		t.Fatalf("ok: got %d %s", rec.Code, rec.Body.String())
	}
	if got := f.sender.recipients(); strings.Join(got, ",") != "privacy@ok.example" {
		t.Fatalf("sent to %v", got)
	}
}

func TestSendAllRespectsDailyLimit(t *testing.T) {
	f := newSendFixture(t, config.Options{DailyLimit: 2}, brokersN("a", "b", "c", "d"))
	body, code := f.sendAll(t)
	if code != http.StatusOK {
		t.Fatalf("status %d: %v", code, body)
	}
	f.waitForJob(t, body["job_id"].(string))
	if n := len(f.sender.recipients()); n != 2 {
		t.Fatalf("sent %d, want 2 (daily limit)", n)
	}
	// A second click within 24h sends nothing more.
	if body, code := f.sendAll(t); code != http.StatusBadRequest {
		t.Fatalf("second send-all: status %d %v, want 400", code, body)
	}
}

// Run with -race: the job goroutine writes progress while handlers read it.
func TestJobStatusPollingIsRaceFree(t *testing.T) {
	var ids []string
	for i := range 20 {
		ids = append(ids, fmt.Sprintf("b%02d", i))
	}
	f := newSendFixture(t, config.Options{}, brokersN(ids...))
	body, code := f.sendAll(t)
	if code != http.StatusOK {
		t.Fatalf("status %d: %v", code, body)
	}
	jobID := body["job_id"].(string)

	// Poll until the job has finished, so reads overlap every write,
	// including the final status change.
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				do(f.h, http.MethodGet, "/api/job/active", nil)
				do(f.h, http.MethodGet, "/api/job/"+jobID+"/status", nil)
			}
		}
	}()
	f.waitForJob(t, jobID)
	time.Sleep(20 * time.Millisecond) // keep polling across the final update
	close(stop)
	<-done
}

// A double click (or two tabs) must not start two jobs from the same plan:
// every broker would get two emails and the daily cap would be exceeded.
func TestConcurrentSendAllStartsOneJob(t *testing.T) {
	var ids []string
	for i := range 10 {
		ids = append(ids, fmt.Sprintf("b%02d", i))
	}
	for attempt := range 5 {
		f := newSendFixture(t, config.Options{}, brokersN(ids...))
		const n = 8
		start := make(chan struct{})
		codes := make(chan int, n)
		jobIDs := make(chan string, n)
		var wg sync.WaitGroup
		for range n {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				body, code := f.sendAll(t)
				codes <- code
				if id, ok := body["job_id"].(string); ok && code == http.StatusOK {
					jobIDs <- id
				}
			}()
		}
		close(start)
		wg.Wait()
		close(codes)
		close(jobIDs)

		started := 0
		for code := range codes {
			if code == http.StatusOK {
				started++
			}
		}
		for id := range jobIDs {
			f.waitForJob(t, id)
		}
		if started != 1 {
			t.Fatalf("attempt %d: %d concurrent send-all requests started %d jobs, want 1", attempt, n, started)
		}
		if got := f.sender.recipients(); len(got) != len(ids) {
			t.Fatalf("attempt %d: sent %d emails to %d brokers: %v", attempt, len(got), len(ids), got)
		}
	}
}

// While a job is sending, a single "Send" could email a broker the job is
// about to (or just did) email.
func TestSendOneRefusedWhileJobRuns(t *testing.T) {
	f := newSendFixture(t, config.Options{}, brokersN("a", "b"))
	f.sender.block = make(chan struct{})
	body, code := f.sendAll(t)
	if code != http.StatusOK {
		t.Fatalf("status %d: %v", code, body)
	}

	// Without the check, the request sends too, and blocks on the sender.
	codes := make(chan int, 1)
	go func() { codes <- do(f.h, http.MethodPost, "/api/send/b", sameOrigin).Code }()
	var code1 int
	select {
	case code1 = <-codes:
	case <-time.After(2 * time.Second):
	}
	close(f.sender.block)
	f.waitForJob(t, body["job_id"].(string))
	if code1 != http.StatusConflict {
		t.Fatalf("send-one during a job: status %d, want 409 (0 means it started sending)", code1)
	}
	if got := f.sender.recipients(); len(got) != 2 {
		t.Fatalf("recipients = %v, want each broker once", got)
	}
}

// When a send can't be recorded, the job stops with an error (not
// "completed") that says the email may have gone out unrecorded.
func TestSendAllStopsWithErrorWhenHistoryFails(t *testing.T) {
	f := newSendFixture(t, config.Options{}, brokersN("a", "b", "c"))
	f.sender.setOnSend(func() { _ = f.store.Close() })

	body, code := f.sendAll(t)
	if code != http.StatusOK {
		t.Fatalf("send-all: %d %v", code, body)
	}
	st := f.waitForJob(t, body["job_id"].(string))
	if st["status"] != JobStatusError || st["error_type"] != "history" {
		t.Errorf("status=%v error_type=%v, want error/history", st["status"], st["error_type"])
	}
	if msg, _ := st["error"].(string); !strings.Contains(msg, "may have been sent") {
		t.Errorf("error = %q, want a warning that the email may have been sent", msg)
	}
	if n := len(f.sender.recipients()); n != 1 {
		t.Errorf("sent %d emails after history failed, want 1", n)
	}
}

// The preview shows exactly what "Send all" would send, and sends nothing.
func TestSendAllPreview(t *testing.T) {
	f := newSendFixture(t, config.Options{ExcludedBrokers: []string{"excluded"}, DailyLimit: 2}, brokersN("a", "b", "c", "excluded", "recent"))
	if err := f.store.Add(&history.Record{BrokerID: "recent", BrokerName: "RECENT", Email: "privacy@recent.example",
		Template: "generic", Status: history.StatusSent, SentAt: time.Now().Add(-time.Hour)}); err != nil {
		t.Fatal(err)
	}

	rec := do(f.h, http.MethodGet, "/api/send-all/preview?status=all", nil)
	var p struct {
		Total         int `json:"total"`
		Brokers       []struct{ ID, Name, Email string }
		SkippedRecent int      `json:"skipped_recent"`
		Deferred      int      `json:"deferred"`
		Sending       []string `json:"sending_fields"`
		DryRun        bool     `json:"dry_run"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil || rec.Code != http.StatusOK {
		t.Fatalf("status %d, %v: %s", rec.Code, err, rec.Body.String())
	}
	// recent counts toward the daily limit of 2, so one of a/b/c fits today.
	if p.Total != 1 || len(p.Brokers) != 1 || p.Brokers[0].ID != "a" || p.SkippedRecent != 1 || p.Deferred != 2 || p.DryRun {
		t.Errorf("preview = %+v", p)
	}
	if len(p.Sending) == 0 {
		t.Error("preview doesn't say which profile fields are sent")
	}
	if n := len(f.sender.recipients()); n != 0 || f.s.jobManager.GetActive() != nil {
		t.Fatalf("preview sent %d emails or started a job", n)
	}
}

// "Send all" only sends what the user confirmed in the preview.
func TestSendAllRequiresConfirmedCount(t *testing.T) {
	f := newSendFixture(t, config.Options{}, brokersN("a", "b"))
	for form, want := range map[string]int{
		"":             http.StatusBadRequest, // no preview
		"confirmed=x":  http.StatusBadRequest,
		"confirmed=1":  http.StatusConflict, // the list grew since the preview
		"confirmed=-1": http.StatusBadRequest,
	} {
		if body, code := f.post(t, "/api/send-all", form); code != want {
			t.Errorf("%q: status %d %v, want %d", form, code, body, want)
		}
	}
	if n := len(f.sender.recipients()); n != 0 {
		t.Fatalf("sent %d emails without a matching confirmation", n)
	}
	body, code := f.post(t, "/api/send-all", "confirmed=2")
	if code != http.StatusOK {
		t.Fatalf("confirmed=2: status %d %v", code, body)
	}
	f.waitForJob(t, body["job_id"].(string))
	if n := len(f.sender.recipients()); n != 2 {
		t.Fatalf("sent %d, want 2", n)
	}
}

// options.dry_run must stop web sends too, not just the CLI.
func TestDryRunBlocksWebSends(t *testing.T) {
	f := newSendFixture(t, config.Options{DryRun: true}, brokersN("a"))

	if body, code := f.sendAll(t); code != http.StatusBadRequest || !strings.Contains(fmt.Sprint(body["error"]), "dry_run") {
		t.Errorf("send-all: status %d %v, want 400 mentioning dry_run", code, body)
	}
	rec := do(f.h, http.MethodPost, "/api/send/a", sameOrigin)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "dry_run") {
		t.Errorf("send one: status %d %q, want 400 mentioning dry_run", rec.Code, rec.Body.String())
	}
	if n := len(f.sender.recipients()); n != 0 {
		t.Fatalf("dry run sent %d emails", n)
	}

	rec = do(f.h, http.MethodGet, "/api/send-all/preview", nil)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"dry_run":true`) || !strings.Contains(rec.Body.String(), `"total":1`) {
		t.Errorf("preview in dry run: %d %s", rec.Code, rec.Body.String())
	}
}

// A broker whose address was corrected after a send counts as not yet sent:
// the default "pending" filter and the plan both include it.
func TestSendAllIncludesBrokerWithCorrectedAddress(t *testing.T) {
	moved := broker.Broker{ID: "moved", Name: "Moved", Email: "dsr@moved.example", Region: "us"}
	f := newSendFixture(t, config.Options{}, []broker.Broker{moved})
	if err := f.store.Add(&history.Record{BrokerID: "moved", BrokerName: "Moved", Email: "privacy@moved.example",
		Template: "generic", Status: history.StatusSent, SentAt: time.Now().Add(-time.Hour)}); err != nil {
		t.Fatal(err)
	}
	body, code := f.sendAll(t)
	if code != http.StatusOK {
		t.Fatalf("status %d: %v", code, body)
	}
	f.waitForJob(t, body["job_id"].(string))
	if got := f.sender.recipients(); strings.Join(got, ",") != "dsr@moved.example" {
		t.Fatalf("sent to %v, want the corrected address", got)
	}
}

// Dashboard counts are per broker at its current address, like the broker
// list: a send to a corrected address doesn't count, and a broker sent to
// twice counts once.
func TestDashboardStatsPerBrokerAtCurrentAddress(t *testing.T) {
	bs := []broker.Broker{
		{ID: "moved", Name: "Moved", Email: "dsr@moved.example", Region: "us"},
		{ID: "twice", Name: "Twice", Email: "privacy@twice.example", Region: "us"},
		{ID: "failed", Name: "Failed", Email: "privacy@failed.example", Region: "us"},
		{ID: "never", Name: "Never", Email: "privacy@never.example", Region: "us"},
	}
	f := newSendFixture(t, config.Options{}, bs)
	at := time.Now().Add(-time.Hour)
	for _, r := range []history.Record{
		{BrokerID: "moved", Email: "privacy@moved.example", Status: history.StatusSent},
		{BrokerID: "twice", Email: "privacy@twice.example", Status: history.StatusSent},
		{BrokerID: "twice", Email: "privacy@twice.example", Status: history.StatusSent},
		{BrokerID: "failed", Email: "privacy@failed.example", Status: history.StatusFailed},
	} {
		r.BrokerName, r.Template, r.SentAt = r.BrokerID, "generic", at
		if err := f.store.Add(&r); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := f.s.getStats(), (Stats{TotalBrokers: 4, Sent: 1, Failed: 1, Pending: 2}); got != want {
		t.Errorf("stats = %+v, want %+v", got, want)
	}

	// The activity log shows where each email went, so the old send reads as such.
	page := do(f.h, http.MethodGet, "/", nil).Body.String()
	if !strings.Contains(page, "privacy@moved.example") {
		t.Error("recent activity doesn't show the address an email went to")
	}
}
