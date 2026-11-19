package sendplan

import (
	"reflect"
	"testing"
	"time"

	"github.com/eraser-privacy/eraser/internal/broker"
	"github.com/eraser-privacy/eraser/internal/history"
)

func brokers(ids ...string) []broker.Broker {
	out := make([]broker.Broker, len(ids))
	for i, id := range ids {
		out[i] = broker.Broker{ID: id}
	}
	return out
}

func ids(bs []broker.Broker) []string {
	out := []string{}
	for _, b := range bs {
		out = append(out, b.ID)
	}
	return out
}

var now = time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)

// sentAt keys last-send times by broker ID, for test brokers with no address.
func sentAt(byID map[string]time.Time) map[history.Recipient]time.Time {
	out := make(map[history.Recipient]time.Time, len(byID))
	for id, at := range byID {
		out[history.RecipientOf(id, "")] = at
	}
	return out
}

func TestBuildSkipsRecentlySent(t *testing.T) {
	last := sentAt(map[string]time.Time{
		"recent": now.Add(-10 * 24 * time.Hour),
		"old":    now.Add(-40 * 24 * time.Hour),
	})
	p := Build(brokers("recent", "old", "never"), last, 0, Options{Now: now, ResendAfterDays: 30, DailyLimit: 250})
	if got, want := ids(p.Send), []string{"old", "never"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Send = %v, want %v", got, want)
	}
	if got, want := ids(p.SkippedRecent), []string{"recent"}; !reflect.DeepEqual(got, want) {
		t.Errorf("SkippedRecent = %v, want %v", got, want)
	}
}

func TestBuildForceResendIgnoresHistory(t *testing.T) {
	last := sentAt(map[string]time.Time{"recent": now.Add(-time.Hour)})
	p := Build(brokers("recent"), last, 0, Options{Now: now, ResendAfterDays: 30, DailyLimit: 250, ForceResend: true})
	if len(p.Send) != 1 || len(p.SkippedRecent) != 0 {
		t.Fatalf("plan = %+v", p)
	}
}

func TestBuildDailyCapCountsEarlierSends(t *testing.T) {
	p := Build(brokers("a", "b", "c", "d"), nil, 248, Options{Now: now, ResendAfterDays: 30, DailyLimit: 250})
	if got, want := ids(p.Send), []string{"a", "b"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Send = %v, want %v", got, want)
	}
	if p.Deferred != 2 || p.RemainingToday != 2 {
		t.Errorf("Deferred = %d RemainingToday = %d, want 2, 2", p.Deferred, p.RemainingToday)
	}
}

func TestBuildDailyCapAlreadyReached(t *testing.T) {
	p := Build(brokers("a"), nil, 300, Options{Now: now, ResendAfterDays: 30, DailyLimit: 250})
	if len(p.Send) != 0 || p.Deferred != 1 || p.RemainingToday != 0 {
		t.Fatalf("plan = %+v", p)
	}
}

func TestBuildLimit(t *testing.T) {
	p := Build(brokers("a", "b", "c"), nil, 0, Options{Now: now, ResendAfterDays: 30, DailyLimit: 250, Limit: 1})
	if got, want := ids(p.Send), []string{"a"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Send = %v, want %v", got, want)
	}
	if p.Deferred != 2 {
		t.Errorf("Deferred = %d, want 2", p.Deferred)
	}
}

func TestWindowStart(t *testing.T) {
	if got, want := WindowStart(now), now.Add(-24*time.Hour); !got.Equal(want) {
		t.Fatalf("WindowStart = %v, want %v", got, want)
	}
}

// Correcting a broker's address makes it eligible again within the window.
func TestBuildResendsAfterAddressChange(t *testing.T) {
	moved := broker.Broker{ID: "moved", Email: "DSR@moved.example"}
	same := broker.Broker{ID: "same", Email: "privacy@same.example"}
	last := map[history.Recipient]time.Time{
		history.RecipientOf("moved", "privacy@moved.example"): now.Add(-time.Hour),
		history.RecipientOf("same", "Privacy@Same.example"):   now.Add(-time.Hour),
	}
	p := Build([]broker.Broker{moved, same}, last, 0, Options{Now: now, ResendAfterDays: 30, DailyLimit: 250})
	if got := ids(p.Send); !reflect.DeepEqual(got, []string{"moved"}) {
		t.Errorf("Send = %v, want [moved]", got)
	}
	if got := ids(p.SkippedRecent); !reflect.DeepEqual(got, []string{"same"}) {
		t.Errorf("SkippedRecent = %v, want [same]", got)
	}
}
