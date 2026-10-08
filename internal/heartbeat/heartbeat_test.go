package heartbeat

import (
	"strings"
	"testing"
	"time"
)

func newTestHeartbeat(clock *time.Time) *Heartbeat {
	h := &Heartbeat{topic: "hb", maxAge: 10 * time.Minute, now: func() time.Time { return *clock }}
	h.last.Store(clock.UnixNano())
	return h
}

func TestCheck(t *testing.T) {
	clock := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	h := newTestHeartbeat(&clock)

	if got := h.Check(); got != "heartbeat not subscribed" {
		t.Errorf("before subscribe: %q", got)
	}

	h.subscribed.Store(true)
	if got := h.Check(); got != "" {
		t.Errorf("fresh start must be healthy, got %q", got)
	}

	// Grace period from the start: still fine just before maxAge.
	clock = clock.Add(10 * time.Minute)
	if got := h.Check(); got != "" {
		t.Errorf("at max age: %q", got)
	}

	clock = clock.Add(time.Second)
	if got := h.Check(); !strings.Contains(got, "no heartbeat on hb for 10m1s") {
		t.Errorf("stale: %q", got)
	}

	h.received()
	if got := h.Check(); got != "" {
		t.Errorf("after heartbeat: %q", got)
	}

	h.MarkUnsubscribed()
	if got := h.Check(); got != "heartbeat not subscribed" {
		t.Errorf("after connection loss: %q", got)
	}
}
