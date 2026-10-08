package dial

import (
	"context"
	"errors"
	"fmt"
	"mqtt-asterisk-dial/internal/ami"
	"mqtt-asterisk-dial/internal/config"
	"sync"
	"testing"
	"time"
)

type fakeOriginator struct {
	mu       sync.Mutex
	requests []ami.OriginateRequest
	// errs are returned by the calls in order; afterwards nil is returned.
	errs []error
}

func (f *fakeOriginator) Originate(ctx context.Context, req ami.OriginateRequest, onResult func(ami.OriginateResult)) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, req)
	if len(f.errs) > 0 {
		err := f.errs[0]
		f.errs = f.errs[1:]
		return err
	}
	return nil
}

func (f *fakeOriginator) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.requests)
}

func testTemplate() config.CallTemplate {
	return config.CallTemplate{
		Name:  "test",
		Topic: "t/active",
		Value: "true",
		Originate: &config.OriginateTemplate{
			Channel:  "Local/start@heizung_melde_kette",
			Context:  "ende",
			Exten:    "s",
			Priority: 1,
			Timeout:  200,
			Setvar:   map[string]string{"__stoerNr": "{{ .stoerNr }}"},
		},
	}
}

func TestBuildOriginateRequest(t *testing.T) {
	req, err := buildOriginateRequest(testTemplate().Originate, map[string]interface{}{"stoerNr": "10"})
	if err != nil {
		t.Fatal(err)
	}
	if req.Channel != "Local/start@heizung_melde_kette" || req.Context != "ende" || req.Exten != "s" || req.Priority != 1 {
		t.Errorf("unexpected request %+v", req)
	}
	if req.Timeout != 200*time.Second {
		t.Errorf("timeout = %s", req.Timeout)
	}
	if req.Variables["__stoerNr"] != "10" {
		t.Errorf("variables = %v", req.Variables)
	}
}

func TestBuildOriginateRequestInvalidTemplate(t *testing.T) {
	ot := testTemplate().Originate
	ot.Setvar = map[string]string{"x": "{{ .broken"}
	if _, err := buildOriginateRequest(ot, nil); err == nil {
		t.Fatal("expected template error")
	}
}

func withFastRetry(t *testing.T) {
	oldDelay, oldTimeout := originateRetryDelay, originateWaitTimeout
	originateRetryDelay, originateWaitTimeout = 10*time.Millisecond, time.Second
	t.Cleanup(func() { originateRetryDelay, originateWaitTimeout = oldDelay, oldTimeout })
}

func TestOriginateRetriesWhenNotConnected(t *testing.T) {
	withFastRetry(t)
	fake := &fakeOriginator{errs: []error{
		ami.ErrNotConnected,
		fmt.Errorf("%w: broken pipe", ami.ErrNotConnected),
	}}
	d := &Dialer{callTemplate: testTemplate(), originator: fake}
	d.originate(map[string]interface{}{"stoerNr": "10"})
	if fake.count() != 3 {
		t.Errorf("attempts = %d, want 3", fake.count())
	}
}

func TestOriginateDoesNotRetryOtherErrors(t *testing.T) {
	withFastRetry(t)
	for _, err := range []error{ami.ErrConnectionLost, errors.New("Originate failed: Error Permission denied")} {
		fake := &fakeOriginator{errs: []error{err}}
		d := &Dialer{callTemplate: testTemplate(), originator: fake}
		d.originate(nil)
		if fake.count() != 1 {
			t.Errorf("%v: attempts = %d, want 1 (a retry could place the call twice)", err, fake.count())
		}
	}
}

func TestOnValueChangedUsesOriginatorWithVariableSnapshot(t *testing.T) {
	fake := &fakeOriginator{}
	d := &Dialer{callTemplate: testTemplate(), originator: fake, variableValues: map[string]interface{}{}}
	d.onVariableChanged("stoerNr", "7")
	d.onValueChanged("false")
	d.onValueChanged("true")
	// Changing the variable afterwards must not affect the triggered call.
	d.onVariableChanged("stoerNr", "8")

	deadline := time.Now().Add(2 * time.Second)
	for fake.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if fake.count() != 1 {
		t.Fatalf("originate calls = %d, want 1", fake.count())
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if got := fake.requests[0].Variables["__stoerNr"]; got != "7" {
		t.Errorf("__stoerNr = %q, want 7", got)
	}
}
