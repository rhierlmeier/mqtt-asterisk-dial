package ami

import (
	"bufio"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

const testChallenge = "123456789"

// fakeServer emulates the parts of AMI the client uses.
type fakeServer struct {
	t        *testing.T
	listener net.Listener
	secret   string

	mu sync.Mutex
	// logins counts successful logins.
	logins int
	// originates holds the raw lines of each received Originate action.
	originates [][]string
	conns      []net.Conn

	// originateError makes Originate fail with this message.
	originateError string
	// closeAfterOriginate closes the connection after the Originate response,
	// before the OriginateResponse event.
	closeAfterOriginate bool
}

func newFakeServer(t *testing.T, secret string) *fakeServer {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &fakeServer{t: t, listener: listener, secret: secret}
	go s.serve()
	t.Cleanup(func() {
		listener.Close()
		s.dropConnections()
	})
	return s
}

func (s *fakeServer) addr() string {
	return s.listener.Addr().String()
}

func (s *fakeServer) dropConnections() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.conns {
		c.Close()
	}
	s.conns = nil
}

func (s *fakeServer) serve() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.conns = append(s.conns, conn)
		s.mu.Unlock()
		go s.handle(conn)
	}
}

// readRaw reads one action as raw "Key: Value" lines (keeps duplicate keys).
func readRaw(reader *bufio.Reader) ([]string, error) {
	var lines []string
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			if len(lines) == 0 {
				continue
			}
			return lines, nil
		}
		lines = append(lines, line)
	}
}

func field(lines []string, key string) string {
	for _, l := range lines {
		k, v, _ := strings.Cut(l, ":")
		if strings.EqualFold(strings.TrimSpace(k), key) {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func (s *fakeServer) handle(conn net.Conn) {
	defer conn.Close()
	fmt.Fprint(conn, "Asterisk Call Manager/9.0.0\r\n")
	reader := bufio.NewReader(conn)
	loggedIn := false
	for {
		lines, err := readRaw(reader)
		if err != nil {
			return
		}
		id := field(lines, "ActionID")
		reply := func(kv ...string) {
			var b strings.Builder
			for i := 0; i+1 < len(kv); i += 2 {
				fmt.Fprintf(&b, "%s: %s\r\n", kv[i], kv[i+1])
			}
			fmt.Fprintf(&b, "ActionID: %s\r\n\r\n", id)
			conn.Write([]byte(b.String()))
		}

		switch strings.ToLower(field(lines, "Action")) {
		case "challenge":
			reply("Response", "Success", "Challenge", testChallenge)
		case "login":
			sum := md5.Sum([]byte(testChallenge + s.secret))
			if strings.EqualFold(field(lines, "AuthType"), "md5") && field(lines, "Key") == hex.EncodeToString(sum[:]) {
				loggedIn = true
				s.mu.Lock()
				s.logins++
				s.mu.Unlock()
				reply("Response", "Success", "Message", "Authentication accepted")
			} else {
				reply("Response", "Error", "Message", "Authentication failed")
			}
		case "ping":
			reply("Response", "Success", "Ping", "Pong")
		case "originate":
			if !loggedIn {
				reply("Response", "Error", "Message", "Permission denied")
				continue
			}
			s.mu.Lock()
			s.originates = append(s.originates, lines)
			originateError := s.originateError
			closeAfter := s.closeAfterOriginate
			s.mu.Unlock()
			if originateError != "" {
				reply("Response", "Error", "Message", originateError)
				continue
			}
			reply("Response", "Success", "Message", "Originate successfully queued")
			if closeAfter {
				return
			}
			// Unrelated event in between, must be ignored by the client.
			fmt.Fprint(conn, "Event: Newchannel\r\nChannel: Local/x\r\n\r\n")
			reply("Event", "OriginateResponse", "Response", "Success", "Reason", "4", "Uniqueid", "1234.5")
		default:
			reply("Response", "Error", "Message", "Invalid/unknown command")
		}
	}
}

func (s *fakeServer) loginCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.logins
}

func startClient(t *testing.T, addr string, secret string) *Client {
	c := New(Config{
		Address:         addr,
		Username:        "dialer",
		Secret:          secret,
		PingInterval:    50 * time.Millisecond,
		ResponseTimeout: time.Second,
		MaxBackoff:      100 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		c.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return c
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestLoginAndOriginate(t *testing.T) {
	server := newFakeServer(t, "s3cret")
	client := startClient(t, server.addr(), "s3cret")
	waitFor(t, "login", client.LoggedIn)

	results := make(chan OriginateResult, 1)
	err := client.Originate(context.Background(), OriginateRequest{
		Channel:   "Local/start@heizung_melde_kette",
		Context:   "ende",
		Exten:     "s",
		Priority:  1,
		Timeout:   200 * time.Second,
		Variables: map[string]string{"__stoerNr": "10", "a": "b"},
	}, func(r OriginateResult) { results <- r })
	if err != nil {
		t.Fatal(err)
	}

	select {
	case r := <-results:
		if !r.Success || r.Reason != "4" || r.Uniqueid != "1234.5" {
			t.Errorf("unexpected result %+v", r)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no OriginateResponse")
	}

	server.mu.Lock()
	lines := server.originates[0]
	server.mu.Unlock()
	want := []string{
		"Channel: Local/start@heizung_melde_kette",
		"Context: ende",
		"Exten: s",
		"Priority: 1",
		"Async: true",
		"Timeout: 200000",
		"Variable: __stoerNr=10",
		"Variable: a=b",
	}
	got := strings.Join(lines, "\n")
	for _, w := range want {
		if !strings.Contains(got, w) {
			t.Errorf("Originate is missing %q, got:\n%s", w, got)
		}
	}
}

func TestWrongSecret(t *testing.T) {
	server := newFakeServer(t, "s3cret")
	client := startClient(t, server.addr(), "wrong")

	time.Sleep(300 * time.Millisecond)
	if client.LoggedIn() {
		t.Fatal("logged in with wrong secret")
	}
	if server.loginCount() != 0 {
		t.Fatal("server accepted login")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err := client.Originate(ctx, OriginateRequest{Channel: "x", Context: "y", Exten: "s"}, nil)
	if err == nil {
		t.Fatal("expected error while not logged in")
	}
}

func TestReconnect(t *testing.T) {
	server := newFakeServer(t, "s3cret")
	client := startClient(t, server.addr(), "s3cret")
	waitFor(t, "first login", client.LoggedIn)

	server.dropConnections()
	waitFor(t, "second login", func() bool { return server.loginCount() >= 2 && client.LoggedIn() })

	if err := client.Originate(context.Background(), OriginateRequest{Channel: "x", Context: "y", Exten: "s"}, nil); err != nil {
		t.Fatalf("originate after reconnect: %v", err)
	}
}

func TestOriginateError(t *testing.T) {
	server := newFakeServer(t, "s3cret")
	server.originateError = "Permission denied"
	client := startClient(t, server.addr(), "s3cret")
	waitFor(t, "login", client.LoggedIn)

	called := false
	err := client.Originate(context.Background(), OriginateRequest{Channel: "x", Context: "y", Exten: "s"},
		func(OriginateResult) { called = true })
	if err == nil || !strings.Contains(err.Error(), "Permission denied") {
		t.Fatalf("expected permission error, got %v", err)
	}
	if errors.Is(err, ErrNotConnected) {
		t.Fatal("an error response must not be retryable")
	}
	client.mu.Lock()
	pending := len(client.originates)
	client.mu.Unlock()
	if pending != 0 || called {
		t.Errorf("callback was kept or called (pending=%d, called=%v)", pending, called)
	}
}

func TestConnectionLostBeforeOriginateResponse(t *testing.T) {
	server := newFakeServer(t, "s3cret")
	server.closeAfterOriginate = true
	client := startClient(t, server.addr(), "s3cret")
	waitFor(t, "login", client.LoggedIn)

	results := make(chan OriginateResult, 1)
	err := client.Originate(context.Background(), OriginateRequest{Channel: "x", Context: "y", Exten: "s"},
		func(r OriginateResult) { results <- r })
	if err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-results:
		if r.Success {
			t.Errorf("expected undetermined result, got %+v", r)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("callback not called after connection loss")
	}
}

func TestNotConnected(t *testing.T) {
	c := New(Config{Address: "127.0.0.1:1"})
	_, err := c.action(context.Background(), "1", "Ping", nil)
	if !errors.Is(err, ErrNotConnected) {
		t.Fatalf("expected ErrNotConnected, got %v", err)
	}
}

func TestReadMessage(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("\r\nResponse: Success\r\nActionID: 7\r\nMessage: a: b\r\nno colon line\r\n\r\n"))
	msg, err := readMessage(reader)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Get("response") != "Success" || msg.Get("ACTIONID") != "7" || msg.Get("Message") != "a: b" {
		t.Errorf("unexpected message %v", msg)
	}
}
