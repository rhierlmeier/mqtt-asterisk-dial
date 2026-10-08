// Package ami implements a minimal client for the Asterisk Manager Interface
// (AMI). It supports the challenge/response login (MD5), Originate and Ping,
// and keeps the connection alive by reconnecting with backoff.
package ami

import (
	"bufio"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrNotConnected is returned when an action could not be sent because no
	// AMI session exists. Asterisk has not seen the action, so it is safe to retry.
	ErrNotConnected = errors.New("not connected to AMI")
	// ErrConnectionLost is returned when the connection ended after the action
	// was sent. Asterisk may have executed the action.
	ErrConnectionLost = errors.New("AMI connection lost while waiting for the response")
)

// Message is a single AMI message (response or event). Keys are stored in
// lower case, because AMI keys are case-insensitive.
type Message map[string]string

// Get returns the value of key (case-insensitive).
func (m Message) Get(key string) string {
	return m[strings.ToLower(key)]
}

// Config configures the AMI client.
type Config struct {
	// Address of the AMI port, e.g. "asterisk:5038".
	Address  string
	Username string
	Secret   string

	// PingInterval is the interval of the keepalive pings (default 20s).
	PingInterval time.Duration
	// ResponseTimeout is the time to wait for the response to an action (default 10s).
	ResponseTimeout time.Duration
	// MaxBackoff limits the wait time between reconnect attempts (default 30s).
	MaxBackoff time.Duration
}

// OriginateRequest describes an Originate action.
type OriginateRequest struct {
	Channel  string
	Context  string
	Exten    string
	Priority int
	// Timeout is the time Asterisk waits for the channel to be answered.
	Timeout  time.Duration
	CallerID string
	// Variables are set on the channel (Variable: name=value).
	Variables map[string]string
}

// OriginateResult is the outcome reported by the OriginateResponse event.
type OriginateResult struct {
	Success bool
	// Reason is the Asterisk reason code (e.g. "4" = answered), or a
	// description if the result could not be determined.
	Reason   string
	Uniqueid string
}

// Client is an AMI client. Create it with New and start it with Run.
type Client struct {
	cfg Config

	mu         sync.Mutex
	conn       net.Conn
	readyCh    chan struct{}
	pending    map[string]chan Message
	originates map[string]func(OriginateResult)

	loggedIn atomic.Bool
	nextID   atomic.Uint64
}

// New creates a client. It does not connect until Run is called.
func New(cfg Config) *Client {
	if cfg.PingInterval == 0 {
		cfg.PingInterval = 20 * time.Second
	}
	if cfg.ResponseTimeout == 0 {
		cfg.ResponseTimeout = 10 * time.Second
	}
	if cfg.MaxBackoff == 0 {
		cfg.MaxBackoff = 30 * time.Second
	}
	return &Client{
		cfg:        cfg,
		readyCh:    make(chan struct{}),
		pending:    make(map[string]chan Message),
		originates: make(map[string]func(OriginateResult)),
	}
}

// LoggedIn reports whether a logged-in AMI session exists.
func (c *Client) LoggedIn() bool {
	return c.loggedIn.Load()
}

// Run connects to AMI and keeps the session alive until ctx is cancelled.
func (c *Client) Run(ctx context.Context) {
	backoff := time.Second
	for {
		start := time.Now()
		err := c.session(ctx)
		if ctx.Err() != nil {
			return
		}
		// A session that was up for a while resets the backoff.
		if time.Since(start) > c.cfg.MaxBackoff {
			backoff = time.Second
		}
		log.Printf("AMI session with %s ended: %v; reconnecting in %s", c.cfg.Address, err, backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, c.cfg.MaxBackoff)
	}
}

// session runs one AMI connection: connect, log in, ping until an error occurs.
func (c *Client) session(ctx context.Context) error {
	dialer := net.Dialer{Timeout: c.cfg.ResponseTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", c.cfg.Address)
	if err != nil {
		return err
	}

	reader := bufio.NewReader(conn)
	conn.SetReadDeadline(time.Now().Add(c.cfg.ResponseTimeout))
	banner, err := reader.ReadString('\n')
	if err != nil {
		conn.Close()
		return fmt.Errorf("reading banner: %w", err)
	}
	if !strings.HasPrefix(banner, "Asterisk Call Manager") {
		conn.Close()
		return fmt.Errorf("unexpected banner %q", strings.TrimSpace(banner))
	}
	conn.SetReadDeadline(time.Time{})

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()

	readErr := make(chan error, 1)
	go func() { readErr <- c.readLoop(reader) }()

	// Closing the connection ends the read loop; wait for it before cleaning up.
	defer func() {
		conn.Close()
		<-readErr
		c.endSession()
	}()

	if err := c.login(ctx); err != nil {
		return err
	}
	log.Printf("Logged in to AMI at %s as %s", c.cfg.Address, c.cfg.Username)

	ticker := time.NewTicker(c.cfg.PingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-readErr:
			// Put the error back for the deferred cleanup.
			readErr <- err
			return err
		case <-ticker.C:
			if _, err := c.action(ctx, c.newActionID(), "Ping", nil); err != nil {
				return fmt.Errorf("ping: %w", err)
			}
		}
	}
}

func (c *Client) login(ctx context.Context) error {
	resp, err := c.action(ctx, c.newActionID(), "Challenge", [][2]string{{"AuthType", "MD5"}})
	if err != nil {
		return fmt.Errorf("challenge: %w", err)
	}
	challenge := resp.Get("Challenge")
	if challenge == "" {
		return errors.New("challenge: empty challenge")
	}
	sum := md5.Sum([]byte(challenge + c.cfg.Secret))

	_, err = c.action(ctx, c.newActionID(), "Login", [][2]string{
		{"Username", c.cfg.Username},
		{"AuthType", "MD5"},
		{"Key", hex.EncodeToString(sum[:])},
		// OriginateResponse belongs to the "call" event class.
		{"Events", "call"},
	})
	if err != nil {
		return fmt.Errorf("login: %w", err)
	}

	c.mu.Lock()
	c.loggedIn.Store(true)
	close(c.readyCh)
	c.mu.Unlock()
	return nil
}

// endSession resets the state after a connection ended. Pending actions fail
// and Originate callbacks that are still waiting get an undetermined result.
func (c *Client) endSession() {
	c.mu.Lock()
	c.conn = nil
	if c.loggedIn.Swap(false) {
		c.readyCh = make(chan struct{})
	}
	for id, ch := range c.pending {
		close(ch)
		delete(c.pending, id)
	}
	callbacks := c.originates
	c.originates = make(map[string]func(OriginateResult))
	c.mu.Unlock()

	for _, cb := range callbacks {
		cb(OriginateResult{Success: false, Reason: "AMI connection lost before OriginateResponse"})
	}
}

func (c *Client) readLoop(reader *bufio.Reader) error {
	for {
		msg, err := readMessage(reader)
		if err != nil {
			return err
		}
		c.dispatch(msg)
	}
}

func (c *Client) dispatch(msg Message) {
	id := msg.Get("ActionID")

	if msg.Get("Event") == "OriginateResponse" {
		c.mu.Lock()
		cb := c.originates[id]
		delete(c.originates, id)
		c.mu.Unlock()
		if cb != nil {
			cb(OriginateResult{
				Success:  strings.EqualFold(msg.Get("Response"), "Success"),
				Reason:   msg.Get("Reason"),
				Uniqueid: msg.Get("Uniqueid"),
			})
		}
		return
	}

	if msg.Get("Response") != "" {
		c.mu.Lock()
		ch := c.pending[id]
		delete(c.pending, id)
		c.mu.Unlock()
		if ch != nil {
			ch <- msg
		}
	}
	// Other events are not needed.
}

// readMessage reads one message: "Key: Value" lines terminated by an empty line.
func readMessage(reader *bufio.Reader) (Message, error) {
	msg := Message{}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			if len(msg) == 0 {
				// Ignore stray empty lines between messages.
				continue
			}
			return msg, nil
		}
		key, value, found := strings.Cut(line, ":")
		if !found {
			continue
		}
		msg[strings.ToLower(strings.TrimSpace(key))] = strings.TrimSpace(value)
	}
}

func (c *Client) newActionID() string {
	return strconv.FormatUint(c.nextID.Add(1), 10)
}

// action sends an action and waits for its response. A response other than
// "Success" (or "Goodbye" for Logoff) is returned as an error.
func (c *Client) action(ctx context.Context, id string, name string, fields [][2]string) (Message, error) {
	var b strings.Builder
	fmt.Fprintf(&b, "Action: %s\r\nActionID: %s\r\n", name, id)
	for _, f := range fields {
		fmt.Fprintf(&b, "%s: %s\r\n", f[0], f[1])
	}
	b.WriteString("\r\n")

	ch := make(chan Message, 1)
	c.mu.Lock()
	if c.conn == nil {
		c.mu.Unlock()
		return nil, ErrNotConnected
	}
	c.pending[id] = ch
	c.conn.SetWriteDeadline(time.Now().Add(c.cfg.ResponseTimeout))
	_, err := c.conn.Write([]byte(b.String()))
	if err != nil {
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, fmt.Errorf("%w: %v", ErrNotConnected, err)
	}
	c.mu.Unlock()

	timer := time.NewTimer(c.cfg.ResponseTimeout)
	defer timer.Stop()
	select {
	case msg, ok := <-ch:
		if !ok {
			return nil, ErrConnectionLost
		}
		if !strings.EqualFold(msg.Get("Response"), "Success") {
			return msg, fmt.Errorf("%s failed: %s %s", name, msg.Get("Response"), msg.Get("Message"))
		}
		return msg, nil
	case <-timer.C:
	case <-ctx.Done():
	}
	c.mu.Lock()
	delete(c.pending, id)
	c.mu.Unlock()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return nil, fmt.Errorf("%s: no response within %s", name, c.cfg.ResponseTimeout)
}

// Originate starts a call. It waits until an AMI session is logged in or ctx
// is done. It returns once Asterisk accepted the request; the outcome of the
// call is reported later via onResult (which may be nil).
func (c *Client) Originate(ctx context.Context, req OriginateRequest, onResult func(OriginateResult)) error {
	if err := c.waitReady(ctx); err != nil {
		return err
	}

	fields := [][2]string{
		{"Channel", req.Channel},
		{"Context", req.Context},
		{"Exten", req.Exten},
		{"Priority", strconv.Itoa(max(req.Priority, 1))},
		{"Async", "true"},
	}
	if req.Timeout > 0 {
		fields = append(fields, [2]string{"Timeout", strconv.FormatInt(req.Timeout.Milliseconds(), 10)})
	}
	if req.CallerID != "" {
		fields = append(fields, [2]string{"CallerID", req.CallerID})
	}
	names := make([]string, 0, len(req.Variables))
	for name := range req.Variables {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		fields = append(fields, [2]string{"Variable", name + "=" + req.Variables[name]})
	}

	id := c.newActionID()
	if onResult != nil {
		// Register before sending: the event may arrive right after the response.
		c.mu.Lock()
		c.originates[id] = onResult
		c.mu.Unlock()
	}
	_, err := c.action(ctx, id, "Originate", fields)
	if err != nil {
		c.mu.Lock()
		delete(c.originates, id)
		c.mu.Unlock()
		return err
	}
	return nil
}

func (c *Client) waitReady(ctx context.Context) error {
	c.mu.Lock()
	ready := c.readyCh
	c.mu.Unlock()
	select {
	case <-ready:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("waiting for AMI login: %w", ctx.Err())
	}
}
