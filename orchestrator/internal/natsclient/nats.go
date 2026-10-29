// Package natsclient is a minimal NATS core client implemented with only the
// Go standard library (CONNECT / INFO / PUB / SUB / MSG / PING-PONG).
// The Conn type exposes a small interface so it can be swapped for nats.go.
package natsclient

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Message is a delivered NATS message.
type Message struct {
	Subject string
	Data    []byte
}

// Handler processes a delivered message.
type Handler func(*Message)

// Conn is a minimal NATS connection with automatic reconnect.
type Conn struct {
	url    string
	name   string
	mu     sync.Mutex
	conn   net.Conn
	rw     *bufio.ReadWriter
	nextID int64
	subs   map[string]subEntry
	closed bool
	// reconnecting guards against concurrent reconnect loops.
	reconnecting bool
}

type subEntry struct {
	subject string
	handler Handler
}

// Dial connects to a NATS server (url like "nats://host:4222").
func Dial(rawURL, name string) (*Conn, error) {
	c := &Conn{url: rawURL, name: name, subs: map[string]subEntry{}}
	if err := c.connect(); err != nil {
		return nil, err
	}
	return c, nil
}

// DialRetry tries to connect with backoff until it succeeds or the context
// of attempts is exhausted; used at startup to degrade gracefully.
func DialRetry(rawURL, name string, attempts int, baseDelay time.Duration) (*Conn, error) {
	var err error
	for i := 0; i < attempts; i++ {
		var c *Conn
		c, err = Dial(rawURL, name)
		if err == nil {
			return c, nil
		}
		time.Sleep(baseDelay << i)
	}
	return nil, err
}

func (c *Conn) hostPort() string {
	u, err := url.Parse(c.url)
	if err != nil || u.Host == "" {
		return strings.TrimPrefix(c.url, "nats://")
	}
	return u.Host
}

// connect establishes the TCP connection and handshake, then replays
// subscriptions. Callers must hold c.mu.
func (c *Conn) connect() error {
	conn, err := net.DialTimeout("tcp", c.hostPort(), 5*time.Second)
	if err != nil {
		return err
	}
	c.conn = conn
	c.rw = bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	// Expect INFO first.
	line, err := c.rw.ReadString('\n')
	if err != nil {
		conn.Close()
		return err
	}
	if !strings.HasPrefix(line, "INFO ") {
		conn.Close()
		return fmt.Errorf("nats: expected INFO, got %q", strings.TrimSpace(line))
	}
	connect := fmt.Sprintf(`CONNECT {"verbose":false,"pedantic":false,"name":%q,"lang":"go","version":"1.0.0","protocol":1}`+"\r\nPING\r\n", c.name)
	if _, err := c.rw.WriteString(connect); err != nil {
		conn.Close()
		return err
	}
	if err := c.rw.Flush(); err != nil {
		conn.Close()
		return err
	}
	// Read until +OK/PONG (verbose=false means we get PONG).
	deadline := time.Now().Add(5 * time.Second)
	for {
		conn.SetReadDeadline(deadline)
		line, err = c.rw.ReadString('\n')
		if err != nil {
			conn.Close()
			return err
		}
		line = strings.TrimSpace(line)
		if line == "PONG" {
			break
		}
		if strings.HasPrefix(line, "-ERR") {
			conn.Close()
			return errors.New("nats: " + line)
		}
	}
	conn.SetReadDeadline(time.Time{})
	// replay subscriptions on reconnect
	for sid, s := range c.subs {
		if _, err := c.rw.WriteString(fmt.Sprintf("SUB %s %s\r\n", s.subject, sid)); err != nil {
			return err
		}
	}
	if err := c.rw.Flush(); err != nil {
		return err
	}
	// The read loop captures THIS connection's reader/writer so that a
	// reconnect swapping c.conn/c.rw never leads to two goroutines splitting
	// frames of one stream.
	go c.readLoop(conn, c.rw)
	return nil
}

func (c *Conn) readLoop(conn net.Conn, rw *bufio.ReadWriter) {
	defer func() {
		c.mu.Lock()
		if c.conn == conn {
			c.conn, c.rw = nil, nil
		}
		dead := !c.closed
		c.mu.Unlock()
		conn.Close()
		if dead {
			c.reconnectLoop()
		}
	}()
	for {
		line, err := rw.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		switch {
		case line == "PING":
			c.mu.Lock()
			if c.rw != nil && c.conn == conn {
				conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
				c.rw.WriteString("PONG\r\n")
				c.rw.Flush()
				conn.SetWriteDeadline(time.Time{})
			}
			c.mu.Unlock()
		case strings.HasPrefix(line, "MSG "):
			// MSG <subject> <sid> [reply-to] <size>
			parts := strings.SplitN(line, " ", 5)
			if len(parts) < 4 {
				continue
			}
			subject := parts[1]
			sid := parts[2]
			sizeToken := parts[3]
			if len(parts) == 5 { // reply-to present; size is last token
				sizeToken = parts[4]
			}
			size, err := strconv.Atoi(sizeToken)
			if err != nil {
				continue
			}
			buf := make([]byte, size+2)
			if _, err := readFull(rw.Reader, buf); err != nil {
				return
			}
			c.mu.Lock()
			s, ok := c.subs[sid]
			c.mu.Unlock()
			if ok && s.handler != nil {
				data := make([]byte, size)
				copy(data, buf[:size])
				go s.handler(&Message{Subject: subject, Data: data})
			}
		case strings.HasPrefix(line, "-ERR"):
			// log-free: server error, keep reading
		}
	}
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// Publish publishes payload to subject.
func (c *Conn) Publish(subject string, payload []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errors.New("nats: connection closed")
	}
	if c.conn == nil {
		if err := c.connect(); err != nil {
			return err
		}
	}
	// Bounded writes: a half-open TCP connection must not wedge the client
	// (and with it every goroutine blocked on c.mu) for minutes.
	c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err := c.rw.WriteString(fmt.Sprintf("PUB %s %d\r\n", subject, len(payload)))
	if err == nil {
		_, err = c.rw.Write(payload)
	}
	if err == nil {
		_, err = c.rw.WriteString("\r\n")
	}
	if err == nil {
		err = c.rw.Flush()
	}
	c.conn.SetWriteDeadline(time.Time{})
	if err != nil {
		// drop broken conn; the read loop's defer will reconnect
		c.conn.Close()
		c.conn, c.rw = nil, nil
	}
	return err
}

// reconnectLoop reconnects with backoff until success or Close. Triggered by
// the read loop when the connection dies, so subscriptions are restored even
// when no publish happens to fail.
func (c *Conn) reconnectLoop() {
	c.mu.Lock()
	if c.reconnecting || c.closed {
		c.mu.Unlock()
		return
	}
	c.reconnecting = true
	c.mu.Unlock()
	go func() {
		defer func() {
			c.mu.Lock()
			c.reconnecting = false
			c.mu.Unlock()
		}()
		delay := 500 * time.Millisecond
		for {
			c.mu.Lock()
			closed := c.closed
			c.mu.Unlock()
			if closed {
				return
			}
			c.mu.Lock()
			if c.conn != nil {
				// another path already reconnected
				c.mu.Unlock()
				return
			}
			err := c.connect()
			c.mu.Unlock()
			if err == nil {
				return
			}
			time.Sleep(delay)
			if delay < 10*time.Second {
				delay *= 2
			}
		}
	}()
}

// Subscribe registers handler for a subject (wildcards * and > allowed).
func (c *Conn) Subscribe(subject string, handler Handler) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextID++
	sid := strconv.FormatInt(c.nextID, 10)
	c.subs[sid] = subEntry{subject: subject, handler: handler}
	if c.conn == nil {
		return nil // will be replayed on connect
	}
	if _, err := c.rw.WriteString(fmt.Sprintf("SUB %s %s\r\n", subject, sid)); err != nil {
		return err
	}
	return c.rw.Flush()
}

// Close shuts down the connection.
func (c *Conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
