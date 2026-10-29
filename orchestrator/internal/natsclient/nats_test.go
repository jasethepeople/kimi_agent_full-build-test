package natsclient

import (
	"bufio"
	"fmt"
	"net"
	"testing"
	"time"
)

// startFakeServer runs a minimal in-process NATS server over a real TCP
// listener on 127.0.0.1:0. It speaks INFO/CONNECT/PING, records SUBs, and
// lets the test inject MSG frames.
func startFakeServer(t *testing.T) (addr string, inject func(subject, sid string, payload []byte, replyTo bool), subs chan string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	subs = make(chan string, 8)
	msgs := make(chan []byte, 8)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		w := bufio.NewWriter(conn)
		w.WriteString(`INFO {"server_id":"fake","version":"2.10.0","port":4222}` + "\r\n")
		w.Flush()
		r := bufio.NewReader(conn)
		// writer goroutine for injected MSG frames
		go func() {
			for frame := range msgs {
				w.Write(frame)
				w.Flush()
			}
		}()
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				return
			}
			switch {
			case len(line) >= 4 && line[:4] == "SUB ":
				var subject, sid string
				fmt.Sscanf(line, "SUB %s %s", &subject, &sid)
				subs <- subject
			case len(line) >= 5 && line[:5] == "PING\n" || len(line) >= 5 && line[:5] == "PING\r":
				w.WriteString("PONG\r\n")
				w.Flush()
			}
		}
	}()
	inject = func(subject, sid string, payload []byte, replyTo bool) {
		var frame string
		if replyTo {
			frame = fmt.Sprintf("MSG %s %s _INBOX.x %d\r\n", subject, sid, len(payload))
		} else {
			frame = fmt.Sprintf("MSG %s %s %d\r\n", subject, sid, len(payload))
		}
		msgs <- append([]byte(frame), append(payload, '\r', '\n')...)
	}
	return ln.Addr().String(), inject, subs
}

func TestMsgDispatchWithAndWithoutReplyTo(t *testing.T) {
	addr, inject, subs := startFakeServer(t)
	conn, err := Dial("nats://"+addr, "test")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	got := make(chan *Message, 2)
	if err := conn.Subscribe("grid.strategy.update.*", func(m *Message) { got <- m }); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if s := <-subs; s != "grid.strategy.update.*" {
		t.Fatalf("server saw SUB %q", s)
	}

	inject("grid.strategy.update.job-1", "1", []byte(`{"strategyId":"gen-1"}`), false)
	inject("grid.strategy.update.job-2", "1", []byte(`{"strategyId":"gen-2"}`), true)

	seen := map[string]string{} // strategyId -> subject
	for i := 0; i < 2; i++ {
		select {
		case m := <-got:
			for _, want := range []string{"gen-1", "gen-2"} {
				if contains(string(m.Data), want) {
					seen[want] = m.Subject
				}
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for message %d (got %v)", i, seen)
		}
	}
	if seen["gen-1"] != "grid.strategy.update.job-1" || seen["gen-2"] != "grid.strategy.update.job-2" {
		t.Fatalf("unexpected dispatch: %v", seen)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestPublishFrameFormat(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	frames := make(chan string, 4)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		w := bufio.NewWriter(conn)
		w.WriteString(`INFO {"server_id":"fake"}` + "\r\n")
		w.Flush()
		r := bufio.NewReader(conn)
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				return
			}
			frames <- line
			if len(line) >= 4 && line[:4] == "PUB " {
				// read payload line
				pl, _ := r.ReadString('\n')
				frames <- pl
			}
			if line == "PING\r\n" || line == "PING\n" {
				w.WriteString("PONG\r\n")
				w.Flush()
			}
		}
	}()
	conn, err := Dial("nats://"+ln.Addr().String(), "pub-test")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if err := conn.Publish("grid.nodes.hello", []byte("{}")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	deadline := time.After(2 * time.Second)
	for {
		select {
		case f := <-frames:
			if len(f) >= 4 && f[:4] == "PUB " {
				if f != "PUB grid.nodes.hello 2\r\n" {
					t.Fatalf("bad PUB frame %q", f)
				}
				return
			}
		case <-deadline:
			t.Fatal("no PUB frame seen")
		}
	}
}

// TestReconnectRestoresSubscriptions kills the server side of the connection
// and verifies the client reconnects and replays its subscriptions.
func TestReconnectRestoresSubscriptions(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	subs := make(chan string, 8)
	conns := make(chan net.Conn, 4)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			conns <- conn
			go func(c net.Conn) {
				w := bufio.NewWriter(c)
				w.WriteString(`INFO {"server_id":"fake"}` + "\r\n")
				w.Flush()
				r := bufio.NewReader(c)
				for {
					line, err := r.ReadString('\n')
					if err != nil {
						return
					}
					if len(line) >= 4 && line[:4] == "SUB " {
						var subject, sid string
						fmt.Sscanf(line, "SUB %s %s", &subject, &sid)
						subs <- subject
					}
					if line == "PING\r\n" || line == "PING\n" {
						w.WriteString("PONG\r\n")
						w.Flush()
					}
				}
			}(conn)
		}
	}()

	conn, err := Dial("nats://"+ln.Addr().String(), "reconnect-test")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if err := conn.Subscribe("grid.strategy.update.*", func(*Message) {}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	serverSide := <-conns
	if s := <-subs; s != "grid.strategy.update.*" {
		t.Fatalf("initial SUB %q", s)
	}

	// Kill the server side; the read loop should notice and reconnect.
	serverSide.Close()

	select {
	case s := <-subs:
		if s != "grid.strategy.update.*" {
			t.Fatalf("replayed SUB %q", s)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("subscription not replayed after reconnect")
	}
}
