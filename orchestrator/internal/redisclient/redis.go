// Package redisclient is a minimal Redis RESP2 client implemented with only
// the Go standard library. It exposes the Client interface so the
// implementation can be swapped (e.g. for go-redis) without touching callers.
package redisclient

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"
)

// ErrNil is returned when a key/field does not exist (Redis nil reply).
var ErrNil = errors.New("redis: nil")

// Client is the contract used by the orchestrator internals.
type Client interface {
	Ping() error
	Get(key string) (string, error)
	Set(key, value string) error
	Del(keys ...string) (int64, error)
	HSet(key string, fieldValues ...string) (int64, error)
	HGet(key, field string) (string, error)
	HGetAll(key string) (map[string]string, error)
	HIncrBy(key, field string, delta int64) (int64, error)
	LPush(key string, values ...string) (int64, error)
	LRange(key string, start, stop int64) ([]string, error)
	RPush(key string, values ...string) (int64, error)
	// BRPop blocks up to timeoutSec; returns ErrNil on timeout.
	BRPop(timeoutSec int, keys ...string) (string, string, error)
	XAdd(key string, fields map[string]string) (string, error)
	XRange(key string, start, stop string) ([]StreamEntry, error)
	SAdd(key string, members ...string) (int64, error)
	SIsMember(key, member string) (bool, error)
	SMembers(key string) ([]string, error)
	HKeys(key string) ([]string, error)
	Close() error
}

// StreamEntry is one entry of a Redis stream.
type StreamEntry struct {
	ID     string
	Fields map[string]string
}

// RedisClient is a single-connection, mutex-guarded RESP client that
// reconnects with backoff on failure.
type RedisClient struct {
	addr string
	mu   sync.Mutex
	conn net.Conn
	rw   *bufio.ReadWriter
}

// New dials (lazily) a Redis server at addr ("host:port").
func New(addr string) *RedisClient {
	return &RedisClient{addr: addr}
}

func (c *RedisClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		c.rw = nil
		return err
	}
	return nil
}

func (c *RedisClient) ensureConn() error {
	if c.conn != nil {
		return nil
	}
	conn, err := net.DialTimeout("tcp", c.addr, 5*time.Second)
	if err != nil {
		return err
	}
	c.conn = conn
	c.rw = bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	return nil
}

// do sends one command and reads the reply, reconnecting once on I/O error.
func (c *RedisClient) do(args ...string) (interface{}, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.ensureConn(); err != nil {
		return nil, err
	}
	reply, err := c.roundTrip(args...)
	if err != nil {
		// reconnect once
		if c.conn != nil {
			c.conn.Close()
		}
		c.conn, c.rw = nil, nil
		if derr := c.ensureConn(); derr != nil {
			return nil, err
		}
		reply, err = c.roundTrip(args...)
	}
	return reply, err
}

func (c *RedisClient) roundTrip(args ...string) (interface{}, error) {
	c.conn.SetDeadline(time.Now().Add(65 * time.Second))
	w := c.rw.Writer
	fmt.Fprintf(w, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(w, "$%d\r\n%s\r\n", len(a), a)
	}
	if err := w.Flush(); err != nil {
		return nil, err
	}
	return readReply(c.rw.Reader)
}

func readReply(r *bufio.Reader) (interface{}, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 3 {
		return nil, fmt.Errorf("redis: malformed reply %q", line)
	}
	body := line[1 : len(line)-2]
	switch line[0] {
	case '+':
		return body, nil
	case '-':
		return nil, errors.New("redis: " + body)
	case ':':
		return strconv.ParseInt(body, 10, 64)
	case '$':
		n, err := strconv.Atoi(body)
		if err != nil {
			return nil, err
		}
		if n < 0 {
			return nil, ErrNil
		}
		buf := make([]byte, n+2)
		if _, err := readFull(r, buf); err != nil {
			return nil, err
		}
		return string(buf[:n]), nil
	case '*':
		n, err := strconv.Atoi(body)
		if err != nil {
			return nil, err
		}
		if n < 0 {
			return nil, ErrNil
		}
		arr := make([]interface{}, n)
		for i := 0; i < n; i++ {
			v, err := readReply(r)
			if err == ErrNil {
				arr[i] = nil
				continue
			}
			if err != nil {
				return nil, err
			}
			arr[i] = v
		}
		return arr, nil
	default:
		return nil, fmt.Errorf("redis: unknown reply type %q", line[0])
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

func toString(v interface{}) (string, error) {
	if v == nil {
		return "", ErrNil
	}
	switch t := v.(type) {
	case string:
		return t, nil
	case int64:
		return strconv.FormatInt(t, 10), nil
	default:
		return "", fmt.Errorf("redis: unexpected reply type %T", v)
	}
}

func toInt(v interface{}) (int64, error) {
	switch t := v.(type) {
	case int64:
		return t, nil
	case string:
		return strconv.ParseInt(t, 10, 64)
	default:
		return 0, fmt.Errorf("redis: unexpected reply type %T", v)
	}
}

func toSlice(v interface{}) ([]string, error) {
	if v == nil {
		return nil, nil
	}
	arr, ok := v.([]interface{})
	if !ok {
		return nil, fmt.Errorf("redis: unexpected reply type %T", v)
	}
	out := make([]string, 0, len(arr))
	for _, e := range arr {
		s, err := toString(e)
		if err == ErrNil {
			continue
		}
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

func (c *RedisClient) Ping() error {
	_, err := c.do("PING")
	return err
}

func (c *RedisClient) Get(key string) (string, error) {
	v, err := c.do("GET", key)
	if err != nil {
		return "", err
	}
	return toString(v)
}

func (c *RedisClient) Set(key, value string) error {
	_, err := c.do("SET", key, value)
	return err
}

func (c *RedisClient) Del(keys ...string) (int64, error) {
	args := append([]string{"DEL"}, keys...)
	v, err := c.do(args...)
	if err != nil {
		return 0, err
	}
	return toInt(v)
}

func (c *RedisClient) HSet(key string, fieldValues ...string) (int64, error) {
	args := append([]string{"HSET", key}, fieldValues...)
	v, err := c.do(args...)
	if err != nil {
		return 0, err
	}
	return toInt(v)
}

func (c *RedisClient) HGet(key, field string) (string, error) {
	v, err := c.do("HGET", key, field)
	if err != nil {
		return "", err
	}
	return toString(v)
}

func (c *RedisClient) HGetAll(key string) (map[string]string, error) {
	v, err := c.do("HGETALL", key)
	if err != nil {
		return nil, err
	}
	arr, _ := v.([]interface{})
	out := make(map[string]string, len(arr)/2)
	for i := 0; i+1 < len(arr); i += 2 {
		k, _ := toString(arr[i])
		val, _ := toString(arr[i+1])
		out[k] = val
	}
	return out, nil
}

func (c *RedisClient) HIncrBy(key, field string, delta int64) (int64, error) {
	v, err := c.do("HINCRBY", key, field, strconv.FormatInt(delta, 10))
	if err != nil {
		return 0, err
	}
	return toInt(v)
}

func (c *RedisClient) LPush(key string, values ...string) (int64, error) {
	args := append([]string{"LPUSH", key}, values...)
	v, err := c.do(args...)
	if err != nil {
		return 0, err
	}
	return toInt(v)
}

func (c *RedisClient) RPush(key string, values ...string) (int64, error) {
	args := append([]string{"RPUSH", key}, values...)
	v, err := c.do(args...)
	if err != nil {
		return 0, err
	}
	return toInt(v)
}

func (c *RedisClient) LRange(key string, start, stop int64) ([]string, error) {
	v, err := c.do("LRANGE", key, strconv.FormatInt(start, 10), strconv.FormatInt(stop, 10))
	if err != nil {
		return nil, err
	}
	return toSlice(v)
}

func (c *RedisClient) BRPop(timeoutSec int, keys ...string) (string, string, error) {
	args := append([]string{"BRPOP"}, keys...)
	args = append(args, strconv.Itoa(timeoutSec))
	v, err := c.do(args...)
	if err != nil {
		return "", "", err
	}
	arr, _ := v.([]interface{})
	if len(arr) != 2 {
		return "", "", fmt.Errorf("redis: malformed BRPOP reply")
	}
	k, _ := toString(arr[0])
	val, _ := toString(arr[1])
	return k, val, nil
}

func (c *RedisClient) XAdd(key string, fields map[string]string) (string, error) {
	args := []string{"XADD", key, "*"}
	for f, v := range fields {
		args = append(args, f, v)
	}
	v, err := c.do(args...)
	if err != nil {
		return "", err
	}
	return toString(v)
}

func (c *RedisClient) XRange(key string, start, stop string) ([]StreamEntry, error) {
	v, err := c.do("XRANGE", key, start, stop)
	if err != nil {
		return nil, err
	}
	arr, _ := v.([]interface{})
	out := make([]StreamEntry, 0, len(arr))
	for _, e := range arr {
		pair, _ := e.([]interface{})
		if len(pair) != 2 {
			continue
		}
		id, _ := toString(pair[0])
		fv, _ := pair[1].([]interface{})
		fields := make(map[string]string, len(fv)/2)
		for i := 0; i+1 < len(fv); i += 2 {
			k, _ := toString(fv[i])
			val, _ := toString(fv[i+1])
			fields[k] = val
		}
		out = append(out, StreamEntry{ID: id, Fields: fields})
	}
	return out, nil
}

func (c *RedisClient) SAdd(key string, members ...string) (int64, error) {
	args := append([]string{"SADD", key}, members...)
	v, err := c.do(args...)
	if err != nil {
		return 0, err
	}
	return toInt(v)
}

func (c *RedisClient) SIsMember(key, member string) (bool, error) {
	v, err := c.do("SISMEMBER", key, member)
	if err != nil {
		return false, err
	}
	n, err := toInt(v)
	return n == 1, err
}

func (c *RedisClient) SMembers(key string) ([]string, error) {
	v, err := c.do("SMEMBERS", key)
	if err != nil {
		return nil, err
	}
	return toSlice(v)
}

func (c *RedisClient) HKeys(key string) ([]string, error) {
	v, err := c.do("HKEYS", key)
	if err != nil {
		return nil, err
	}
	return toSlice(v)
}
