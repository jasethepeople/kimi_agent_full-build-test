package redisclient

import (
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// FakeClient is an in-memory implementation of Client used in unit tests.
// It mirrors Redis semantics for the subset of commands the orchestrator uses.
type FakeClient struct {
	mu       sync.Mutex
	strings  map[string]string
	hashes   map[string]map[string]string
	lists    map[string][]string
	sets     map[string]map[string]bool
	streams  map[string][]StreamEntry
	streamID atomic.Int64
}

// NewFake creates an empty in-memory Redis.
func NewFake() *FakeClient {
	return &FakeClient{
		strings: map[string]string{},
		hashes:  map[string]map[string]string{},
		lists:   map[string][]string{},
		sets:    map[string]map[string]bool{},
		streams: map[string][]StreamEntry{},
	}
}

func (f *FakeClient) Ping() error  { return nil }
func (f *FakeClient) Close() error { return nil }

func (f *FakeClient) Get(key string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.strings[key]
	if !ok {
		return "", ErrNil
	}
	return v, nil
}

func (f *FakeClient) Set(key, value string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.strings[key] = value
	return nil
}

func (f *FakeClient) Del(keys ...string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var n int64
	for _, k := range keys {
		if _, ok := f.strings[k]; ok {
			delete(f.strings, k)
			n++
		}
		if _, ok := f.hashes[k]; ok {
			delete(f.hashes, k)
			n++
		}
		if _, ok := f.lists[k]; ok {
			delete(f.lists, k)
			n++
		}
		if _, ok := f.sets[k]; ok {
			delete(f.sets, k)
			n++
		}
		if _, ok := f.streams[k]; ok {
			delete(f.streams, k)
			n++
		}
	}
	return n, nil
}

func (f *FakeClient) HSet(key string, fieldValues ...string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	h, ok := f.hashes[key]
	if !ok {
		h = map[string]string{}
		f.hashes[key] = h
	}
	var added int64
	for i := 0; i+1 < len(fieldValues); i += 2 {
		if _, exists := h[fieldValues[i]]; !exists {
			added++
		}
		h[fieldValues[i]] = fieldValues[i+1]
	}
	return added, nil
}

func (f *FakeClient) HGet(key, field string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	h, ok := f.hashes[key]
	if !ok {
		return "", ErrNil
	}
	v, ok := h[field]
	if !ok {
		return "", ErrNil
	}
	return v, nil
}

func (f *FakeClient) HGetAll(key string) (map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]string{}
	for k, v := range f.hashes[key] {
		out[k] = v
	}
	return out, nil
}

func (f *FakeClient) HKeys(key string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := []string{}
	for k := range f.hashes[key] {
		out = append(out, k)
	}
	sort.Strings(out)
	return out, nil
}

func (f *FakeClient) HIncrBy(key, field string, delta int64) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	h, ok := f.hashes[key]
	if !ok {
		h = map[string]string{}
		f.hashes[key] = h
	}
	cur, _ := strconv.ParseInt(h[field], 10, 64)
	cur += delta
	h[field] = strconv.FormatInt(cur, 10)
	return cur, nil
}

func (f *FakeClient) LPush(key string, values ...string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	l := f.lists[key]
	for _, v := range values {
		l = append([]string{v}, l...)
	}
	f.lists[key] = l
	return int64(len(l)), nil
}

func (f *FakeClient) RPush(key string, values ...string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lists[key] = append(f.lists[key], values...)
	return int64(len(f.lists[key])), nil
}

func (f *FakeClient) LRange(key string, start, stop int64) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	l := f.lists[key]
	n := int64(len(l))
	if start < 0 {
		start += n
	}
	if stop < 0 {
		stop += n
	}
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop || n == 0 {
		return []string{}, nil
	}
	out := make([]string, stop-start+1)
	copy(out, l[start:stop+1])
	return out, nil
}

func (f *FakeClient) BRPop(timeoutSec int, keys ...string) (string, string, error) {
	deadline := time.Now().Add(time.Duration(timeoutSec) * time.Second)
	for {
		f.mu.Lock()
		for _, k := range keys {
			l := f.lists[k]
			if len(l) > 0 {
				v := l[len(l)-1]
				f.lists[k] = l[:len(l)-1]
				f.mu.Unlock()
				return k, v, nil
			}
		}
		f.mu.Unlock()
		if time.Now().After(deadline) {
			return "", "", ErrNil
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (f *FakeClient) XAdd(key string, fields map[string]string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := strconv.FormatInt(time.Now().UnixMilli(), 10) + "-" +
		strconv.FormatInt(f.streamID.Add(1), 10)
	cp := make(map[string]string, len(fields))
	for k, v := range fields {
		cp[k] = v
	}
	f.streams[key] = append(f.streams[key], StreamEntry{ID: id, Fields: cp})
	return id, nil
}

func (f *FakeClient) XRange(key string, start, stop string) ([]StreamEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]StreamEntry, len(f.streams[key]))
	copy(out, f.streams[key])
	return out, nil
}

func (f *FakeClient) SAdd(key string, members ...string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.sets[key]
	if !ok {
		s = map[string]bool{}
		f.sets[key] = s
	}
	var added int64
	for _, m := range members {
		if !s[m] {
			s[m] = true
			added++
		}
	}
	return added, nil
}

func (f *FakeClient) SIsMember(key, member string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sets[key][member], nil
}

func (f *FakeClient) SMembers(key string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := []string{}
	for m := range f.sets[key] {
		out = append(out, m)
	}
	sort.Strings(out)
	return out, nil
}
