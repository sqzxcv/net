// Tests for the netun fork addition requestBody.ReadLazy.

package http2

import (
	"bytes"
	"errors"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

// lazyBuf counts how many times a buffer was asked for.
type lazyBuf struct {
	size  int
	calls atomic.Int32
}

func (l *lazyBuf) get() []byte { l.calls.Add(1); return make([]byte, l.size) }

type lazyResult struct {
	p   []byte
	n   int
	err error
}

func readLazyAsync(p *pipe, l *lazyBuf) chan lazyResult {
	ch := make(chan lazyResult, 1)
	go func() {
		d, n, err := p.readLazy(l.get)
		ch <- lazyResult{d, n, err}
	}()
	return ch
}

func TestPipeReadLazyWaitsWithoutBuffer(t *testing.T) {
	p := &pipe{b: new(bytes.Buffer)}
	l := &lazyBuf{size: 3}
	ch := readLazyAsync(p, l)
	select {
	case r := <-ch:
		t.Fatalf("readLazy returned on an empty open pipe: %+v", r)
	case <-time.After(50 * time.Millisecond):
	}
	if c := l.calls.Load(); c != 0 {
		t.Fatalf("get called %d times while waiting; want 0", c)
	}
	p.Write([]byte("hello"))
	r := <-ch
	if r.err != nil || r.n != 3 || string(r.p[:r.n]) != "hel" || l.calls.Load() != 1 {
		t.Fatalf("readLazy = %q, %d, %v (get calls %d); want \"hel\", 3, nil (1)", r.p[:r.n], r.n, r.err, l.calls.Load())
	}
	// The rest is still there for the next call, as with Read.
	r = <-readLazyAsync(p, l)
	if r.err != nil || string(r.p[:r.n]) != "lo" {
		t.Fatalf("second readLazy = %q, %v; want \"lo\", nil", r.p[:r.n], r.err)
	}
}

func TestPipeReadLazyCloseSkipsGet(t *testing.T) {
	p := &pipe{b: new(bytes.Buffer)}
	l := &lazyBuf{size: 8}
	ch := readLazyAsync(p, l)
	ran := false
	time.Sleep(20 * time.Millisecond)
	p.closeWithErrorAndCode(io.EOF, func() { ran = true })
	r := <-ch
	if r.err != io.EOF || r.n != 0 || r.p != nil || !ran || l.calls.Load() != 0 {
		t.Fatalf("readLazy after close = %v, %d, %v (readFn ran %v, get calls %d); want nil, 0, EOF, true, 0",
			r.p, r.n, r.err, ran, l.calls.Load())
	}
}

func TestPipeReadLazyBreakSkipsGet(t *testing.T) {
	p := &pipe{b: new(bytes.Buffer)}
	p.Write([]byte("unread"))
	errBoom := errors.New("boom")
	p.BreakWithError(errBoom)
	l := &lazyBuf{size: 8}
	r := <-readLazyAsync(p, l)
	if r.err != errBoom || r.p != nil || l.calls.Load() != 0 {
		t.Fatalf("readLazy after break = %v, %v (get calls %d); want nil, boom, 0", r.p, r.err, l.calls.Load())
	}
}

// TestRequestBodyReadLazy drives a real Server through a real client
// connection: ReadLazy parks on an idle stream without asking for a buffer,
// returns the bytes the client sends, reports EOF at end of body, and is woken
// by Close.
func TestRequestBodyReadLazy(t *testing.T) {
	type step struct {
		data  string
		err   error
		calls int32
	}
	steps := make(chan step, 8)
	waiting := make(chan struct{}, 4)
	closeBody := make(chan struct{})

	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		lr, ok := r.Body.(interface {
			ReadLazy(func() []byte) ([]byte, int, error)
		})
		if !ok {
			t.Errorf("request body %T has no ReadLazy", r.Body)
			return
		}
		for i := 0; i < 3; i++ {
			l := &lazyBuf{size: 64}
			waiting <- struct{}{}
			if i == 2 {
				go func() { <-closeBody; r.Body.Close() }()
			}
			p, n, err := lr.ReadLazy(l.get)
			steps <- step{string(p[:n]), err, l.calls.Load()}
		}
	})

	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	go (&Server{}).ServeConn(c2, &ServeConnOpts{Handler: h})
	cc, err := (&Transport{}).NewClientConn(c1)
	if err != nil {
		t.Fatal(err)
	}
	pr, pw := io.Pipe()
	defer pw.Close()
	req, _ := http.NewRequest("PUT", "https://example.com/", pr)
	resp, err := cc.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	expectParked := func() {
		t.Helper()
		<-waiting
		select {
		case s := <-steps:
			t.Fatalf("handler did not park in ReadLazy: got %+v", s)
		case <-time.After(100 * time.Millisecond):
		}
	}
	expect := func(want step) {
		t.Helper()
		select {
		case s := <-steps:
			if s.data != want.data || s.calls != want.calls || (want.err == nil) != (s.err == nil) {
				t.Fatalf("got %+v; want %+v", s, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for %+v", want)
		}
	}

	for _, msg := range []string{"hello", "world"} {
		expectParked()
		if _, err := pw.Write([]byte(msg)); err != nil {
			t.Fatal(err)
		}
		expect(step{data: msg, calls: 1})
	}
	expectParked()
	close(closeBody)
	expect(step{err: errClosedBody, calls: 0})
}
