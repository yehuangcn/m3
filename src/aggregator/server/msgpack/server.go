// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package msgpack

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3aggregator/aggregator"
	networkserver "github.com/m3db/m3aggregator/server"
	"github.com/m3db/m3metrics/protocol/msgpack"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/net"

	"github.com/uber-go/tally"
)

const (
	unknownRemoteHostAddress = "<unknown>"
)

type serverMetrics struct {
	openConnections tally.Gauge
	addMetricErrors tally.Counter
	decodeErrors    tally.Counter
}

func newServerMetrics(scope tally.Scope) serverMetrics {
	return serverMetrics{
		openConnections: scope.Gauge("open-connections"),
		addMetricErrors: scope.Counter("add-metric-errors"),
		decodeErrors:    scope.Counter("decode-errors"),
	}
}

type addConnectionFn func(conn net.Conn) bool
type removeConnectionFn func(conn net.Conn)
type handleConnectionFn func(conn net.Conn)

// server is a msgpack based server that receives incoming connections containing
// msgpack-encoded traffic and delegates to the processor to process incoming data
type server struct {
	sync.Mutex

	address      string
	aggregator   aggregator.Aggregator
	listener     net.Listener
	opts         Options
	log          xlog.Logger
	iteratorPool msgpack.UnaggregatedIteratorPool

	closed   int32
	numConns int32
	conns    []net.Conn
	wgConns  sync.WaitGroup
	metrics  serverMetrics

	addConnectionFn    addConnectionFn
	removeConnectionFn removeConnectionFn
	handleConnectionFn handleConnectionFn
}

// NewServer creates a new msgpack server
func NewServer(address string, aggregator aggregator.Aggregator, opts Options) networkserver.Server {
	instrumentOpts := opts.InstrumentOptions()
	scope := instrumentOpts.MetricsScope().SubScope("server")
	s := &server{
		address:      address,
		aggregator:   aggregator,
		opts:         opts,
		log:          instrumentOpts.Logger(),
		iteratorPool: opts.IteratorPool(),
		metrics:      newServerMetrics(scope),
	}

	// Set up the connection functions
	s.addConnectionFn = s.addConnection
	s.removeConnectionFn = s.removeConnection
	s.handleConnectionFn = s.handleConnection

	// Start reporting metrics
	go s.reportMetrics()

	return s
}

func (s *server) ListenAndServe() error {
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}
	s.listener = listener
	go s.serve()
	return nil
}

func (s *server) serve() error {
	connCh, errCh := xnet.StartAcceptLoop(s.listener, s.opts.Retrier())
	for conn := range connCh {
		if !s.addConnectionFn(conn) {
			conn.Close()
		} else {
			s.wgConns.Add(1)
			go s.handleConnectionFn(conn)
		}
	}
	return <-errCh
}

func (s *server) Close() {
	s.Lock()
	if atomic.LoadInt32(&s.closed) == 1 {
		s.Unlock()
		return
	}
	atomic.StoreInt32(&s.closed, 1)
	openConns := make([]net.Conn, len(s.conns))
	copy(openConns, s.conns)
	s.Unlock()

	// Close all open connections
	for _, conn := range openConns {
		conn.Close()
	}

	// Close the listener
	if s.listener != nil {
		s.listener.Close()
	}

	// Wait for all connection handlers to finish
	s.wgConns.Wait()
}

func (s *server) addConnection(conn net.Conn) bool {
	s.Lock()
	defer s.Unlock()

	if atomic.LoadInt32(&s.closed) == 1 {
		return false
	}
	s.conns = append(s.conns, conn)
	atomic.AddInt32(&s.numConns, 1)
	return true
}

func (s *server) removeConnection(conn net.Conn) {
	s.Lock()
	defer s.Unlock()

	numConns := len(s.conns)
	for i := 0; i < numConns; i++ {
		if s.conns[i] == conn {
			// Move the last connection to i and reduce the number of connections by 1
			s.conns[i] = s.conns[numConns-1]
			s.conns = s.conns[:numConns-1]
			atomic.AddInt32(&s.numConns, -1)
			return
		}
	}
}

func (s *server) handleConnection(conn net.Conn) {
	defer func() {
		conn.Close()
		s.removeConnectionFn(conn)
		s.wgConns.Done()
	}()

	it := s.iteratorPool.Get()
	it.Reset(conn)
	defer it.Close()

	// Iterate over the incoming metrics stream and queue up metrics
	for it.Next() {
		metric := it.Metric()
		policiesList := it.PoliciesList()
		if err := s.aggregator.AddMetricWithPoliciesList(metric, policiesList); err != nil {
			s.log.WithFields(
				xlog.NewLogField("metric", metric.String()),
				xlog.NewLogField("policies", policiesList),
				xlog.NewLogErrField(err),
			).Errorf("error adding metric with policies")
			s.metrics.addMetricErrors.Inc(1)
		}
	}

	// If there is an error during decoding, it's likely due to a broken connection
	// and therefore we ignore the EOF error
	if err := it.Err(); err != nil && err != io.EOF {
		remoteAddress := unknownRemoteHostAddress
		if remoteAddr := conn.RemoteAddr(); remoteAddr != nil {
			remoteAddress = remoteAddr.String()
		}
		s.log.WithFields(
			xlog.NewLogField("remoteAddress", remoteAddress),
			xlog.NewLogErrField(err),
		).Error("decode error")
		s.metrics.decodeErrors.Inc(1)
	}
}

func (s *server) reportMetrics() {
	interval := s.opts.InstrumentOptions().ReportInterval()
	t := time.Tick(interval)

	for {
		<-t
		if atomic.LoadInt32(&s.closed) == 1 {
			return
		}
		s.metrics.openConnections.Update(float64(atomic.LoadInt32(&s.numConns)))
	}
}