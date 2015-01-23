// framework, protocol filter and transfer

/*
work follow:
	mixer start client listener -> accept next conn -> call ProtoStack.Identify(tcpconn) in goroutine
	-> ProtoStack.Identify return initbuf, transfer, pipe
	-> call mixer.AddTransfer(transfer)(will call transfer.Forward() in goroutine)
	-> call pipe.Init(initbuf,tcpconn)
	-> call mixer.HandleInConn(transfer, pipe)
	-> when tcpconn disconnect, transfer send CMsg to mixer

	mixer.AddTransfer(transfer) will initial transfer listener and peer connection befor return
	-> just return nil if already initialed
*/

package mixer

import (
	"errors"
	"net"
	"sync"
	"time"

	"github.com/wheelcomplex/preinit/cmtp"
	"github.com/wheelcomplex/preinit/palloc"
)

//
type ForwardErr struct {
	Income bool
	In     *TransferErr
	Out    *TransferErr
}

// Transfer encode/decode frame when called Forward by master
type Transfer interface {
	//
	New() Transfer

	// HandleInConn add incoming/accepted *net.TCPConn to Transfer
	HandleInConn(initbuf []byte, rw *net.TCPConn) error

	// HandleOutConn add dialout *net.TCPConn to Transfer
	HandleOutConn(initbuf []byte, rw *net.TCPConn) error

	// inbytes for bytes read from *net.TCPConn
	// outbytes for bytes write to *net.TCPConn
	Forward() *ForwardErr

	//
	Close() error
}

// NopTransfer
// accept new pipe, waiting for input, send msg and close pipe
type NopTransfer struct {
	m       sync.Mutex       //
	inCh    chan ProtoPipe   //
	outCh   chan ProtoPipe   //
	closing chan struct{}    //
	closed  chan struct{}    //
	timeout time.Duration    // read timeout
	iostat  chan *ForwardErr //
	count   int64            //
	flag    bool             //
}

//
var NopTransferMsg = []byte("disconnected by NopTransfer")

//
func NewNopTransfer(timeout time.Duration) *NopTransfer {
	if timeout <= 0 {
		timeout = PROTOCOL_DEFAULT_TIMEOUT * time.Microsecond
	}
	return &NopTransfer{
		inCh:    make(chan ProtoPipe, cmtp.CHANNEL_BUFFER_SIZE),
		outCh:   make(chan ProtoPipe, cmtp.CHANNEL_BUFFER_SIZE),
		closing: make(chan struct{}, cmtp.CHANNEL_INIT_SIZE),
		closed:  make(chan struct{}, cmtp.CHANNEL_INIT_SIZE),
		iostat:  make(chan *ForwardErr, cmtp.CHANNEL_BUFFER_SIZE),
		timeout: timeout,
	}
}

//
func (pt *NopTransfer) HandleInConn(initbuf []byte, rw *net.TCPConn) error {
	select {
	case <-pt.closing:
		return errors.New("NopTransfer already closed")
	default:
		pt.inCh <- NewNopProtoPipe(initbuf, rw)
	}
	return nil
}

//
func (pt *NopTransfer) HandleOutConn(initbuf []byte, rw *net.TCPConn) error {
	select {
	case <-pt.closing:
		return errors.New("NopTransfer already closed")
	default:
		pt.outCh <- NewNopProtoPipe(initbuf, rw)
	}
	return nil
}

// msg
func (pt *NopTransfer) msg(count int64, in bool, rw ProtoPipe, wg *sync.WaitGroup) {
	te := &TransferErr{
		Id: count,
	}
	timelimit := time.Now().Add(pt.timeout)
	rbuf := palloc.Get(PROTOCOL_MAX_HEADER_SIZE)
	wn := 0
	rw.SetDeadline(timelimit)
	if in {
		// write msg if something read, or just disconnect
		nr, ne := rw.Read(rbuf)
		if nr > 0 && ne == nil {
			wn, _ = rw.Write(NopTransferMsg)
		}
	} else {
		// write msg and disconnect
		wn, _ = rw.Write(NopTransferMsg)
	}
	rw.Close()
	palloc.Put(rbuf)
	wg.Done()
	return
}

// stat
func (pt *NopTransfer) stat(out chan *TransferErr) {
	te := &TransferErr{}
	for st := range pt.iostat {
		te.InBytes += st.InBytes
		te.OutBytes += st.OutBytes
	}
	out <- te
}

// Forward accept new pipe, waiting for input, send msg and close pipe
func (pt *NopTransfer) Forward() *TransferErr {
	defer close(pt.closed)
	out := make(chan *TransferErr, cmtp.CHANNEL_INIT_SIZE)
	go pt.stat(out)
	wg := sync.WaitGroup{}
	for {
		select {
		case rw := <-pt.inCh:
			pt.count++
			wg.Add(1)
			go pt.msg(pt.count, rw, &wg)
		case rw := <-pt.outCh:
			pt.count++
			wg.Add(1)
			go pt.msg(pt.count, rw, &wg)
		case <-pt.closing:
			// all msg done
			wg.Wait()
			close(pt.iostat)
			// all iostat done
			te := <-out
			te.Name = "NopTransfer"
			return te
		}
	}
}

//
func (pt *NopTransfer) Close() error {
	pt.m.Lock()
	defer pt.m.Unlock()
	if pt.flag {
		// already closed
		return nil
	}
	close(pt.closing)
	// pt.closed will close by Forward()
	<-pt.closed
	pt.flag = true
	close(pt.inCh)
	close(pt.iostat)
	return nil
}

//
// state of protocol identify
type IdentState int

const (
	IDENT_STATE_UNSET  IdentState = iota
	IDENT_STATE_ERROR             // error
	IDENT_STATE_MORE              // no match, please read more
	IDENT_STATE_BYPASS            // bypass in this connection
	IDENT_STATE_STACK             // next identify chain matched
	IDENT_STATE_TRAN              // transfer matched
)

var IdentErrMaxHeader = errors.New("reach max identify buffer size")
var IdentErrEmptyIO = errors.New("reach max empty io")
var IdentErrInvalid = errors.New("identify return invalid state")
var IdentErrStack = errors.New("identify return invalid stack")
var IdentErrTran = errors.New("identify return invalid transfer")

// max bytes for protocol identify
const PROTOCOL_MAX_HEADER_SIZE int = 128

// default timeout for identify, in time.Microsecond
const PROTOCOL_DEFAULT_TIMEOUT time.Duration = 500

// ProtoFilter identify protocol of input stream
// ProtoFilter must impl as multi-routine safe
type ProtoFilter interface {

	//
	New() ProtoFilter

	// MinSize mini buffer size for protocol identify
	MinSize() int

	// MaxSize max buffer size for protocol identify
	MaxSize() int

	// Identify protocol of initbuf, when match return IDENT_STATE_STACK/IDENT_STATE_TRAN
	// IDENT_STATE_STACK for next identifystack
	// IDENT_STATE_TRAN for next transfer
	Identify(initbuf []byte) (state IdentState, nextStack interface{})
}

// NopFilter
// return NoTransfer+NopProtoPipe
type NopFilter struct {
}

//
func NewNopFilter() *NopFilter {
	return &NopFilter{}
}

//
func (pf *NopFilter) Name() string {
	return "NopFilter"

}

//
func (pf *NopFilter) MiniSize() int {
	return 1
}

//
func (pf *NopFilter) MaxSize() int {
	return PROTOCOL_MAX_HEADER_SIZE
}

// Identify protocol of initbuf
func (pf *NopFilter) Identify(initbuf []byte) (state IdentState, next *ProtoStack) {
	if len(initbuf) < 1 {
		state = IDENT_STATE_MORE
		return
	}
	state = IDENT_STATE_TRAN
	next = NewNopTransfer(0)
	return
}

// ProtoStack
// one ProtoStack for one new connection
// Concurrency safe
type ProtoStack struct {
	timeout   time.Duration
	defaultTr Transfer
	filters   []ProtoFilter
	tranxs    []Transfer
	min       []int
	max       []int
	bypass    []bool
	m         sync.Mutex
}

//
func NewProtoStack(timeout time.Duration, defaultTr Transfer) *ProtoStack {
	if timeout <= 0 {
		timeout = time.Millisecond
	}
	if defaultTr == nil {
		defaultTr = NewNopTransfer(0)
	}
	ps := &ProtoStack{
		timeout:   timeout,
		defaultTr: defaultTr,
		filters:   make([]ProtoFilter, 0),
		tranxs:    make([]Transfer, 0),
		min:       make([]int, 0),
		max:       make([]int, 0),
		bypass:    make([]bool, 0),
	}
	//
	return ps
}

//
func (ps *ProtoStack) AddFilter(pf ProtoFilter, ps Transfer) {
	ps.m.Lock()
	defer ps.m.Unlock()
	ps.filters = append(ps.filters, pf)
	ps.tranxs = append(ps.tranxs, ps)
	ps.min = append(ps.min, pf.MinSize())
	ps.max = append(ps.max, pf.MaxSize())
	ps.bypass = append(ps.bypass, false)
}

// SynPoolNew use for sync.Pool
func (ps *ProtoStack) SynPoolNew() func() interface{} {
	return func() interface{} {
		return ps.Clone()
	}
}

//
func (ps *ProtoStack) chainidentify(rbuf []byte) (state IdentState, next *ProtoStack) {
	rptr := len(rbuf)
	if len(ps.filters) == 0 {
		state = IDENT_STATE_TRAN
		next = ps.defaultTr
		return
	}
	bypasscnt := 0
	for idx, _ := range ps.bypass {
		if ps.bypass[idx] {
			bypasscnt++
		}
	}
	if bypasscnt == len(ps.bypass) {
		// all filter disabled
		state = IDENT_STATE_TRAN
		next = ps.defaultTr
		pipe = ps.defaultProtoPipe
		return
	}
	for idx, _ := range ps.filters {
		if ps.bypass[idx] || rptr < ps.min[idx] || rptr > ps.max[idx] {
			continue
		}
		state, next, pipe = ps.filters[idx].Identify(rbuf)
		switch state {
		case IDENT_STATE_MORE:
			// continue to next filter
		case IDENT_STATE_BYPASS:
			ps.bypass[idx] = true
			// continue to next filter
		case IDENT_STATE_STACK:
			_, ok := inext.(*ProtoStack)
			if !ok {
				state = IDENT_STATE_ERROR
				next = IdentErrStack
			}
			// goto next ProtoStack
			return
		case IDENT_STATE_TRAN:
			_, ok := inext.(*Transfer)
			if !ok {
				state = IDENT_STATE_ERROR
				next = IdentErrTran
			}
			// goto transfer
			return
		default:
			next = IdentErrInvalid
			return
		}
	}
	state = IDENT_STATE_MORE
	next = nil
	return
}

// Identify read from network and check every filter with read buffer until timeout/error/matched
// when timeout return default transfer
// returned error or next transfer
func (ps *ProtoStack) Identify(rw *net.TCPConn) (initbuf []byte, tran *Transfer, pipe ProtoPipe, err error) {
	initbuf = make([]byte, PROTOCOL_MAX_HEADER_SIZE)
	var state IdentState
	var next *ProtoStack
	rptr := 0
	rw.SetReadDeadline(time.Now().Add(ps.timeout))
	var emptyio int
	curpc := pc
	for {
		// read stream
		nr, ne := rw.Read(initbuf[rptr:])
		if nr > 0 {
			rptr += nr
		}
		state, next, pipe = curps.chainidentify(initbuf[:rptr])
		switch state {
		case IDENT_STATE_MORE:
			// continue to read
		case IDENT_STATE_STACK:
			curpc = next.(*ProtoStack)
			// continue to identify
		case IDENT_STATE_TRAN:
			// using returned transfer
			err = nil
			tran = next.(*Transfer)
			initbuf = initbuf[:rptr]
			return
		default:
			if _, ok := next.(error); ok {
				err = next.(error)
			} else {
				err = IdentErrInvalid
			}
			return
		}
		if ne != nil {
			// timeout or read error
			if te, ok := ne.(net.OpError); ok {
				if te.Timeout() {
					// read timeout, using default transfer
					err = nil
					tran = curps.defaultTr
					pipe = curps.defaultProtoPipe
					initbuf = initbuf[:rptr]
					return
				}
				if te.Temporary() {
					if emptyio < cmtp.CMTP_MAX_EMPTY_IO {
						emptyio++
						continue
					} else {
						err = IdentErrEmptyIO
						return
					}
				}
			}
			err = ne
			return
		}
		if rptr >= PROTOCOL_MAX_HEADER_SIZE {
			err = IdentErrMaxHeader
			return
		}
		if nr <= 0 {
			if emptyio < cmtp.CMTP_MAX_EMPTY_IO {
				emptyio++
				continue
			} else {
				err = IdentErrEmptyIO
				return
			}
		}
	}
}

//
