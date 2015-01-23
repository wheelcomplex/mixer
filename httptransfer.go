// http echo server for mixer

package mixer

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/wheelcomplex/preinit/cmtp"
	"github.com/wheelcomplex/preinit/queues"
)

// http state
const (
	HTTP_STATE_KEEPALIVE int = iota
	HTTP_STATE_READREQHEADER
	HTTP_STATE_PARSEREQHEADER
	HTTP_STATE_READREQBODY
	HTTP_STATE_PARSEREQBODY
	HTTP_STATE_SENDHEADER
	HTTP_STATE_SENDBODY
	HTTP_STATE_CLOSED
)

const (
	HTTP_MAX_HEADER_SIZE  int = 4 * 1024
	HTTP_MINI_HEADER_SIZE int = 6 // GET /\n
)

var http200ok []byte = []byte(`HTTP/1.1 200 OK
Connection: Keep-Alive
Content-Length: 0

`)

//
func httpMethodParser(buf []byte) (int, error) {
	//tpf("httpMethodParser, %d: %s\n", len(buf), buf)
	if len(buf) == 0 {
		// invalid request
		return 2, errors.New("invalid http request method , short buf")
	}
	if buf[0] != byte('G') && buf[0] != byte('P') && buf[0] != byte('H') {
		// invalid request
		return 2, errors.New("invalid http request method, ivalid char")
	}
	switch {
	case len(buf) > 5 && bytes.Equal(buf[:5], []byte("GET /")):
		// GET /
		//tpf("httpMethodParser, GET / %d: %s\n", len(buf), buf)
	case len(buf) > 6 && bytes.Equal(buf[:6], []byte("POST /")):
		// POST /
		//tpf("httpMethodParser, POST / %d: %s\n", len(buf), buf)
	case len(buf) > 6 && bytes.Equal(buf[:6], []byte("HEAD /")):
		// HEAD /
		//tpf("httpMethodParser, HEAD / %d: %s\n", len(buf), buf)
	default:
		// invalid request
		return 2, errors.New("invalid http request method")
	}
	return 0, nil
}

// httpURIParser find \r\n\r\n or \n\n
// return 0 for found, 1 for read more, 2 for error
func httpURIParser(buf []byte) (int, error) {
	//tpf("httpURIParser: %s\n", buf)
	//tpf("httpURIParser: %v\n", buf)
	mcode, merr := httpMethodParser(buf)
	if mcode != 0 {
		return mcode, merr
	}
	for {
		buflen := len(buf)
		ptr := bytes.IndexByte(buf, byte('\n'))
		// TODO: byte('\n') => const, []byte("POST /") => const
		//tpf("httpURIParser: %d, %v\n", ptr, ptr)
		if ptr < 0 {
			// read more
			return 1, nil
		} else {
			// ptr >= 0
			if ptr < HTTP_MINI_HEADER_SIZE {
				// invalid request
				return 2, errors.New("invalid http request when <n> found")
			}
			if ptr+1 >= buflen {
				// read more
				return 1, nil
			}
			if buf[ptr+1] == '\n' {
				// \n\n found
				return 0, nil
			}
			if ptr+2 >= buflen {
				// read more
				return 1, nil
			}
			if buf[ptr-1] == '\r' && buf[ptr+1] == '\r' && buf[ptr+2] == '\n' {
				// \r\n\r\n found
				return 0, nil
			}
			ptr++
			if ptr >= buflen {
				// read more
				return 1, nil
			}
			buf = buf[ptr:]
			// next check
		}
	}
}

// HttpEchoTransfer has multi-io support
type HttpEchoTransfer struct {
	newrawio   chan *net.TCPConn
	lastIdx    int
	rawio      []*net.TCPConn
	idleslot   *queues.LifoQ
	workerMsg  chan *cmtp.CMsg
	closing    chan struct{}
	closed     chan struct{}
	m          sync.Mutex
	forwarding bool
	waitCh     chan error
}

//
func NewHttpEchoTransfer() *HttpEchoTransfer {
	return &HttpEchoTransfer{
		newrawio:  make(chan *net.TCPConn, cmtp.CHANNEL_BUFFER_SIZE),
		lastIdx:   -1,
		rawio:     make([]*net.TCPConn, 0, cmtp.CHANNEL_BUFFER_SIZE),
		idleslot:  queues.NewLifoQ(cmtp.CHANNEL_BUFFER_SIZE * 64),
		workerMsg: make(chan *cmtp.CMsg, cmtp.CHANNEL_BUFFER_SIZE),
		waitCh:    make(chan error, cmtp.CHANNEL_INIT_SIZE),
		closing:   make(chan struct{}, cmtp.CHANNEL_INIT_SIZE),
		closed:    make(chan struct{}, cmtp.CHANNEL_INIT_SIZE),
	}
}

// HandleConn add *net.TCPConn to Transfer
// NOTE: HttpEchoTransfer do nothing to TranStream
func (xf *HttpEchoTransfer) HandleConn(rw *net.TCPConn) error {
	select {
	case <-xf.closing:
		return errors.New("HttpEchoTransfer already closed")
	default:
		xf.newrawio <- rw
	}
	return nil
}

//
func (xf *HttpEchoTransfer) echoserver(rwidx int, wg *sync.WaitGroup) {
	defer wg.Done()

	state := HTTP_STATE_KEEPALIVE
	// xf.workerMsg
	inmsg := &cmtp.CMsg{
		Id: uint64(rwidx),
	}
	outmsg := &cmtp.CMsg{
		Id: uint64(rwidx),
	}
	var rw *net.TCPConn
	// TODO: use slab pool/sync pool
	buf := make([]byte, HTTP_MAX_HEADER_SIZE) // 4k
	iotimeout := time.Second * 5
	var nowts, iostarts, timelimit time.Time
	var totalreadbytes, totalwritebytes int
	var reqcount int
	var emptyio int
	rw = xf.rawio[rwidx]
	tpf("echoserver, new connection#%d, %v\n", rwidx, rw)
	for {
		select {
		case <-xf.closing:
			inmsg.Err = fmt.Errorf("conn#%d, closed for master closing", rwidx)
			tpf("%s ...\n", inmsg.Err.Error())
			xf.workerMsg <- inmsg
			//xf.workerMsg <- outmsg
			return
		default:
		}
		for {
			switch state {
			case HTTP_STATE_KEEPALIVE:
				reqcount++
				//tpf("conn#%d, keepalive request#%d ...\n", rwidx, reqcount)
				state = HTTP_STATE_READREQHEADER
				iostarts = time.Now()
				timelimit = iostarts.Add(iotimeout)
				rw.SetDeadline(timelimit)
				buf = buf[:cap(buf)]
				totalreadbytes = 0
				emptyio = 0
				fallthrough
			case HTTP_STATE_READREQHEADER:
				//for item := range rw.Range() {
				//	tpf("reading from %s %v, limit %v\n", item.Name, item.Filter, timelimit)
				//}
				readbytes, readerr := rw.Read(buf[totalreadbytes:])
				if readbytes > 0 {
					inmsg.Code += uint64(readbytes)
					totalreadbytes += readbytes
					// try to parse
					if totalreadbytes >= HTTP_MINI_HEADER_SIZE {
						pcode, perr := httpURIParser(buf[:totalreadbytes])
						switch pcode {
						case 0:
							// it is ok, \n\n found
							//tpf("conn#%d, LFRT found.\n", rwidx)
							state = HTTP_STATE_PARSEREQHEADER
							continue
						case 1:
							// need more bytes
							//tpf("conn#%d, LFRT no found.\n", rwidx)
							if totalreadbytes >= HTTP_MAX_HEADER_SIZE {
								inmsg.Err = fmt.Errorf("conn#%d, header too large: %d >= %d", rwidx, totalreadbytes, HTTP_MAX_HEADER_SIZE)
								tpf("%s ...\n", inmsg.Err.Error())
								xf.workerMsg <- inmsg
								//xf.workerMsg <- outmsg
								return
							}
							continue
						default:
							// error
							inmsg.Err = fmt.Errorf("conn#%d, httpURIParser error: %s", rwidx, perr)
							tpf("%s ...\n", inmsg.Err.Error())
							xf.workerMsg <- inmsg
							//xf.workerMsg <- outmsg
							return
						}
					}
					if readerr != nil {
						if emptyio < cmtp.CMTP_MAX_EMPTY_IO {
							emptyio++
						} else {
							inmsg.Err = fmt.Errorf("conn#%d, HTTP_STATE_READREQHEADER error: %s", rwidx, "too many read error")
							tpf("%s ...\n", inmsg.Err.Error())
							xf.workerMsg <- inmsg
							//xf.workerMsg <- outmsg
							return
						}
					}
					//
				} else {
					if readerr == nil {
						if emptyio < cmtp.CMTP_MAX_EMPTY_IO {
							emptyio++
						} else {
							inmsg.Err = fmt.Errorf("conn#%d, HTTP_STATE_READREQHEADER error: %s", rwidx, "too many empty read")
							tpf("%s ...\n", inmsg.Err.Error())
							xf.workerMsg <- inmsg
							//xf.workerMsg <- outmsg
							return
						}
					} else {
						inmsg.Err = readerr
						if totalreadbytes > 0 {
							tpf("conn#%d, HTTP_STATE_READREQHEADER normal error: %s\n", rwidx, inmsg.Err.Error())
						}
						xf.workerMsg <- inmsg
						//xf.workerMsg <- outmsg
						return
					}
				}
			case HTTP_STATE_PARSEREQHEADER:
				fallthrough
			case HTTP_STATE_READREQBODY:
				fallthrough
			case HTTP_STATE_PARSEREQBODY:
				iostarts = time.Now()
				timelimit = iostarts.Add(iotimeout)
				rw.SetDeadline(timelimit)
				totalwritebytes = 0
				emptyio = 0
				// send http200ok response
				//tpf("send: %v\n", http200ok)
				//tpf("send: %s\n", http200ok)
				fallthrough
			case HTTP_STATE_SENDHEADER:
				fallthrough
			case HTTP_STATE_SENDBODY:
				writebytes, writeerr := rw.Write(http200ok[totalwritebytes:])
				if writebytes > 0 {
					outmsg.Code += uint64(writebytes)
					totalwritebytes += writebytes
					// try to parse
					if totalwritebytes >= len(http200ok) {
						state = HTTP_STATE_KEEPALIVE
					}
					if writeerr != nil {
						if emptyio < cmtp.CMTP_MAX_EMPTY_IO {
							emptyio++
						} else {
							outmsg.Err = fmt.Errorf("conn#%d, HTTP_STATE_SENDBODY error: %s", rwidx, "too many write error")
							tpf("%s ...\n", outmsg.Err.Error())
							//xf.workerMsg <- inmsg
							xf.workerMsg <- outmsg
							return
						}
					}
					//
				} else {
					if writeerr == nil {
						if emptyio < cmtp.CMTP_MAX_EMPTY_IO {
							emptyio++
						} else {
							outmsg.Err = fmt.Errorf("conn#%d, HTTP_STATE_SENDBODY error: %s", rwidx, "too many empty write")
							tpf("%s ...\n", outmsg.Err.Error())
							//xf.workerMsg <- inmsg
							xf.workerMsg <- outmsg
							return
						}
					} else {
						outmsg.Err = writeerr
						tpf("%s ...\n", outmsg.Err.Error())
						//xf.workerMsg <- inmsg
						xf.workerMsg <- outmsg
						return
					}
				}
			case HTTP_STATE_CLOSED:
				// never reach here befor header Connection: close parsed
				//xf.workerMsg <- inmsg
				xf.workerMsg <- outmsg
				return
			default:
				inmsg.Err = fmt.Errorf("conn#%d, invalid http state: %d", rwidx, state)
				tpf("%s ...\n", inmsg.Err.Error())
				xf.workerMsg <- inmsg
				//xf.workerMsg <- outmsg
				return
			}
		}
	}
}

// Forward bytes beetwen raw stream and tran stream
// inbytes for bytes read from raw stream
// outbytes for bytes write to raw stream
func (xf *HttpEchoTransfer) Forward() (inbytes uint64, outbytes uint64, err error) {
	xf.m.Lock()
	defer xf.m.Unlock()
	if xf.forwarding {
		err = errors.New("already forwarding")
		return
	}
	var wg sync.WaitGroup
	/*
		// CMsg
		type CMsg struct {
			Code uint64
			Id   uint64
			Msg  []byte
			Err  error
		}
	*/
	var msg *cmtp.CMsg
	var rw *net.TCPConn
	/*
		newrawio  chan io.ReadWriteCloser
		lastIdx   int
		rawio     []io.ReadWriteCloser
		idleslot  *queues.LifoQ
		workerMsg chan *cmtp.CMsg
	*/
	var rwidx int
	time.Sleep(1e9)
	tpf("HttpEchoTransfer forwarding ...\n")
	for {
		select {
		case rw = <-xf.newrawio:
			idlecount := xf.idleslot.Pop()
			if idlecount == nil {
				xf.lastIdx++
				rwidx = xf.lastIdx
				xf.rawio = append(xf.rawio, rw)
			} else {
				rwidx = idlecount.(int)
				xf.rawio[rwidx] = rw
			}
			wg.Add(1)
			go xf.echoserver(rwidx, &wg)
		case msg = <-xf.workerMsg:
			//tpf("echoserver exit with msg %v\n", msg)
			//tpf("echoserver exit with msg %s\n", msg.String())
			//msg = <-xf.workerMsg
			//tpf("echoserver exit with msg2 %s\n", msg.String())
			xf.rawio[msg.Id].Close()
			xf.rawio[msg.Id] = nil
			xf.idleslot.Push(int(msg.Id))
			// TODO: in/out bytes in msg.Msg
		}
	}
	wg.Wait()
	close(xf.closed)
	return
}

//
func (xf *HttpEchoTransfer) Close() error {
	if xf.closing != nil {
		close(xf.closing)
		// waiting for forwarder closed
		<-xf.closed
	}
	if xf.rawio != nil {
		for idx, _ := range xf.rawio {
			if xf.rawio[idx] != nil {
				xf.rawio[idx].Close()
			}
		}
	}
	xf.rawio = nil
	if xf.tranio != nil {
		xf.tranio.Close()
	}
	xf.tranio = nil
	select {
	case <-xf.waitCh:
	default:
		close(xf.waitCh)
	}
	return nil
}

//
func (xf *HttpEchoTransfer) Wait() <-chan error {
	return xf.waitCh
}
