// stable code for mixer

package mixer

import (
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

// TransferErr use for carry Transfer exit msg
type TransferErr struct {
	Id       int64
	Name     string
	InBytes  uint64
	OutBytes uint64
	Err      error
}

// String return formated string of CMsg
func (mc *TransferErr) String() string {
	if mc.Err == nil {
		return fmt.Sprintf("transfer#%d %s, in %d, out %d, error: <nil>", mc.Id, mc.Name, mc.InBytes, mc.OutBytes)
	}
	return fmt.Sprintf("transfer#%d %s, in %d, out %d, error: %s", mc.Id, mc.Name, mc.InBytes, mc.OutBytes, mc.Err.Error())
}

// Error return formated string of TransferErr
func (mc TransferErr) Error() string {
	return mc.String()
}

// ProtoPipe assemble/disassemble/dispatcher raw data when called read/write by Transfer
type ProtoPipe interface {

	//
	New() ProtoPipe

	// Chain set rw as top of pipe
	Chain(initbuf []byte, rw ProtoPipe) error

	//
	Read(p []byte) (n int, err error)

	//
	Write(p []byte) (n int, err error)

	//
	SetReadDeadline(t time.Time) error

	//
	SetWriteDeadline(t time.Time) error

	//
	SetDeadline(t time.Time) error

	// Close close upstream pipe and discard internal buffer
	Close() error
}

// NopProtoPipe
// convert *net.TCPConn to ProtoPipe
type NopProtoPipe struct {
	initbuf  []byte
	rawio    *net.TCPConn
	upstream ProtoPipe
	rw       io.ReadWriteCloser
}

//
func NewNopProtoPipe(initbuf []byte, rw *net.TCPConn) *NopProtoPipe {
	return &NopProtoPipe{
		initbuf: initbuf,
		rawio:   rw,
		rw:      rw,
	}
}

//
func (xf *NopProtoPipe) Chain(initbuf []byte, rw ProtoPipe) error {
	palloc.Put(xf.initbuf)
	xf.initbuf = initbuf
	if xf.upstream != nil {
		xf.upstream.Close()
	}
	if xf.rawio != nil {
		xf.rawio.Close()
	}
	xf.upstream = rw
	xf.rw = rw
	return nil
}

//
func (xf *NopProtoPipe) Read(p []byte) (n int, err error) {
	if len(xf.initbuf) > 0 {
		n = copy(p, xf.initbuf)
		xf.initbuf = xf.initbuf[n:]
		if len(xf.initbuf) == 0 {
			palloc.Put(xf.initbuf)
			xf.initbuf = nil
		}
		return
	}
	if xf.rw != nil {
		return xf.rw.Read(p)
	}
	err = errors.New("can not read from empty NopProtoPipe")
	return
}

//
func (xf *NopProtoPipe) Write(p []byte) (n int, err error) {
	if xf.rw != nil {
		return xf.rw.Write(p)
	}
	err = errors.New("can not Write to empty NopProtoPipe")
	return
}

//
func (xf *NopProtoPipe) SetReadDeadline(t time.Time) error {
	if xf.upstream != nil {
		return xf.upstream.SetReadDeadline(t)
	}
	if xf.rawio != nil {
		return xf.rawio.SetReadDeadline(t)
	}
	return errors.New("can not SetReadDeadline at empty NopProtoPipe")
}

//
func (xf *NopProtoPipe) SetWriteDeadline(t time.Time) error {
	if xf.upstream != nil {
		return xf.upstream.SetWriteDeadline(t)
	}
	if xf.rawio != nil {
		return xf.rawio.SetWriteDeadline(t)
	}
	return errors.New("can not SetWriteDeadline at empty NopProtoPipe")
}

//
func (xf *NopProtoPipe) SetDeadline(t time.Time) error {
	if xf.upstream != nil {
		return xf.upstream.SetDeadline(t)
	}
	if xf.rawio != nil {
		return xf.rawio.SetDeadline(t)
	}
	return errors.New("can not SetDeadline at empty NopProtoPipe")
}

//
func (xf *NopProtoPipe) Close() error {
	if xf.upstream != nil {
		xf.upstream.Close()
	}
	if xf.rawio != nil {
		xf.rawio.Close()
	}
	xf.rawio = nil
	xf.rw = nil
	palloc.Put(xf.initbuf)
	xf.initbuf = nil
	return nil
}
