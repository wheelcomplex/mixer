// NoopTCPFilter is a no op DispFilter for mixer

package mixer

import (
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

// DispFilter assemble/disassemble/dispatcher raw data when called read/write by TranFilter
// dispatcher usage: New() -> BindUpStream() -> called read/write by TranFilter
type DispFilter interface {

	//
	New() DispFilter

	// BindUpStream bind up stream DispFilter to DispFilter
	BindUpStream(rw DispFilter) error

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

	//
	Reset()

	//
	Close() error
}

// TranFilter encode/decode frame when called Forward by master
// usage: New() -> AddTranStream() -> AddDispStream() -> called Forward() by master
type TranFilter interface {

	//
	New() TranFilter

	// AddUpFilter add DispChain to filter
	AddUpFilter(rw *DispChain) error

	// AddTranStream add io.ReadWriter to filter
	AddTranStream(rw io.ReadWriteCloser) error

	// inbytes for bytes read from DispChain
	// outbytes for bytes write to DispChain
	Forward() (inbytes uint64, outbytes uint64, err error)

	//
	Close() error

	//
	Wait() <-chan error

	// RealsedDispChain release DispChain from filter
	RealsedDispChain() <-chan *DispChain
}

//
type Unit struct {
	Name   string
	Filter DispFilter
}

//
type DispChain struct {
	m     sync.Mutex // mutex
	first DispFilter
	last  DispFilter
	units map[string]DispFilter
	order []string
}

//
func NewDispChain() *DispChain {
	return &DispChain{
		units: make(map[string]DispFilter),
		order: make([]string, 0, 64),
	}
}

//
func (xf *DispChain) AddDispFilter(name string, tf DispFilter) {
	xf.m.Lock()
	defer xf.m.Unlock()
	if _, ok := xf.units[name]; ok == true {
		return
	}
	if xf.first == nil {
		xf.first = tf
	}
	xf.units[name] = tf
	xf.order = append(xf.order, name)
	if xf.last != nil {
		tf.BindUpStream(xf.last)
	}
	xf.last = tf
	return
}

//
func (xf *DispChain) New() DispFilter {
	return xf.Clone()
}

//
func (xf *DispChain) BindUpStream(rw DispFilter) error {
	if xf.first == nil {
		return errors.New("can not bind raw stream to empty DispChain")
	}
	return xf.first.BindUpStream(rw)
}

//
func (xf *DispChain) Read(p []byte) (n int, err error) {
	if xf.last == nil {
		err = errors.New("can not read from empty DispChain")
		return
	}
	return xf.last.Read(p)
}

//
func (xf *DispChain) Write(p []byte) (n int, err error) {
	if xf.last == nil {
		err = errors.New("can not write to empty DispChain")
		return
	}
	return xf.last.Write(p)
}

//
func (xf *DispChain) SetReadDeadline(t time.Time) error {
	if xf.first == nil {
		return errors.New("can not SetReadDeadline at empty DispChain")
	}
	return xf.first.SetReadDeadline(t)
}

//
func (xf *DispChain) SetWriteDeadline(t time.Time) error {
	if xf.first == nil {
		return errors.New("can not SetWriteDeadline at empty DispChain")
	}
	return xf.first.SetWriteDeadline(t)
}

//
func (xf *DispChain) SetDeadline(t time.Time) error {
	if xf.first == nil {
		return errors.New("can not SetDeadline at empty DispChain")
	}
	return xf.first.SetDeadline(t)
}

//
func (xf *DispChain) Close() error {
	if xf.last == nil {
		return nil
	}
	// last will close all upstream filter
	return xf.last.Close()
}

//
func (xf *DispChain) Clone() *DispChain {
	newxf := NewDispChain()
	for _, uidx := range xf.order {
		newxf.AddDispFilter(uidx, xf.units[uidx].New())
	}
	return newxf
}

//
func (xf *DispChain) Reset() {
	xf.last.Reset()
}

//
func (xf *DispChain) First() DispFilter {
	return xf.first
}

//
func (xf *DispChain) Last() DispFilter {
	return xf.last
}

//
func (xf *DispChain) Len() int {
	return len(xf.order)
}

//
func (xf *DispChain) Range() <-chan *Unit {
	tfs := make(chan *Unit, xf.Len())
	for _, uidx := range xf.order {
		tfs <- &Unit{
			Name:   uidx,
			Filter: xf.units[uidx],
		}
	}
	close(tfs)
	return tfs
}

// NoopTCPFilter has not multi-io support
type NoopTCPFilter struct {
	rawio    *net.TCPConn
	upstream DispFilter
}

//
func NewNoopTCPFilter(rw *net.TCPConn) *NoopTCPFilter {
	return &NoopTCPFilter{
		rawio: rw,
	}
}

//
func (xf *NoopTCPFilter) New() DispFilter {
	return NewNoopTCPFilter(nil)
}

//
func (xf *NoopTCPFilter) Reset() {
	if xf.upstream != nil {
		xf.upstream.Reset()
	}
	if xf.rawio != nil {
		xf.rawio.Close()
	}
	xf.rawio = nil
	return
}

//
func (xf *NoopTCPFilter) BindUpStream(rw DispFilter) error {
	if xf.upstream != nil {
		xf.upstream.Close()
	}
	xf.upstream = rw
	return nil
}

//
func (xf *NoopTCPFilter) Read(p []byte) (n int, err error) {
	if xf.upstream != nil {
		//tpf("NoopTCPFilter, read from upstream %v, %v\n", xf.upstream, p)
		return xf.upstream.Read(p)
	}
	if xf.rawio != nil {
		//tpf("NoopTCPFilter, read from rawio %v, %v\n", xf.rawio, p)
		return xf.rawio.Read(p)
	}
	err = errors.New("can not read from empty NoopTCPFilter")
	return
}

//
func (xf *NoopTCPFilter) Write(p []byte) (n int, err error) {
	if xf.upstream != nil {
		return xf.upstream.Write(p)
	}
	if xf.rawio != nil {
		return xf.rawio.Write(p)
	}
	err = errors.New("can not Write to empty NoopTCPFilter")
	return
}

//
func (xf *NoopTCPFilter) SetReadDeadline(t time.Time) error {
	if xf.upstream != nil {
		return xf.upstream.SetReadDeadline(t)
	}
	if xf.rawio != nil {
		return xf.rawio.SetReadDeadline(t)
	}
	return errors.New("can not SetReadDeadline at empty NoopTCPFilter")
}

//
func (xf *NoopTCPFilter) SetWriteDeadline(t time.Time) error {
	if xf.upstream != nil {
		return xf.upstream.SetWriteDeadline(t)
	}
	if xf.rawio != nil {
		return xf.rawio.SetWriteDeadline(t)
	}
	return errors.New("can not SetWriteDeadline at empty NoopTCPFilter")
}

//
func (xf *NoopTCPFilter) SetDeadline(t time.Time) error {
	if xf.upstream != nil {
		return xf.upstream.SetDeadline(t)
	}
	if xf.rawio != nil {
		return xf.rawio.SetDeadline(t)
	}
	return errors.New("can not SetDeadline at empty NoopTCPFilter")
}

//
func (xf *NoopTCPFilter) Close() error {
	if xf.upstream != nil {
		xf.upstream.Close()
	}
	if xf.rawio != nil {
		xf.rawio.Close()
	}
	xf.rawio = nil
	return nil
}
