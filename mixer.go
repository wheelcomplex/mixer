// mixer is a tcp server framework for Common Multiplexing Transport Protocol (CMTP)

package mixer

import (
	"errors"
	"net"
	"runtime"
	"sync"
	"time"

	"github.com/wheelcomplex/preinit/cmtp"
	"github.com/wheelcomplex/preinit/misc"
)

// tpf is short alias of misc.Tpf
var tpf = misc.Tpf

// Config for Mixer
type Config struct {

	// key for remote identify and stream crypt
	//
	Key string

	// timeout in time.Microsecond for protocol identify
	// default is PROTOCOL_DEFAULT_TIMEOUT
	IdentTimeout int

	// tcp listeners
	// listen on host:port for client connection
	// empty list to disable client listening
	// default: map[string]string{"0.0.0.0:6099":"0.0.0.0:6099"}
	TCPListens map[string]string
}

//
func NewConfig(key string) *Config {
	return &Config{
		Key:          key,
		IdentTimeout: PROTOCOL_DEFAULT_TIMEOUT,
		TCPListens:   make(map[string]string),
	}
}

// passiveConfig is default config for MixerServer
var passiveConfig = Config{
	Key:          "",
	TCPListens:   map[string]string{"0.0.0.0:6099": "0.0.0.0:6099"},
	IdentTimeout: PROTOCOL_DEFAULT_TIMEOUT,
}

// Merge feed cfg with value from template
// WARNING: no deep copy
func (cfg *Config) Merge(template *Config) {
	if cfg == nil {
		cfg = NewConfig("")
	}
	if len(cfg.Key) == 0 {
		cfg.Key = template.Key
	}
	if cfg.IdentTimeout <= 0 {
		cfg.IdentTimeout = template.IdentTimeout
	}
	if len(cfg.TCPListens) == 0 && len(template.TCPListens) > 0 {
		for idx, _ := range template.TCPListens {
			cfg.TCPListens[idx] = template.TCPListens[idx]
		}
	}

}

//
type MixerServer struct {
	cfg           *Config                     // config data for server
	token         uint64                      // auth token for remote identify, is hash of Key
	dispListeners map[string]*net.TCPListener //
	closing       chan struct{}               //
	closed        chan struct{}               //
	m             sync.Mutex                  //
	err           error                       //
	waitCh        chan error                  //
	destroyed     chan struct{}               //
	wg            sync.WaitGroup              //
	running       bool                        //
}

// NewServer initial MixerServer using cfg config
func NewServer(cfg *Config) (*MixerServer, error) {
	// default setup
	if cfg == nil {
		cfg = NewConfig("")
	}
	cfg.Merge(&passiveConfig)
	ms := &MixerServer{
		cfg:           cfg,
		dispConns:     make(map[string]map[int]*net.TCPConn),
		dispListeners: make(map[string]*net.TCPListener),
		closing:       make(chan struct{}, 128),
		closed:        make(chan struct{}, 128),
		destroyed:     make(chan struct{}, 128),
		waitCh:        make(chan error, 128),
	}
	//
	return ms, nil
}

// NewListenServer return MixerServer with listener list
func NewListenServer(key string, listens map[string]string) (*MixerServer, error) {
	cfg := NewConfig(key)
	cfg.TCPListens = listens
	return NewServer(cfg)
}

// AddProtoFilter add ProtoFilter(dispacher) to server
// must call befor server start
func (ms *MixerServer) AddProtoFilter(name string, tf ProtoFilter) error {
	ms.m.Lock()
	defer ms.m.Unlock()
	if ms.running {
		return errors.New("can't add filter when server running")
	}
	ms.cfg.DFilters.AddProtoFilter(name, tf)
	return nil
}

// RegisterFilter register protocol filter to server
// must call befor server start
func (ms *MixerServer) RegisterFilter(pf ProtoFilter) error {
	ms.m.Lock()
	defer ms.m.Unlock()
	if ms.running {
		return errors.New("can't add filter when server running")
	}
	ms.cfg.DFilters.AddProtoFilter(name, tf)
	return nil
}

// RegisterTransfer register transfer to server
// must call befor server start
func (ms *MixerServer) RegisterTransfer(tf Transfer) error {
	ms.m.Lock()
	defer ms.m.Unlock()
	if ms.running {
		return errors.New("can't add filter when server running")
	}
	ms.cfg.DFilters.AddProtoFilter(name, tf)
	return nil
}

//
func (ms *MixerServer) closer() {
	// waiting for closing event
	<-ms.closing

	//
	ms.cfg.TFilter.Close()
	ms.cfg.TFilter.Wait()

	// waiting for all goroutine exit
	ms.wg.Wait()

	// all goroutine exited
	close(ms.closed)
	//
	ms.Close()
}

// Wait until server exited
func (ms *MixerServer) Wait() <-chan error {
	return ms.waitCh
}

//// newDispSession used for sync.pool to create session struct
//func (ms *MixerServer) newDispSession() func() interface{} {
//	return func() interface{} {
//		return ms.cfg.DFilters.Clone()
//	}
//}

//
func (ms *MixerServer) dispAcceptClient(idx string, wg *sync.WaitGroup) {
	defer ms.dispListeners[idx].Close()
	defer wg.Done()

	// *net.TCPListener
	// ms.dispListeners[idx]
	// ms.cfg.TFilter
	tpf("dispAcceptClient from  %s ...\n", idx)
	var tempDelay time.Duration // how long to sleep on accept failure
	var errcount int
	max := 1 * time.Second
	for {
		select {
		case <-ms.closing:
			return
		default:
		}
		// TODO: close listener when closing
		newrw, newerr := ms.dispListeners[idx].AcceptTCP()
		if newerr != nil {
			if ne, ok := newerr.(net.Error); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if tempDelay > max {
					tempDelay = max
				}
				tpf("http: Accept error: %v; retrying in %v", newerr, tempDelay)
				time.Sleep(tempDelay)
				errcount++
				if errcount > cmtp.CMTP_MAX_EMPTY_IO {
					tpf("dispAcceptClient %d exit for error: %v\n", idx, newerr)
				} else {
					continue
				}
			}
			tpf("dispAcceptClient %d exit for error: %v\n", idx, newerr)
			return
		}
		//
		//tpf("dispAcceptClient %s, AcceptTCP: %v: %v\n", idx, newrw, newerr)
		tempDelay = 0
		//
		dispchain := ms.cfg.DFilters.Clone()
		if dispchain.Len() == 0 {
			dispchain.AddProtoFilter("noopfilter", NewNoopTCPFilter(newrw))
		} else {
			dispchain.Reset()
			dispchain.BindUpStream(NewNoopTCPFilter(newrw))
		}
		//tpf("dispAcceptClient %s, AcceptTCP: %v: %v\n", idx, newrw, newerr)
		ms.cfg.TFilter.AddFilter(dispchain)
	}
}

//
func (ms *MixerServer) dispListener() {
	defer ms.wg.Done()

	var wg sync.WaitGroup

	for idx, _ := range ms.cfg.TCPListens {
		tpf("Accepting %s for client ...\n", idx)
		wg.Add(1)
		go ms.dispAcceptClient(idx, &wg)
	}
	wg.Wait()
}

//
func (ms *MixerServer) tranAcceptClient(idx string, wg *sync.WaitGroup) {
	defer wg.Done()
	// *net.TCPListener
	// ms.tranListeners[idx]
	select {}
}

//
func (ms *MixerServer) tranListener() {
	defer ms.wg.Done()

	var wg sync.WaitGroup

	for idx, _ := range ms.cfg.TranListens {
		tpf("Accepting %s for transport ...\n", idx)
		wg.Add(1)
		go ms.tranAcceptClient(idx, &wg)
	}
	wg.Wait()
}

//
func (ms *MixerServer) tranPeerHandler(idx string, wg *sync.WaitGroup) {
	defer wg.Done()
	// *net.TCPConn
	// ms.tranConns[idx]
	select {}
}

//
func (ms *MixerServer) tranPeerConnector() {
	defer ms.wg.Done()

	var wg sync.WaitGroup

	for idx, _ := range ms.cfg.TranPeerList {
		tpf("handle %s for transport ...\n", idx)
		wg.Add(1)
		go ms.tranPeerHandler(idx, &wg)
	}
	wg.Wait()
}

// Start run server in background
// caller should call Wait() to wait for server exit
func (ms *MixerServer) Start() error {
	ms.m.Lock()
	if ms.running {
		ms.m.Unlock()
		return errors.New("server already running")
	}
	ms.running = true
	ms.m.Unlock()
	//
	tpf("using cpus: %d\n", runtime.GOMAXPROCS(-1))
	//
	// setup filter
	//
	tpf("TFilter: %v\n", ms.cfg.TFilter)
	if ms.cfg.TFilter == nil {
		ms.cfg.TFilter = NewHttpEchoTransfer()
	}
	tpf("TFilter: %v\n", ms.cfg.TFilter)

	//
	ms.wg.Add(1)
	go func() {
		defer ms.wg.Done()
		in, out, err := ms.cfg.TFilter.Forward()
		tpf("TFilter.Forward exit: %d, %d, %v\n", in, out, err)
	}()

	//
	go ms.closer()
	//
	// initial client listener
	//
	for idx, laddr := range ms.cfg.TCPListens {
		nl, err := net.Listen("tcp", laddr)
		tpf("listening %s for client ... %v\n", idx, err)
		if err != nil {
			return err
		}
		// func (l *TCPListener) AcceptTCP() (*TCPConn, error)
		ms.dispListeners[idx] = nl.(*net.TCPListener)
	}
	ms.wg.Add(1)
	go ms.dispListener()
	//
	// initial peer listener
	//
	for idx, laddr := range ms.cfg.TranListens {
		nl, err := net.Listen("tcp", laddr)
		tpf("listening %s for peer ... %v\n", idx, err)
		if err != nil {
			return err
		}
		// func (l *TCPListener) AcceptTCP() (*TCPConn, error)
		ms.tranListeners[idx] = nl.(*net.TCPListener)
	}
	ms.wg.Add(1)
	go ms.tranListener()
	//
	// initial peer connect
	//
	for idx, raddr := range ms.cfg.TranPeerList {
		ms.tranConns[idx] = make(map[int]*net.TCPConn)
		for i := 0; i < ms.cfg.TranNum; i++ {
			nl, err := net.Dial("tcp", raddr)
			tpf("#%d, connecting to peer %s ... %v\n", i, idx, err)
			if err != nil {
				return err
			}
			ms.tranConns[idx][i] = nl.(*net.TCPConn)
		}
	}
	ms.wg.Add(1)
	go ms.tranPeerConnector()
	//
	return nil
}

//
func (ms *MixerServer) Close() {
	ms.m.Lock()
	defer ms.m.Unlock()
	select {
	case <-ms.destroyed:
		return
	default:
	}
	select {
	case <-ms.closing:
	default:
		close(ms.closing)
	}
	// waiting for all goroutine exited
	<-ms.closed
	// release resources

	//
	close(ms.destroyed)
	// send msg out
	go func(err error) {
		for {
			ms.waitCh <- ms.err
		}
	}(ms.err)
}

//
//
//
//
//
//
//
//
//
//
