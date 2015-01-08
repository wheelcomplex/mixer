// mixer is a tcp server framework for Common Multiplexing Transport Protocol (CMTP)

package mixer

import (
	"errors"
	"net"
	"runtime"
	"sync"

	"github.com/wheelcomplex/preinit/misc"
)

// tpf is short alias of misc.Tpf
var tpf = misc.Tpf

// Config for Mixer
type Config struct {

	// key for remote identify and stream crypt
	//
	Key string

	// dispatcher listeners
	// listen on host:port for client connection
	// empty list to disable client listening
	// default: map[string]string{"0.0.0.0:6099":"0.0.0.0:6099"}
	DispListens map[string]string

	// dispatcher filter use to handle data assemble/disassemble
	// default: NewTCPDispFilter("--CMTP--", "0.0.0.0:6099")
	DFilters *DispChain

	// transport listeners
	// listen on host:port for transport connection
	// peer listening only accept the remote with same token/key
	// empty list to disable peer listening
	// TranListens and TranPeerList should not empty at the same time
	// default: map[string]string{"0.0.0.0:9099":"0.0.0.0:9099"}
	TranListens map[string]string

	// remote forwarder list, format: host:port, index by name
	// on startup, connect to remote with token
	// TranListens and TranPeerList should not empty at the same time
	// default: {} (empty)
	TranPeerList map[string]string

	// number of connections create to each trnasport peer
	// default is 5
	TranNum int

	// transport filter use to handle data frame encode/decode
	// default: NewTCPDispFilter("--CMTP--", "0.0.0.0:6099")
	TFilter TranFilter

	// auth token for remote identify
	// is hash of Key
	token uint64
}

//
func NewConfig(key string) *Config {
	return &Config{
		Key:          key,
		DispListens:  make(map[string]string),
		DFilters:     NewDispChain(),
		TranListens:  make(map[string]string),
		TranPeerList: make(map[string]string),
		TranNum:      5,
		TFilter:      nil,
	}
}

// passiveConfig is default config for MixerServer
var passiveConfig = Config{
	Key:          "",
	DispListens:  map[string]string{"0.0.0.0:6099": "0.0.0.0:6099"},
	TranListens:  map[string]string{"0.0.0.0:9099": "0.0.0.0:9099"},
	TranPeerList: map[string]string{},
	DFilters:     NewDispChain(),
	TFilter:      nil,
	TranNum:      5,
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
	if len(cfg.DispListens) == 0 && len(template.DispListens) > 0 {
		for idx, _ := range template.DispListens {
			cfg.DispListens[idx] = template.DispListens[idx]
		}
	}
	if len(cfg.TranListens) == 0 && len(template.TranListens) > 0 {
		for idx, _ := range template.TranListens {
			cfg.TranListens[idx] = template.TranListens[idx]
		}
	}
	if len(cfg.TranPeerList) == 0 && len(template.TranPeerList) > 0 {
		for idx, _ := range template.TranPeerList {
			cfg.TranPeerList[idx] = template.TranPeerList[idx]
		}
	}
	if cfg.DFilters.Len() == 0 && template.DFilters.Len() > 0 {
		cfg.DFilters = template.DFilters.Clone()
	}
	if cfg.TFilter == nil {
		cfg.TFilter = template.TFilter
	}
	if cfg.TranNum <= 0 {
		cfg.TranNum = template.TranNum
	}
}

//
type MixerServer struct {
	cfg           *Config                         // config data for server
	tranConns     map[string]map[int]*net.TCPConn //
	tranListeners map[string]*net.TCPListener     //
	dispConns     map[string]map[int]*net.TCPConn //
	dispListeners map[string]*net.TCPListener     //
	closing       chan struct{}                   //
	closed        chan struct{}                   //
	m             sync.Mutex                      //
	err           error                           //
	waitCh        chan error                      //
	destroyed     chan struct{}                   //
	wg            sync.WaitGroup                  //
	running       bool                            //
	dispPool      *sync.Pool                      //
}

// NewServer initial MixerServer using cfg config
func NewServer(cfg *Config) (*MixerServer, error) {
	// default setup
	cfg.Merge(&passiveConfig)
	ms := &MixerServer{
		cfg:           cfg,
		tranConns:     make(map[string]map[int]*net.TCPConn),
		tranListeners: make(map[string]*net.TCPListener),
		dispConns:     make(map[string]map[int]*net.TCPConn),
		dispListeners: make(map[string]*net.TCPListener),
		closing:       make(chan struct{}, 128),
		closed:        make(chan struct{}, 128),
		destroyed:     make(chan struct{}, 128),
		waitCh:        make(chan error, 128),
	}
	if err := ms.ConfigCheck(ms.cfg); err != nil {
		return nil, err
	}
	//
	return ms, nil
}

// NewActiveServer return MixerServer using active config
// mixer will connect to another mixer
func NewActiveServer(key string, peerList map[string]string) (*MixerServer, error) {
	cfg := NewConfig(key)
	cfg.Merge(&passiveConfig)
	// disable client listener
	cfg.TranListens = map[string]string{}
	// disable peer listener
	cfg.TranListens = map[string]string{}
	//
	cfg.TranPeerList = peerList
	return NewServer(cfg)
}

// NewPassiveServer return MixerServer using passive config
// mixer will waitting connect from another mixer
func NewPassiveServer(key string) (*MixerServer, error) {
	cfg := NewConfig(key)
	return NewServer(cfg)
}

// NewFullServer return MixerServer using passive config + peerList
// mixer will connect to another mixer, and wait for connect from another mixer/local client
func NewFullServer(key string, peerList map[string]string) (*MixerServer, error) {
	cfg := NewConfig(key)
	cfg.Merge(&passiveConfig)
	//
	cfg.TranPeerList = peerList
	return NewServer(cfg)
}

// ConfigCheck check config and return error
func (ms *MixerServer) ConfigCheck(cfg *Config) error {
	if cfg == nil {
		return errors.New("invalid config: nil config")
	}
	if len(cfg.TranPeerList) == 0 && len(cfg.TranListens) == 0 {
		return errors.New("invalid config: both TranPeerList and TranListens empty")
	}
	return nil
}

// AddDispFilter add DispFilter(dispacher) to server
// must call befor server start
func (ms *MixerServer) AddDispFilter(name string, tf DispFilter) error {
	ms.m.Lock()
	defer ms.m.Unlock()
	if ms.running {
		return errors.New("can't add filter when server running")
	}
	ms.cfg.DFilters.AddDispFilter(name, tf)
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

// newDispSession used for sync.pool to create session struct
func (ms *MixerServer) newDispSession() func() interface{} {
	return func() interface{} {
		return ms.cfg.DFilters.Clone()
	}
}

//
func (ms *MixerServer) dispAcceptClient(idx string, wg *sync.WaitGroup) {
	defer ms.dispListeners[idx].Close()
	defer wg.Done()

	// *net.TCPListener
	// ms.dispListeners[idx]
	// ms.cfg.TFilter
	tpf("dispAcceptClient from  %s ...\n", idx)
	for {
		select {
		case <-ms.closing:
			return
		default:
		}
		// TODO: close listener when closing
		newrw, newerr := ms.dispListeners[idx].AcceptTCP()
		if newerr != nil {
			tpf("dispAcceptClient %d exit for error: %v\n", idx, newerr)
			return
		}
		//
		//tpf("dispAcceptClient %s, AcceptTCP: %v: %v\n", idx, newrw, newerr)
		//
		dispchain := ms.dispPool.Get().(*DispChain)
		if dispchain.Len() == 0 {
			dispchain.AddDispFilter("noopfilter", NewNoopTCPFilter(newrw))
		} else {
			dispchain.Reset()
			dispchain.BindUpStream(NewNoopTCPFilter(newrw))
		}
		//tpf("dispAcceptClient %s, AcceptTCP: %v: %v\n", idx, newrw, newerr)
		ms.cfg.TFilter.AddUpFilter(dispchain)
	}
}

//
func (ms *MixerServer) dispListener() {
	defer ms.wg.Done()

	var wg sync.WaitGroup

	for idx, _ := range ms.cfg.DispListens {
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
		ms.cfg.TFilter = NewHttpEchoTranFilter()
	}
	tpf("TFilter: %v\n", ms.cfg.TFilter)

	//
	ms.wg.Add(1)
	go func() {
		defer ms.wg.Done()
		in, out, err := ms.cfg.TFilter.Forward()
		tpf("TFilter.Forward exit: %d, %d, %v\n", in, out, err)
	}()
	ms.wg.Add(1)
	go func() {
		defer ms.wg.Done()
		for item := range ms.cfg.TFilter.RealsedDispChain() {
			ms.dispPool.Put(item)
		}
	}()

	//
	ms.dispPool = &sync.Pool{
		New: ms.newDispSession(),
	}

	//
	go ms.closer()
	//
	// initial client listener
	//
	for idx, laddr := range ms.cfg.DispListens {
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
