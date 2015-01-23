// framework, protocol filter and transfer

package mixer

// TransConfig for cmtp transfer
type TransConfig struct {
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
}
