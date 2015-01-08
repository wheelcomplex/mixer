// mixer is a tcp server framework for Common Multiplexing Transport Protocol (CMTP)

package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"runtime"

	"github.com/wheelcomplex/mixer"
	"github.com/wheelcomplex/preinit/misc"
)

func main() {

	svgport := flag.String("svgport", ":9980", "svg http port")
	profileport := flag.Int("port", 6060, "profile http port")
	runlimit := flag.Int("time", 60, "run time(seconds) limit for each hash")
	size := flag.Int("size", 256, "block size")
	//	countsize := flag.Int("counter", 8, "counter size")
	groupsize := flag.Int("groupsize", 2048, "group size")
	cpus := flag.Int("cpu", 0, "cpus")
	//	lock := flag.Bool("lock", true, "lock os thread")
	//	stat := flag.Bool("stat", true, "show interval stat")
	flag.Parse()
	fmt.Printf(" go tool pprof http://localhost:%d/debug/pprof/profile\n", *profileport)
	fmt.Printf(" svg output http://localhost%s/\n", *svgport)
	go func() {
		fmt.Println(http.ListenAndServe(fmt.Sprintf("localhost:%d", *profileport), nil))
	}()

	if *cpus <= 0 {
		*cpus = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(*cpus)
	if *size < 1 {
		*size = 1
	}

	if *groupsize < 16 {
		*groupsize = 16
	}

	if *runlimit < 1 {
		*runlimit = 1
	}

	var peerList map[string]string = map[string]string{"localnginx": "127.0.0.1:80"}
	ms, err := mixer.NewFullServer("abccd", peerList)
	if err != nil {
		misc.Tpf("NewFullServer: %s\n", err)
		return
	}
	if err := ms.Start(); err != nil {
		misc.Tpf("Start: %s\n", err)
		return
	}
	misc.Tpf("running ...\n")

	misc.Tpf("End: %s\n", <-ms.Wait())
	return
}
