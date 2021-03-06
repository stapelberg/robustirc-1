// robustirc-localnet starts 3 RobustIRC servers on localhost on random ports
// with temporary data directories, generating a self-signed SSL certificate.
// stdout and stderr are redirected to a file in the temporary data directory
// of each node.
//
// robustirc-localnet can be used for playing around with RobustIRC, especially
// when developing.
package main

import (
	"flag"
	"log"
	"time"

	"github.com/robustirc/robustirc/localnet"
)

var (
	localnetDir = flag.String("localnet_dir",
		"~/.config/robustirc-localnet",
		"Directory in which to keep state for robustirc-localnet (SSL certificates, PID files, etc.)")

	stop = flag.Bool("stop",
		false,
		"Whether to stop the currently running localnet instead of starting a new one")

	delete_tempdirs = flag.Bool("delete_tempdirs",
		true,
		"If false, temporary directories are left behind for manual inspection")

	port = flag.Int("port",
		-1,
		"Port to (try to) use for the first RobustIRC server. If in use, another will be tried.")
)

func main() {
	flag.Parse()

	l, err := localnet.NewLocalnet(*port, *localnetDir)
	if err != nil {
		log.Fatalf("%v", err)
	}

	if *stop {
		l.Kill(*delete_tempdirs)
		return
	}

	if l.Running() {
		log.Fatalf("There already is a localnet instance running. Either use -stop or specify a different -localnet_dir")
	}

	success := false

	defer func() {
		if success {
			return
		}
		log.Printf("Could not successfully set up localnet, cleaning up.\n")
		l.Kill(*delete_tempdirs)
	}()

	l.StartIRCServer(true)
	l.StartIRCServer(false)
	l.StartIRCServer(false)
	l.StartBridge()

	try := 0
	for try < 10 {
		try++
		if l.Healthy() {
			success = true
			break
		} else {
			time.Sleep(1 * time.Second)
		}
	}
}
