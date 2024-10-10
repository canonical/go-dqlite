package main

import (
	"encoding/gob"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"

	"github.com/canonical/go-dqlite/app"
	"golang.org/x/term"
)

func main() {
	dir := flag.String("dir", "", "local data directory")
	address := flag.String("address", "", "new address of this node")
	mode := flag.String("mode", "", "'prepare' or 'propagate'")
	flag.Parse()
	if *address == "" {
		log.Fatal("address is required")
	}
	if *dir == "" {
		log.Fatal("dir is required")
	}

	switch *mode {
	case "prepare":
		kern, err := app.PrepareRecovery(*dir, *address)
		if err != nil {
			log.Fatal(err)
		}
		var sink io.Writer
		if term.IsTerminal(1 /* STDOUT_FILENO */) {
			base := fmt.Sprintf("dqlite-recover-kernel-%s-%s", *dir, *address)
			f, err := ioutil.TempFile(".", base)
			if err != nil {
				log.Fatal(err)
			}
			sink = f
		} else {
			sink = os.Stdout
		}
		enc := gob.NewEncoder(sink)
		if err := enc.Encode(kern); err != nil {
			log.Fatal(err)
		}
	case "propagate":
		dec := gob.NewDecoder(os.Stdin)
		var kern app.RecoveryKernel
		if err := dec.Decode(&kern); err != nil {
			log.Fatal(err)
		}
		if err := kern.Propagate(*dir, *address); err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatal("mode must be 'prepare' or 'propagate'")
	}
}
