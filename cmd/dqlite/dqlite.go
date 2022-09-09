package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"

	"github.com/canonical/go-dqlite/app"
	"github.com/canonical/go-dqlite/client"
	"github.com/canonical/go-dqlite/internal/shell"
	"github.com/peterh/liner"
	"github.com/spf13/cobra"
)

func main() {
	var crt string
	var key string
	var servers *[]string
	var format string

	cmd := &cobra.Command{
		Use:   "dqlite -s <servers> <database> [command]",
		Short: "Standard dqlite shell",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(*servers) == 0 {
				return fmt.Errorf("no servers provided")
			}
			var store client.NodeStore
			var err error

			first := (*servers)[0]
			if strings.HasPrefix(first, "file://") {
				if len(*servers) > 1 {
					return fmt.Errorf("can't mix server store and explicit list")
				}
				path := first[len("file://"):]
				if _, err := os.Stat(path); err != nil {
					return fmt.Errorf("open servers store: %w", err)
				}

				store, err = client.DefaultNodeStore(path)
				if err != nil {
					return fmt.Errorf("open servers store: %w", err)
				}
			} else {
				infos := make([]client.NodeInfo, len(*servers))
				for i, address := range *servers {
					infos[i].Address = address
				}
				store = client.NewInmemNodeStore()
				store.Set(context.Background(), infos)
			}

			if (crt != "" && key == "") || (key != "" && crt == "") {
				return fmt.Errorf("both TLS certificate and key must be given")
			}

			dial := client.DefaultDialFunc

			if crt != "" {
				cert, err := tls.LoadX509KeyPair(crt, key)
				if err != nil {
					return err
				}

				data, err := ioutil.ReadFile(crt)
				if err != nil {
					return err
				}

				pool := x509.NewCertPool()
				if !pool.AppendCertsFromPEM(data) {
					return fmt.Errorf("bad certificate")
				}

				config := app.SimpleDialTLSConfig(cert, pool)
				dial = client.DialFuncWithTLS(dial, config)

			}

			sh, err := shell.New(args[0], store, shell.WithDialFunc(dial), shell.WithFormat(format))
			if err != nil {
				return err
			}

			if len(args) > 1 {
				for _, input := range strings.Split(args[1], ";") {
					result, err := sh.Process(context.Background(), input)
					if err != nil {
						return err
					} else if result != "" {
						fmt.Println(result)
					}
				}
				return nil
			}

			line := liner.NewLiner()
			defer line.Close()

			for {
				input, err := line.Prompt("dqlite> ")
				if err != nil {
					if err == io.EOF {
						break
					}
					return err
				}

				result, err := sh.Process(context.Background(), input)
				if err != nil {
					fmt.Println("Error: ", err)
				} else {
					line.AppendHistory(input)
					if result != "" {
						fmt.Println(result)
					}
				}
			}

			return nil
		},
	}

	flags := cmd.Flags()
	servers = flags.StringSliceP("servers", "s", nil, "comma-separated list of db servers, or file://<store>")
	flags.StringVarP(&crt, "cert", "c", "", "public TLS cert")
	flags.StringVarP(&key, "key", "k", "", "private TLS key")
	flags.StringVarP(&format, "format", "f", "tabular", "output format (tabular, json)")

	cmd.MarkFlagRequired("servers")

	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
