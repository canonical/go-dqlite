package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"io/ioutil"
	"os"

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

	cmd := &cobra.Command{
		Use:   "dqlite -s <servers> <database>",
		Short: "Standard dqlite shell",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			infos := make([]client.NodeInfo, len(*servers))
			for i, address := range *servers {
				infos[i].Address = address
			}

			store := client.NewInmemNodeStore()
			store.Set(context.Background(), infos)

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

			sh, err := shell.New(args[0], store, shell.WithDialFunc(dial))
			if err != nil {
				return err
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
				} else if result != "" {
					fmt.Println(result)
				}
			}

			return nil
		},
	}

	flags := cmd.Flags()
	servers = flags.StringSliceP("servers", "s", nil, "comma-separated list of db servers")
	flags.StringVarP(&crt, "cert", "c", "", "public TLS cert")
	flags.StringVarP(&key, "key", "k", "", "private TLS key")

	cmd.MarkFlagRequired("servers")

	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
