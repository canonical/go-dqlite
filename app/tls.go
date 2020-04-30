package app

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"

	"github.com/canonical/go-dqlite/internal/protocol"
)

// SimpleTLSConfig returns a pair of TLS configuration objects with sane
// defaults, one to be used as server-side configuration when listening to
// incoming connections and one to be used as client-side configuration when
// establishing outgoing connections.
//
// The returned configs can be used as "listen" and "dial" parameters for the
// WithTLS option.
//
// In order to generate a suitable TLS certificate you can use the openssl
// command, for example:
//
//   DNS=$(hostname)
//   IP=$(hostname -I | cut -f 1 -d ' ')
//   CN=example.com
//   openssl req -x509 -newkey rsa:4096 -sha256 -days 3650 \
//     -nodes -keyout cluster.key -out cluster.crt -subj "/CN=$CN" \
//     -addext "subjectAltName=DNS:$DNS,IP:$IP"
//
// then load the resulting key pair and pool with:
//
//   cert, _ := tls.LoadX509KeyPair("cluster.crt", "cluster.key")
//   data, _ := ioutil.ReadFile("cluster.crt")
//   pool := x509.NewCertPool()
//   pool.AppendCertsFromPEM(data)
//
// and finally use the WithTLS option together with the SimpleTLSConfig helper:
//
//   app, _ := app.New("/my/dir", app.WithTLS(app.SimpleTLSConfig(cert, pool)))
//
// See SimpleListenTLSConfig and SimpleDialTLSConfig for details.

func SimpleTLSConfig(cert tls.Certificate, pool *x509.CertPool) (*tls.Config, *tls.Config) {
	listen := SimpleListenTLSConfig(cert, pool)
	dial := SimpleDialTLSConfig(cert, pool)
	return listen, dial
}

// SimpleListenTLSConfig returns a server-side TLS configuration with sane
// defaults (e.g. TLS version, ciphers and mutual authentication).
//
// The cert parameter must be a public/private key pair, typically loaded from
// disk using tls.LoadX509KeyPair().
//
// The pool parameter can be used to specify a custom signing CA (e.g. for
// self-signed certificates).
//
// When server and client both use the same certificate, the same key pair and
// pool should be passed to SimpleDialTLSConfig() in order to generate the
// client-side config.
//
// The returned config can be used as "listen" parameter for the WithTLS
// option.
func SimpleListenTLSConfig(cert tls.Certificate, pool *x509.CertPool) *tls.Config {
	// See https://github.com/denji/golang-tls
	config := &tls.Config{
		MinVersion:               tls.VersionTLS12,
		CipherSuites:             protocol.TLSCipherSuites,
		PreferServerCipherSuites: true,
		CurvePreferences:         []tls.CurveID{tls.CurveP521, tls.CurveP384, tls.CurveP256},
		Certificates:             []tls.Certificate{cert},
		RootCAs:                  pool,
		ClientCAs:                pool,
		ClientAuth:               tls.RequireAndVerifyClientCert,
	}
	config.BuildNameToCertificate()

	return config
}

// SimpleDialTLSConfig returns a client-side TLS configuration with sane
// defaults (e.g. TLS version, ciphers and mutual authentication).
//
// The cert parameter must be a public/private key pair, typically loaded from
// disk using tls.LoadX509KeyPair().
//
// The pool parameter can be used to specify a custom signing CA (e.g. for
// self-signed certificates).
//
// When server and client both use the same certificate, the same key pair and
// pool should be passed to SimpleListenTLSConfig() in order to generate the
// server-side config.
//
// The returned config can be used as "client" parameter for the WithTLS App
// option, or as "config" parameter for the client.DialFuncWithTLS() helper.
func SimpleDialTLSConfig(cert tls.Certificate, pool *x509.CertPool) *tls.Config {
	config := &tls.Config{
		MinVersion:               tls.VersionTLS12,
		CipherSuites:             protocol.TLSCipherSuites,
		PreferServerCipherSuites: true,
		RootCAs:                  pool,
		Certificates:             []tls.Certificate{cert},
	}

	x509cert, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		panic(fmt.Errorf("parse certificate: %v", err))
	}
	if len(x509cert.DNSNames) == 0 {
		panic("certificate has no DNS extension")
	}
	config.ServerName = x509cert.DNSNames[0]

	return config
}
