package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	_ "embed"
	"encoding/base64"
	"encoding/pem"
	"flag"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/quic-go/webtransport-go"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/quic-go/http3/qlog"
)

//go:embed index.html
var browserIndex string

func main() {
	addr := flag.String("addr", ":6121", "WebTransport listen address")
	browserAddr := flag.String("browser-addr", ":6120", "browser client listen address")
	protocolsFlag := flag.String("protocols", "webtransport-test,webtransport-test-2", "comma-separated application protocols")
	certOutPath := flag.String("cert-out", "cert.pem", "path to write the self-signed certificate as PEM (for non-browser clients that trust it as a root)")
	flag.Parse()

	var protocols []string
	for p := range strings.SplitSeq(*protocolsFlag, ",") {
		if p = strings.TrimSpace(p); p != "" {
			protocols = append(protocols, p)
		}
	}

	privateKeyBytes := []byte("webtransport-example-cert-key-01")
	certKey, err := ecdsa.ParseRawPrivateKey(elliptic.P256(), privateKeyBytes)
	if err != nil {
		log.Fatalf("failed to create certificate key: %v", err)
	}

	// The W3C serverCertificateHashes API requires the certificate to be valid now,
	// but for less than two weeks. A weekly window keeps the hash stable across restarts.
	now := time.Now().UTC()
	validFrom := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	validFrom = validFrom.AddDate(0, 0, -int((validFrom.Weekday()+6)%7))
	validUntil := validFrom.Add(13 * 24 * time.Hour)

	certTemplate := x509.Certificate{
		SerialNumber:          big.NewInt(validFrom.Unix()),
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             validFrom,
		NotAfter:              validUntil,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		// The cert is self-signed and used directly as a trust anchor by non-browser clients,
		// which requires it to be marked as a CA (RFC 5280 §4.2.1.9).
		IsCA:        true,
		DNSNames:    []string{"localhost"},
		IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1), net.ParseIP("::1")},
	}
	// Passing nil makes ECDSA signing deterministic according to RFC 6979.
	certDER, err := x509.CreateCertificate(nil, &certTemplate, &certTemplate, &certKey.PublicKey, certKey)
	if err != nil {
		log.Fatalf("failed to create certificate: %v", err)
	}

	hash := sha256.Sum256(certDER)
	certHash := base64.RawStdEncoding.EncodeToString(hash[:])
	fmt.Printf("Server Certificate Hash: %s\n", certHash)

	// Non-browser clients (e.g. the Swift client) validate the certificate chain rather than
	// pinning a hash, so write it out as PEM to be used as a trusted root (it is self-signed).
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	if err := os.WriteFile(*certOutPath, certPEM, 0o644); err != nil {
		log.Fatalf("failed to write certificate PEM: %v", err)
	}
	fmt.Printf("Server certificate written to: %s\n", *certOutPath)

	tlsConf := &tls.Config{
		Certificates: []tls.Certificate{{
			Certificate: [][]byte{certDER},
			PrivateKey:  certKey,
		}},
	}
	if err := runServer(tlsConf, certHash, *addr, *browserAddr, protocols); err != nil {
		log.Fatalf("failed to run server: %v", err)
	}
}

func runServer(tlsConf *tls.Config, certHash, addr, browserAddr string, protocols []string) error {
	webTransportAddr := addr
	if host, port, err := net.SplitHostPort(addr); err == nil && host == "" {
		webTransportAddr = net.JoinHostPort("localhost", port)
	}
	webTransportURL := "https://" + webTransportAddr + "/webtransport"

	browserURL := browserAddr
	if host, port, err := net.SplitHostPort(browserAddr); err == nil && host == "" {
		browserURL = net.JoinHostPort("localhost", port)
	}

	browserHTML := strings.ReplaceAll(browserIndex, "{{SERVER_CERTIFICATE_HASH}}", certHash)
	browserHTML = strings.ReplaceAll(browserHTML, "{{WEBTRANSPORT_URL}}", webTransportURL)

	go func() {
		log.Printf("serving browser client on http://%s", browserURL)
		if err := http.ListenAndServe(browserAddr, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/" {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			fmt.Fprint(w, browserHTML)
		})); err != nil {
			log.Printf("serving browser client failed: %s", err)
		}
	}()

	h3Server := &http3.Server{
		Addr:      addr,
		TLSConfig: http3.ConfigureTLSConfig(tlsConf),
		QUICConfig: &quic.Config{
			Tracer:                           qlog.DefaultConnectionTracer,
			EnableDatagrams:                  true,
			EnableStreamResetPartialDelivery: true,
		},
	}
	webtransport.ConfigureHTTP3Server(h3Server)
	mux := http.NewServeMux()
	h3Server.Handler = mux

	s := webtransport.Server{
		ApplicationProtocols: protocols,
		H3:                   h3Server,
		CheckOrigin:          func(*http.Request) bool { return true },
	}

	// Create a new HTTP endpoint /webtransport.
	mux.HandleFunc("/webtransport", func(w http.ResponseWriter, r *http.Request) {
		sess, err := s.Upgrade(w, r)
		if err != nil {
			log.Printf("upgrading failed: %s", err)
			w.WriteHeader(500)
			return
		}
		fmt.Printf("negotiated protocol: %s\n", sess.SessionState().ApplicationProtocol)
		go echoSession(sess)
	})

	log.Printf("listening on %s", webTransportURL)
	return s.ListenAndServe()
}

func echoSession(sess *webtransport.Session) {
	ctx := sess.Context()

	go func() {
		for {
			stream, err := sess.AcceptStream(ctx)
			if err != nil {
				log.Printf("accepting stream failed: %v", err)
				return
			}
			log.Printf("accepted bidirectional stream %d", stream.StreamID())
			go func() {
				defer stream.Close()
				buffer := make([]byte, 32*1024)
				for {
					n, readErr := stream.Read(buffer)
					if n > 0 {
						log.Printf("received stream data: %q", buffer[:n])
						if string(buffer[:n]) == "open" {
							openServerStream(ctx, sess)
						} else if _, writeErr := stream.Write(buffer[:n]); writeErr != nil {
							log.Printf("writing stream echo failed: %v", writeErr)
							return
						}
					}
					if readErr != nil {
						log.Printf("reading stream failed: %v", readErr)
						return
					}
				}
			}()
		}
	}()

	go func() {
		for {
			stream, err := sess.AcceptUniStream(ctx)
			if err != nil {
				log.Printf("accepting unidirectional stream failed: %v", err)
				return
			}
			log.Printf("accepted unidirectional stream %d", stream.StreamID())
			go func() {
				buffer := make([]byte, 32*1024)
				for {
					n, readErr := stream.Read(buffer)
					if n > 0 {
						log.Printf("received unidirectional stream data: %q", buffer[:n])
						if string(buffer[:n]) == "open" {
							openServerUniStream(ctx, sess)
						}
					}
					if readErr != nil {
						log.Printf("reading unidirectional stream failed: %v", readErr)
						return
					}
				}
			}()
		}
	}()

	for {
		data, err := sess.ReceiveDatagram(ctx)
		if err != nil {
			return
		}
		fmt.Printf("received datagram: %q\n", data)
		if err := sess.SendDatagram(data); err != nil {
			return
		}
	}
}

// openServerStream opens a new server-initiated bidirectional stream in response to an "open" request.
func openServerStream(ctx context.Context, sess *webtransport.Session) {
	stream, err := sess.OpenStreamSync(ctx)
	if err != nil {
		log.Printf("opening bidirectional stream failed: %v", err)
		return
	}
	log.Printf("opened bidirectional stream %d", stream.StreamID())
	defer stream.Close()
	if _, err := stream.Write([]byte("opened")); err != nil {
		log.Printf("writing to opened bidirectional stream failed: %v", err)
	}
}

// openServerUniStream opens a new server-initiated unidirectional stream in response to an "open" request.
func openServerUniStream(ctx context.Context, sess *webtransport.Session) {
	stream, err := sess.OpenUniStreamSync(ctx)
	if err != nil {
		log.Printf("opening unidirectional stream failed: %v", err)
		return
	}
	log.Printf("opened unidirectional stream %d", stream.StreamID())
	defer stream.Close()
	if _, err := stream.Write([]byte("opened")); err != nil {
		log.Printf("writing to opened unidirectional stream failed: %v", err)
	}
}
