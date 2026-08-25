package push

import (
	"crypto/tls"
	"errors"
	"os"

	pkcs12 "software.sslmate.com/src/go-pkcs12"
)

// loadP12Certificate decodes both modern SHA-256/AES PKCS#12 files and legacy
// SHA-1/3DES files. The APNs TLS client expects the leaf certificate first,
// followed by any intermediate certificates included in the P12 bundle.
func loadP12Certificate(filename, password string) (tls.Certificate, error) {
	p12Data, err := os.ReadFile(filename)
	if err != nil {
		return tls.Certificate{}, err
	}

	privateKey, leaf, caCerts, err := pkcs12.DecodeChain(p12Data, password)
	if err != nil {
		return tls.Certificate{}, err
	}
	if privateKey == nil {
		return tls.Certificate{}, errors.New("pkcs12 bundle contains no private key")
	}
	if leaf == nil {
		return tls.Certificate{}, errors.New("pkcs12 bundle contains no leaf certificate")
	}

	certificateChain := make([][]byte, 0, 1+len(caCerts))
	certificateChain = append(certificateChain, leaf.Raw)
	for _, cert := range caCerts {
		if cert != nil {
			certificateChain = append(certificateChain, cert.Raw)
		}
	}

	return tls.Certificate{
		Certificate: certificateChain,
		PrivateKey:  privateKey,
		Leaf:        leaf,
	}, nil
}
