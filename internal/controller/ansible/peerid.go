package ansible

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/base64"
	"fmt"

	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func derivePeerID(privKeyBase64 string) (string, error) {
	// 1. Decode the base64 string.
	derBytes, err := base64.StdEncoding.DecodeString(privKeyBase64)
	if err != nil {
		return "", fmt.Errorf("base64 decode: %v", err)
	}

	// 2. Parse the PKCS#8 key (standard format for Ed25519 private keys).
	rawKey, err := x509.ParsePKCS8PrivateKey(derBytes)
	if err != nil {
		return "", fmt.Errorf("PKCS8 parse: %v", err)
	}

	// 3. Cast to Go's standard Ed25519 key.
	edPriv, ok := rawKey.(ed25519.PrivateKey)
	if !ok {
		return "", fmt.Errorf("key is not of type Ed25519")
	}

	// 4. Convert to the crypto.PrivKey format required by libp2p.
	// libp2p expects the private key bytes followed by the public ones (64 bytes total).
	priv, err := crypto.UnmarshalEd25519PrivateKey(edPriv)
	if err != nil {
		return "", fmt.Errorf("libp2p unmarshal: %v", err)
	}

	// 5. Generate the PeerID.
	id, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		return "", fmt.Errorf("PeerID generation: %v", err)
	}

	return id.String(), nil
}
