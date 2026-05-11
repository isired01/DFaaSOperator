package ansible

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/base64"
	"fmt"

	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func calcolaPeerID(privKeyBase64 string) (string, error) {
	// 1. Decodifica la stringa Base64
	derBytes, err := base64.StdEncoding.DecodeString(privKeyBase64)
	if err != nil {
		return "", fmt.Errorf("errore decodifica base64: %v", err)
	}

	// 2. Parsing della chiave PKCS#8 (formato standard per le chiavi private Ed25519)
	rawKey, err := x509.ParsePKCS8PrivateKey(derBytes)
	if err != nil {
		return "", fmt.Errorf("errore parsing PKCS8: %v", err)
	}

	// 3. Cast alla chiave Ed25519 standard di Go
	edPriv, ok := rawKey.(ed25519.PrivateKey)
	if !ok {
		return "", fmt.Errorf("la chiave non è di tipo Ed25519")
	}

	// 4. Conversione nel formato crypto.PrivKey richiesto da libp2p
	// Libp2p vuole i byte della chiave privata seguiti da quelli della pubblica (64 byte totali)
	priv, err := crypto.UnmarshalEd25519PrivateKey(edPriv)
	if err != nil {
		return "", fmt.Errorf("errore unmarshal per libp2p: %v", err)
	}

	// 5. Generazione del PeerID
	id, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		return "", fmt.Errorf("errore generazione PeerID: %v", err)
	}

	return id.String(), nil
}
