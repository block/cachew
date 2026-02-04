package githubapp

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"os"
	"time"

	"github.com/alecthomas/errors"
	"github.com/golang-jwt/jwt/v5"
)

type JWTGenerator struct {
	appID      string
	privateKey *rsa.PrivateKey
	expiration time.Duration
}

func NewJWTGenerator(appID, privateKeyPath string, expiration time.Duration) (*JWTGenerator, error) {
	privateKey, err := loadPrivateKey(privateKeyPath)
	if err != nil {
		return nil, errors.Wrap(err, "load private key")
	}

	return &JWTGenerator{
		appID:      appID,
		privateKey: privateKey,
		expiration: expiration,
	}, nil
}

func (g *JWTGenerator) GenerateJWT() (string, error) {
	now := time.Now()

	claims := jwt.RegisteredClaims{
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(now.Add(g.expiration)),
		Issuer:    g.appID,
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	signedToken, err := token.SignedString(g.privateKey)
	if err != nil {
		return "", errors.Wrap(err, "sign JWT")
	}

	return signedToken, nil
}

func loadPrivateKey(path string) (*rsa.PrivateKey, error) {
	keyData, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.Wrapf(err, "read private key file: %s", path)
	}

	block, _ := pem.Decode(keyData)
	if block == nil {
		return nil, errors.Errorf("failed to decode PEM block from private key file: %s", path)
	}

	privateKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err == nil {
		return privateKey, nil
	}

	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, errors.Wrap(err, "parse private key (tried both PKCS1 and PKCS8)")
	}

	rsaKey, ok := key.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.Errorf("private key is not RSA (type: %T)", key)
	}

	return rsaKey, nil
}
