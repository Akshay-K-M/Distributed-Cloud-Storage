package checksum

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"strings"
)

// Hex encodes a digest as lowercase hex.
func Hex(sum []byte) string {
	return hex.EncodeToString(sum)
}

// SHA256Hex hashes b and returns lowercase hex.
func SHA256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// EqualHex compares two hex strings case-insensitively.
func EqualHex(a, b string) bool {
	return strings.EqualFold(strings.TrimSpace(a), strings.TrimSpace(b))
}

// Reader hashes r and returns lowercase hex and bytes read.
func Reader(r io.Reader) (string, int64, error) {
	h := sha256.New()
	n, err := io.Copy(h, r)
	if err != nil {
		return "", n, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}
