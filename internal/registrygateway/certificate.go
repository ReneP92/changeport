package registrygateway

import (
	"crypto/tls"
	"fmt"
	"os"
	"sync"
	"time"
)

// Certificate serves a TLS certificate from files, such as a Secret that
// cert-manager renews, and loads it again once the files change.
type Certificate struct {
	CertFile, KeyFile string

	mu       sync.Mutex
	loaded   *tls.Certificate
	modified time.Time
}

// Get implements tls.Config's GetCertificate.
func (c *Certificate) Get(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	modified, err := c.lastModified()
	if err != nil {
		return nil, err
	}
	if c.loaded == nil || !modified.Equal(c.modified) {
		certificate, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			if c.loaded != nil {
				// A Secret's files change one after another; serve the
				// last pair until both are new.
				return c.loaded, nil
			}
			return nil, fmt.Errorf("load the certificate: %w", err)
		}
		c.loaded, c.modified = &certificate, modified
	}
	return c.loaded, nil
}

func (c *Certificate) lastModified() (time.Time, error) {
	var latest time.Time
	for _, file := range []string{c.CertFile, c.KeyFile} {
		info, err := os.Stat(file)
		if err != nil {
			return time.Time{}, fmt.Errorf("the certificate: %w", err)
		}
		if info.ModTime().After(latest) {
			latest = info.ModTime()
		}
	}
	return latest, nil
}
