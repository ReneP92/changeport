package hostgateway

import (
	"context"
	"log/slog"
	"net"
	"strings"

	"golang.org/x/net/dns/dnsmessage"
)

// DNS answers queries for names under Domain with Address, the host's
// loopback address, where the gateway's port is published. It answers only
// A queries with an address; other types under Domain get no records, and
// names outside it are refused.
type DNS struct {
	Domain  string
	Address [4]byte
	Logger  *slog.Logger
}

// Serve answers the queries conn receives until ctx is done.
func (d *DNS) Serve(ctx context.Context, conn net.PacketConn) error {
	go func() {
		<-ctx.Done()
		// Closing ends the read below; there is nothing to do on an error.
		_ = conn.Close()
	}()
	buf := make([]byte, 512)
	for {
		n, from, err := conn.ReadFrom(buf)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		answer, err := d.Answer(buf[:n])
		if err != nil {
			d.Logger.Debug("ignored a malformed DNS query", "from", from, "error", err)
			continue
		}
		if _, err := conn.WriteTo(answer, from); err != nil {
			d.Logger.Warn("could not answer a DNS query", "to", from, "error", err)
		}
	}
}

// Answer returns the response to a DNS query.
func (d *DNS) Answer(query []byte) ([]byte, error) {
	var parser dnsmessage.Parser
	header, err := parser.Start(query)
	if err != nil {
		return nil, err
	}
	question, err := parser.Question()
	if err != nil {
		return nil, err
	}
	name := strings.ToLower(strings.TrimSuffix(question.Name.String(), "."))
	inDomain := name == d.Domain || strings.HasSuffix(name, "."+d.Domain)
	response := dnsmessage.Header{ID: header.ID, Response: true, Authoritative: inDomain, RecursionDesired: header.RecursionDesired}
	if !inDomain {
		response.RCode = dnsmessage.RCodeRefused
	}
	builder := dnsmessage.NewBuilder(nil, response)
	builder.EnableCompression()
	if err := builder.StartQuestions(); err != nil {
		return nil, err
	}
	if err := builder.Question(question); err != nil {
		return nil, err
	}
	if inDomain && question.Type == dnsmessage.TypeA && question.Class == dnsmessage.ClassINET {
		if err := builder.StartAnswers(); err != nil {
			return nil, err
		}
		header := dnsmessage.ResourceHeader{Name: question.Name, Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET, TTL: 60}
		if err := builder.AResource(header, dnsmessage.AResource{A: d.Address}); err != nil {
			return nil, err
		}
	}
	return builder.Finish()
}
