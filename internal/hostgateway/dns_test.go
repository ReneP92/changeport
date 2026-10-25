package hostgateway

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"
)

// query builds a DNS query for name and type.
func query(t *testing.T, name string, qtype dnsmessage.Type) []byte {
	t.Helper()
	builder := dnsmessage.NewBuilder(nil, dnsmessage.Header{ID: 42, RecursionDesired: true})
	if err := builder.StartQuestions(); err != nil {
		t.Fatal(err)
	}
	if err := builder.Question(dnsmessage.Question{Name: dnsmessage.MustNewName(name), Type: qtype, Class: dnsmessage.ClassINET}); err != nil {
		t.Fatal(err)
	}
	message, err := builder.Finish()
	if err != nil {
		t.Fatal(err)
	}
	return message
}

func TestDNSAnswersTheKafkaDomainWithTheHost(t *testing.T) {
	d := &DNS{Domain: domain, Address: [4]byte{127, 0, 0, 1}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	tests := []struct {
		name    string
		qtype   dnsmessage.Type
		rcode   dnsmessage.RCode
		answers []string
	}{
		{"bootstrap.acme.kafka.changeport.test.", dnsmessage.TypeA, dnsmessage.RCodeSuccess, []string{"127.0.0.1"}},
		{"B3.Acme.Kafka.Changeport.Test.", dnsmessage.TypeA, dnsmessage.RCodeSuccess, []string{"127.0.0.1"}},
		// No IPv6 address, so that clients do not wait for one.
		{"bootstrap.acme.kafka.changeport.test.", dnsmessage.TypeAAAA, dnsmessage.RCodeSuccess, nil},
		{"example.com.", dnsmessage.TypeA, dnsmessage.RCodeRefused, nil},
		{"changeport.test.", dnsmessage.TypeA, dnsmessage.RCodeRefused, nil},
	}
	for _, tt := range tests {
		response, err := d.Answer(query(t, tt.name, tt.qtype))
		if err != nil {
			t.Fatalf("Answer(%s %v) = %v", tt.name, tt.qtype, err)
		}
		var message dnsmessage.Message
		if err := message.Unpack(response); err != nil {
			t.Fatal(err)
		}
		var answers []string
		for _, answer := range message.Answers {
			if a, ok := answer.Body.(*dnsmessage.AResource); ok {
				answers = append(answers, net.IP(a.A[:]).String())
			}
		}
		if message.ID != 42 || !message.Response || message.RCode != tt.rcode || len(answers) != len(tt.answers) ||
			(len(answers) == 1 && answers[0] != tt.answers[0]) {
			t.Errorf("%s %v: id %d, rcode %v, answers %v; want rcode %v, answers %v",
				tt.name, tt.qtype, message.ID, message.RCode, answers, tt.rcode, tt.answers)
		}
	}
	if _, err := d.Answer([]byte{1, 2, 3}); err == nil {
		t.Error("Answer() of a malformed query succeeded")
	}
}

func TestDNSServesOverUDP(t *testing.T) {
	d := &DNS{Domain: domain, Address: [4]byte{127, 0, 0, 1}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Serve(ctx, conn) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Serve() = %v", err)
		}
	}()

	// The system resolver's way: a UDP query to the server.
	resolver := &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "udp", conn.LocalAddr().String())
	}}
	lookup, cancelLookup := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelLookup()
	addresses, err := resolver.LookupHost(lookup, "b0.acme.kafka.changeport.test")
	if err != nil || len(addresses) != 1 || addresses[0] != "127.0.0.1" {
		t.Errorf("LookupHost() = %v, %v; want 127.0.0.1", addresses, err)
	}
}
