package cdc

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"strings"
	"testing"
)

func TestPublic(t *testing.T) {
	for _, address := range []string{"8.8.8.8", "52.94.236.248", "2600:1f18::1", "2a00:1450:4001::1", "::ffff:8.8.8.8"} {
		if !Public(netip.MustParseAddr(address)) {
			t.Errorf("Public(%s) = false", address)
		}
	}
	for _, address := range []string{
		"0.0.0.0", "0.1.2.3", "10.0.0.1", "100.64.0.1", "127.0.0.1", "169.254.169.254", "172.16.0.1", "192.0.0.1",
		"192.0.2.1", "192.168.1.1", "198.18.0.1", "198.51.100.1", "203.0.113.1", "224.0.0.1", "240.0.0.1",
		"255.255.255.255", "::", "::1", "::ffff:10.0.0.1", "::ffff:169.254.169.254", "64:ff9b::a00:1", "64:ff9b:1::1",
		"100::1", "2001::1", "2001:db8::1", "2002:a00:1::1", "fc00::1", "fd00:ec2::254", "fe80::1", "ff02::1",
	} {
		if Public(netip.MustParseAddr(address)) {
			t.Errorf("Public(%s) = true", address)
		}
	}
}

// fakeLookup answers lookups from a fixed table.
func fakeLookup(answers map[string][]string) func(context.Context, string) ([]netip.Addr, error) {
	return func(_ context.Context, host string) ([]netip.Addr, error) {
		answer, ok := answers[host]
		if !ok {
			return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
		}
		addrs := make([]netip.Addr, len(answer))
		for i, address := range answer {
			addrs[i] = netip.MustParseAddr(address)
		}
		return addrs, nil
	}
}

func TestGuardRefusesHostsWithAnAddressThatIsNotPublic(t *testing.T) {
	guard := Guard{Lookup: fakeLookup(map[string][]string{
		"db.example.com":       {"8.8.8.8"},
		"internal.example.com": {"10.0.0.5"},
		"mixed.example.com":    {"8.8.8.8", "169.254.169.254"},
	})}
	addrs, err := guard.lookup(t.Context(), "db.example.com")
	if err != nil || len(addrs) != 1 || addrs[0] != "8.8.8.8" {
		t.Errorf("lookup(db.example.com) = %v, %v", addrs, err)
	}
	for host, want := range map[string]string{
		"internal.example.com": "internal.example.com resolves to 10.0.0.5, not a public address",
		"mixed.example.com":    "mixed.example.com resolves to 169.254.169.254, not a public address",
		"127.0.0.1":            "127.0.0.1 is not a public address",
		"::ffff:10.1.2.3":      "::ffff:10.1.2.3 is not a public address",
	} {
		_, err := guard.lookup(t.Context(), host)
		if !errors.Is(err, ErrNotPublic) || !strings.Contains(err.Error(), want) {
			t.Errorf("lookup(%s) = %v, want %q", host, err, want)
		}
	}
	if _, err := guard.dial(t.Context(), "tcp", "10.0.0.5:5432"); !errors.Is(err, ErrNotPublic) {
		t.Errorf("dial(10.0.0.5) = %v, want ErrNotPublic", err)
	}
	if _, err := guard.dial(t.Context(), "tcp", "db.example.com:5432"); err == nil {
		t.Error("dial accepted a name instead of an address")
	}

	private := Guard{AllowPrivate: true, Lookup: guard.Lookup}
	if addrs, err := private.lookup(t.Context(), "internal.example.com"); err != nil || addrs[0] != "10.0.0.5" {
		t.Errorf("lookup with private addresses allowed = %v, %v", addrs, err)
	}
}
