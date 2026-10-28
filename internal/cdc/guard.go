package cdc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
)

// nonPublic are the address ranges that are not on the public internet, or
// that embed an address that may not be, beyond those netip.Addr reports on
// itself (loopback, private, link-local, multicast, unspecified).
var nonPublic = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),       // this network
	netip.MustParsePrefix("100.64.0.0/10"),   // shared address space (carrier-grade NAT)
	netip.MustParsePrefix("192.0.0.0/24"),    // IETF protocol assignments
	netip.MustParsePrefix("192.0.2.0/24"),    // documentation
	netip.MustParsePrefix("198.18.0.0/15"),   // benchmarking
	netip.MustParsePrefix("198.51.100.0/24"), // documentation
	netip.MustParsePrefix("203.0.113.0/24"),  // documentation
	netip.MustParsePrefix("240.0.0.0/4"),     // reserved, and the broadcast address
	netip.MustParsePrefix("64:ff9b::/96"),    // NAT64, which embeds an IPv4 address
	netip.MustParsePrefix("64:ff9b:1::/48"),  // local NAT64
	netip.MustParsePrefix("100::/64"),        // discard
	netip.MustParsePrefix("2001::/23"),       // IETF protocol assignments, Teredo among them
	netip.MustParsePrefix("2001:db8::/32"),   // documentation
	netip.MustParsePrefix("2002::/16"),       // 6to4, which embeds an IPv4 address
}

// Public reports whether an address is on the public internet: not this
// network, a private network, a cloud's metadata service, or a range kept
// for special uses.
func Public(addr netip.Addr) bool {
	addr = addr.Unmap()
	if !addr.IsValid() || addr.IsUnspecified() || addr.IsLoopback() || addr.IsPrivate() ||
		addr.IsLinkLocalUnicast() || addr.IsLinkLocalMulticast() || addr.IsInterfaceLocalMulticast() || addr.IsMulticast() {
		return false
	}
	for _, prefix := range nonPublic {
		if prefix.Contains(addr) {
			return false
		}
	}
	return true
}

// ErrNotPublic means a host resolves to an address that is not public.
var ErrNotPublic = errors.New("not a public address")

// NotPublicError is a host the guard refused, and the address that made it.
type NotPublicError struct {
	Host string
	Addr netip.Addr
}

func (e *NotPublicError) Error() string {
	if e.Host == e.Addr.String() {
		return e.Host + " is not a public address"
	}
	return fmt.Sprintf("%s resolves to %s, not a public address", e.Host, e.Addr)
}

// Is makes a NotPublicError an ErrNotPublic.
func (e *NotPublicError) Is(target error) bool { return target == ErrNotPublic }

// Guard connects only to public addresses, so that the checks can never be
// pointed at Changeport's own network: it resolves a host once, refuses it
// if any of its addresses is not public, and dials only the addresses it
// resolved, so that the name cannot resolve elsewhere in between. The TLS
// server name stays the host's.
type Guard struct {
	// AllowPrivate lets the checks reach private addresses, for local
	// development and tests, where the database is on a private network.
	AllowPrivate bool
	// Lookup resolves a host name to its addresses; nil uses the default
	// resolver.
	Lookup func(ctx context.Context, host string) ([]netip.Addr, error)
	// Dialer dials the resolved addresses; nil uses a zero net.Dialer.
	Dialer *net.Dialer
}

// lookup resolves host to its addresses, refusing it if any is not public.
// One address that is not public refuses the host, so that a name cannot
// mix a public address with an internal one.
func (g Guard) lookup(ctx context.Context, host string) ([]string, error) {
	var addrs []netip.Addr
	if addr, err := netip.ParseAddr(host); err == nil {
		addrs = []netip.Addr{addr}
	} else {
		lookup := g.Lookup
		if lookup == nil {
			lookup = func(ctx context.Context, host string) ([]netip.Addr, error) {
				return net.DefaultResolver.LookupNetIP(ctx, "ip", host)
			}
		}
		addrs, err = lookup(ctx, host)
		if err != nil {
			return nil, err
		}
	}
	result := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		if !g.AllowPrivate && !Public(addr) {
			if addr.String() == host {
				return nil, &NotPublicError{Host: host, Addr: addr}
			}
			return nil, &NotPublicError{Host: host, Addr: addr.Unmap()}
		}
		result = append(result, addr.Unmap().String())
	}
	return result, nil
}

// dial connects to an address lookup returned, checking it again.
func (g Guard) dial(ctx context.Context, network, address string) (net.Conn, error) {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return nil, fmt.Errorf("refusing to dial %s, which is not an address", address)
	}
	if !g.AllowPrivate && !Public(addr) {
		return nil, &NotPublicError{Host: host, Addr: addr}
	}
	dialer := g.Dialer
	if dialer == nil {
		dialer = &net.Dialer{}
	}
	return dialer.DialContext(ctx, network, address)
}
