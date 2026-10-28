package cdc

import (
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

func validConnection() Connection {
	return Connection{Host: "db.example.com", Port: 5432, Database: "app", User: "changeport_app_x1y2z3", Password: "S3CRET", TLSMode: VerifyFull}
}

func TestConnectionValidate(t *testing.T) {
	if err := validConnection().Validate(); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*Connection){
		"no host":          func(c *Connection) { c.Host = "" },
		"a host with port": func(c *Connection) { c.Host = "db.example.com/5432" },
		"two hosts":        func(c *Connection) { c.Host = "a.example.com,b.example.com" },
		"a socket":         func(c *Connection) { c.Host = "/var/run/postgresql" },
		"no port":          func(c *Connection) { c.Port = 0 },
		"a port too high":  func(c *Connection) { c.Port = 65536 },
		"no database":      func(c *Connection) { c.Database = "" },
		"no user":          func(c *Connection) { c.User = "" },
		"no password":      func(c *Connection) { c.Password = "" },
		"no TLS mode":      func(c *Connection) { c.TLSMode = "require" },
		"verify-ca no CA":  func(c *Connection) { c.TLSMode = VerifyCA },
		"a CA not PEM":     func(c *Connection) { c.CA = "not a certificate" },
	} {
		c := validConnection()
		change(&c)
		if c.Validate() == nil {
			t.Errorf("Validate() accepted %s", name)
		}
	}
}

// listen serves each connection with serve on a port of this machine, and
// returns the port.
func listen(t *testing.T, serve func(net.Conn)) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = conn.Close() }()
				serve(conn)
			}()
		}
	}()
	return listener.Addr().(*net.TCPAddr).Port
}

func checkLocal(t *testing.T, port int) Report {
	t.Helper()
	connection := validConnection()
	connection.Host, connection.Port = "127.0.0.1", port
	return Checker{Guard: Guard{AllowPrivate: true}, ConnectTimeout: 3 * time.Second}.Check(t.Context(), Target{Provider: Generic, Connection: connection, Names: testNames(t)})
}

func TestCheckServerWithoutTLS(t *testing.T) {
	// The server answers Postgres's SSLRequest with N, for no TLS.
	port := listen(t, func(conn net.Conn) {
		request := make([]byte, 8)
		if _, err := io.ReadFull(conn, request); err == nil {
			_, _ = conn.Write([]byte("N"))
		}
	})
	report := checkLocal(t, port)
	if got := statuses(report); got[Reachable] != Pass || got[TLS] != Fail || got[Certificate] != Skipped || got[Version] != Skipped {
		t.Errorf("statuses = %v, want reachable, and TLS failed", got)
	}
	if got := message(t, report, TLS); !strings.HasPrefix(got, "The server does not accept TLS connections.") {
		t.Errorf("TLS message = %q", got)
	}
	if report.OK() {
		t.Error("the report is OK")
	}
}

func TestCheckClosedPort(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	report := checkLocal(t, port)
	want := "Changeport cannot reach 127.0.0.1:" + strconv.Itoa(port) + ": connect: connection refused."
	if got := message(t, report, Reachable); !strings.HasPrefix(got, want) {
		t.Errorf("reachable message = %q, want it to start with %q", got, want)
	}
	if got := statuses(report); got[TLS] != Skipped || got[ReplicationConnection] != Skipped || len(got) != len(connectionChecks)+len(serverChecks) {
		t.Errorf("statuses = %v, want every check after reachable skipped, and no table checks", got)
	}
}

func TestCheckRefusesPrivateAddresses(t *testing.T) {
	connection := validConnection()
	connection.Host = "internal.example.com"
	checker := Checker{Guard: Guard{Lookup: fakeLookup(map[string][]string{"internal.example.com": {"10.0.0.5"}})}}
	report := checker.Check(t.Context(), Target{Provider: Generic, Connection: connection, Names: testNames(t), Tables: chosen})
	want := "The host internal.example.com resolves to 10.0.0.5, which is not public. Changeport connects to databases only over the internet"
	if got := message(t, report, Reachable); !strings.HasPrefix(got, want) {
		t.Errorf("reachable message = %q, want %q", got, want)
	}
	if got := statuses(report); got[Published] != Skipped {
		t.Errorf("statuses = %v, want the table checks skipped", got)
	}

	connection.Host = "missing.example.com"
	report = checker.Check(t.Context(), Target{Provider: Generic, Connection: connection, Names: testNames(t)})
	if got := message(t, report, Reachable); got != "The host missing.example.com does not exist." {
		t.Errorf("reachable message = %q", got)
	}
}

func TestCheckIncompleteConnection(t *testing.T) {
	connection := validConnection()
	connection.Password = ""
	report := Checker{}.Check(t.Context(), Target{Provider: Generic, Connection: connection, Names: testNames(t)})
	if got := message(t, report, Reachable); got != "Enter the role's password." {
		t.Errorf("reachable message = %q", got)
	}
}

func TestKeyed(t *testing.T) {
	for _, c := range []struct {
		identity   string
		primaryKey bool
		keyed      bool
	}{
		{"d", true, true}, {"d", false, false}, {"i", false, true}, {"f", true, true}, {"f", false, false}, {"n", true, false},
	} {
		if keyed, reason := keyed(c.identity, c.primaryKey); keyed != c.keyed || (keyed == (reason != "")) {
			t.Errorf("keyed(%q, %t) = %t, %q", c.identity, c.primaryKey, keyed, reason)
		}
	}
}
