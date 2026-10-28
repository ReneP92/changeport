package cdc

import (
	"strings"
	"testing"
)

func TestValidateName(t *testing.T) {
	for _, name := range []string{"orders", "app-db", "a", "pg14", strings.Repeat("a", MaxSourceNameLength)} {
		if err := ValidateName(name); err != nil {
			t.Errorf("ValidateName(%q) = %v", name, err)
		}
	}
	for name, want := range map[string]string{
		"": "name the source",
		strings.Repeat("a", MaxSourceNameLength+1): "at most 40",
		"Orders":         "lowercase",
		"1orders":        "starting with a letter",
		"orders-":        "ending with",
		"app_db":         "lowercase letters, digits and dashes",
		"app.db":         "lowercase letters, digits and dashes",
		"changeport-cdc": `reserved for the platform`,
		"strimzi":        `"strimzi"`,
		"connect-orders": `"connect-"`,
	} {
		err := ValidateName(name)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("ValidateName(%q) = %v, want an error with %q", name, err, want)
		}
	}
}

func TestNewID(t *testing.T) {
	seen := map[string]bool{}
	for range 100 {
		id := NewID()
		if err := ValidateID(id); err != nil {
			t.Fatal(err)
		}
		seen[id] = true
	}
	if len(seen) < 99 {
		t.Errorf("100 IDs had %d distinct values", len(seen))
	}
	for _, id := range []string{"", "abc12", "abc1234", "ABC123", "ab-123"} {
		if ValidateID(id) == nil {
			t.Errorf("ValidateID(%q) accepted it", id)
		}
	}
}

func TestNamesFor(t *testing.T) {
	names, err := NamesFor("app-db", "x1y2z3")
	if err != nil {
		t.Fatal(err)
	}
	want := Names{
		Source:            "app-db",
		Role:              "changeport_app_db_x1y2z3",
		Publication:       "changeport_app_db_x1y2z3",
		Slot:              "changeport_app_db_x1y2z3",
		SignalTopic:       "changeport.cdc.signals.app-db",
		NotificationTopic: "changeport.cdc.notifications.app-db",
	}
	if names != want {
		t.Errorf("NamesFor() = %+v, want %+v", names, want)
	}
	// The longest name stays within Postgres's 63 bytes.
	names, err = NamesFor(strings.Repeat("a", MaxSourceNameLength), "x1y2z3")
	if err != nil || len(names.Slot) > 63 {
		t.Errorf("NamesFor(longest) = %q, %v; want at most 63 bytes", names.Slot, err)
	}
	if _, err := NamesFor("App", "x1y2z3"); err == nil {
		t.Error("NamesFor() accepted an invalid name")
	}
	if _, err := NamesFor("app", "X1"); err == nil {
		t.Error("NamesFor() accepted an invalid ID")
	}
}

func TestTopicName(t *testing.T) {
	for table, want := range map[Table]string{
		{"public", "orders"}:      "app.public.orders",
		{"Sales", "Order Lines"}:  "app.Sales.Order_Lines",
		{"public", "my-table.v2"}: "app.public.my-table.v2",
		{"public", "café"}:        "app.public.caf_",
		{"public", "😀"}:           "app.public.__", // two UTF-16 units, as in Java
		{"public", `odd"name$`}:   "app.public.odd_name_",
		{"données", "ventes_v1"}:  "app.donn_es.ventes_v1",
	} {
		if got := TopicName("app", table); got != want {
			t.Errorf("TopicName(%v) = %q, want %q", table, got, want)
		}
	}
}

func TestValidateTables(t *testing.T) {
	if err := ValidateTables([]Table{{"public", "a"}, {"public", "b"}}); err != nil {
		t.Error(err)
	}
	many := make([]Table, MaxTables+1)
	for i := range many {
		many[i] = Table{"public", strings.Repeat("t", i+1)}
	}
	for name, tables := range map[string][]Table{
		"none":      nil,
		"too many":  many,
		"no schema": {{"", "a"}},
		"no name":   {{"public", ""}},
		"twice":     {{"public", "a"}, {"public", "a"}},
		"NUL":       {{"public", "a\x00b"}},
	} {
		if ValidateTables(tables) == nil {
			t.Errorf("ValidateTables(%s) accepted them", name)
		}
	}
}

func TestTopicsFindsCollisionsAndLongNames(t *testing.T) {
	long := Table{"public", strings.Repeat("x", 63)}
	tables := []Table{
		{"public", "orders"},
		{"public", "order lines"}, // order_lines
		{"public", "order_lines"},
		{"a_b", "c"}, // app.a_b.c and app.a.b_c count as equal
		{"a", "b_c"},
		{"public", "customers"},
		long,
	}
	got := Topics("app", tables, []string{"app.public.customers", "app_public.orders_archive"})
	problems := map[Table]string{}
	for _, topic := range got {
		problems[topic.Table] = topic.Problem
	}
	for table, want := range map[Table]string{
		{"public", "orders"}:      "",
		{"public", "order lines"}: "collides with the topic app.public.order_lines of table public.order_lines",
		{"public", "order_lines"}: "collides with the topic app.public.order_lines of table public.order lines",
		{"a_b", "c"}:              "collides with the topic app.a.b_c",
		{"a", "b_c"}:              "collides with the topic app.a_b.c",
		{"public", "customers"}:   "app.public.customers exists already",
		long:                      "",
	} {
		if !strings.Contains(problems[table], want) || (want == "" && problems[table] != "") {
			t.Errorf("problem of %v = %q, want %q", table, problems[table], want)
		}
	}
	if got[0].Topic != "app.public.orders" {
		t.Errorf("topics are not in the tables' order: %+v", got[0])
	}

	// Postgres's identifiers of at most 63 bytes keep topic names within
	// 200 characters, but a server built with longer ones could exceed it.
	longest := Topics("app", []Table{{strings.Repeat("s", 100), strings.Repeat("t", 100)}}, nil)
	if !strings.Contains(longest[0].Problem, "more than the 200 allowed") {
		t.Errorf("problem = %q, want a name too long", longest[0].Problem)
	}
}

func TestTopicsCollidesWithExistingByDotsAndUnderscores(t *testing.T) {
	got := Topics("app", []Table{{"public", "orders"}}, []string{"app_public_orders"})
	if !strings.Contains(got[0].Problem, "collides with the existing topic app_public_orders") {
		t.Errorf("problem = %q", got[0].Problem)
	}
}
