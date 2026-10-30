// Command cdcsql prints the SQL the console shows customers for a step of
// setting up a Postgres CDC source (internal/cdc), so that the tests on kind
// run exactly that SQL against the stand-in database.
//
// Usage: go run ./cmd/cdcsql -step role|tables|add|remove|cleanup -source app -id x1y2z3 \
//
//	[-provider generic] [-password ...] [-tables public.orders,public.customers] [-remaining ...]
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/ReneP92/changeport/internal/cdc"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if !errors.Is(err, flag.ErrHelp) {
			fmt.Fprintln(os.Stderr, "error:", err)
		}
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("cdcsql", flag.ContinueOnError)
	fs.SetOutput(stderr)
	step := fs.String("step", "", "the step: role, tables, add, remove or cleanup")
	source := fs.String("source", "", "the source's name")
	id := fs.String("id", "", "the source's ID")
	provider := fs.String("provider", string(cdc.Generic), "where the database runs")
	password := fs.String("password", "", "the role's password, for the role step")
	tableList := fs.String("tables", "", "comma-separated schema.table names: the source's, or those added or removed")
	remainingList := fs.String("remaining", "", "comma-separated schema.table names the source keeps, for the remove step")
	if err := fs.Parse(args); err != nil {
		return err
	}
	names, err := cdc.NamesFor(*source, *id)
	if err != nil {
		return err
	}
	tables, err := parseTables(*tableList)
	if err != nil {
		return err
	}
	remaining, err := parseTables(*remainingList)
	if err != nil {
		return err
	}
	var script cdc.Script
	switch *step {
	case "role":
		script, err = cdc.RoleSQL(cdc.Provider(*provider), names, *password)
	case "tables":
		script, err = cdc.TablesSQL(names, tables)
	case "add":
		script, err = cdc.AddTablesSQL(names, tables)
	case "remove":
		script, err = cdc.RemoveTablesSQL(names, tables, remaining)
	case "cleanup":
		script, err = cdc.CleanupSQL(cdc.Provider(*provider), names, tables)
	default:
		return fmt.Errorf("unknown step %q; use role, tables, add, remove or cleanup", *step)
	}
	if err != nil {
		return err
	}
	_, err = io.WriteString(stdout, script.SQL)
	return err
}

// parseTables reads comma-separated schema.table names.
func parseTables(list string) ([]cdc.Table, error) {
	var tables []cdc.Table
	if list == "" {
		return nil, nil
	}
	for table := range strings.SplitSeq(list, ",") {
		schema, name, ok := strings.Cut(table, ".")
		if !ok {
			return nil, fmt.Errorf("table %q is not schema.table", table)
		}
		tables = append(tables, cdc.Table{Schema: schema, Name: name})
	}
	return tables, nil
}
