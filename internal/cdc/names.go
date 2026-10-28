// Package cdc holds what the console and the controllers share about Postgres
// CDC sources, independent of Kubernetes: the names a source uses, the SQL
// customers run to set their database up for it, and the checks that the
// database is ready. The design is in docs/cdc.md.
package cdc

import (
	"crypto/rand"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"unicode/utf16"

	"github.com/ReneP92/changeport/internal/topics"
)

// Limits on sources.
const (
	// MaxSourceNameLength keeps the names derived from a source's name
	// within Postgres's 63 bytes.
	MaxSourceNameLength = 40
	// MaxTables is the most tables one source syncs.
	MaxTables = 200
	// IDLength is the length of a source's ID.
	IDLength = 6
)

// sourceName is a Kubernetes name (a DNS label) that starts with a letter.
var sourceName = regexp.MustCompile(`^[a-z]([a-z0-9-]*[a-z0-9])?$`)

// ValidateName checks a source's name, which is its resource's name and the
// prefix of its topics.
func ValidateName(name string) error {
	switch {
	case name == "":
		return errors.New("name the source")
	case len(name) > MaxSourceNameLength:
		return fmt.Errorf("use at most %d characters", MaxSourceNameLength)
	case !sourceName.MatchString(name):
		return errors.New("use lowercase letters, digits and dashes, starting with a letter and ending with a letter or digit")
	case topics.Reserved(name + "."):
		return fmt.Errorf("names starting with %q are reserved for the platform", reservedPrefix(name+"."))
	}
	return nil
}

func reservedPrefix(name string) string {
	for _, prefix := range topics.ReservedPrefixes {
		if strings.HasPrefix(name, prefix) {
			return prefix
		}
	}
	return ""
}

// idCharacters are the characters of a source's ID.
const idCharacters = "abcdefghijklmnopqrstuvwxyz0123456789"

// NewID returns a random ID for a new source. It makes the names a source
// leaves in the customer's database unique to it, so that two sources of the
// same name, of one customer after another or of two customers, never share
// a replication slot.
func NewID() string {
	id := make([]byte, IDLength)
	for i, b := range randomBytes(IDLength) {
		// 256 is not a multiple of 36, which skews the first 4 characters a
		// little; an ID needs to be unique, not uniform.
		id[i] = idCharacters[int(b)%len(idCharacters)]
	}
	return string(id)
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b) // never returns an error
	return b
}

// ValidateID checks a source's ID.
func ValidateID(id string) error {
	if len(id) != IDLength || strings.Trim(id, idCharacters) != "" {
		return fmt.Errorf("a source's ID is %d lowercase letters and digits, not %q", IDLength, id)
	}
	return nil
}

// Names are the names a source uses, in the customer's database and in Kafka.
type Names struct {
	// Source is the source's name.
	Source string
	// Role is the Postgres role Changeport logs in as.
	Role string
	// Publication is the publication of the source's tables.
	Publication string
	// Slot is the replication slot the connector creates and reads.
	Slot string
	// SignalTopic carries the requests for snapshots to the connector.
	SignalTopic string
	// NotificationTopic carries the connector's snapshot progress.
	NotificationTopic string
}

// NamesFor returns the names of the source with the given name and ID. In the
// customer's database, the role, the publication and the slot share one name,
// changeport_<name>_<id>, with the name's dashes as underscores.
func NamesFor(name, id string) (Names, error) {
	if err := ValidateName(name); err != nil {
		return Names{}, fmt.Errorf("source name %q: %w", name, err)
	}
	if err := ValidateID(id); err != nil {
		return Names{}, err
	}
	database := "changeport_" + strings.ReplaceAll(name, "-", "_") + "_" + id
	return Names{
		Source:            name,
		Role:              database,
		Publication:       database,
		Slot:              database,
		SignalTopic:       "changeport.cdc.signals." + name,
		NotificationTopic: "changeport.cdc.notifications." + name,
	}, nil
}

// Table is a table in the customer's database.
type Table struct {
	Schema string `json:"schema"`
	Name   string `json:"name"`
}

func (t Table) String() string { return t.Schema + "." + t.Name }

// TopicName is the topic of a source's table, named as Debezium names it:
// <source>.<schema>.<table>, with each character Kafka does not allow in
// topic names replaced by an underscore. Like Debezium, which runs on Java,
// it counts characters in UTF-16, so a character outside the Basic
// Multilingual Plane, such as an emoji, becomes two underscores.
func TopicName(source string, table Table) string {
	var b strings.Builder
	for _, r := range source + "." + table.Schema + "." + table.Name {
		if r < 128 && legalTopicCharacter(byte(r)) { //nolint:gosec // G115: below 128
			b.WriteRune(r)
		} else {
			b.WriteString(strings.Repeat("_", len(utf16.Encode([]rune{r}))))
		}
	}
	return b.String()
}

func legalTopicCharacter(c byte) bool {
	return c == '.' || c == '_' || c == '-' || ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9')
}

// ValidateTables checks the tables chosen for a source: at least one, at most
// MaxTables, each named and chosen once.
func ValidateTables(tables []Table) error {
	switch {
	case len(tables) == 0:
		return errors.New("choose at least one table")
	case len(tables) > MaxTables:
		return fmt.Errorf("choose at most %d tables", MaxTables)
	}
	for i, table := range tables {
		if table.Schema == "" || table.Name == "" {
			return fmt.Errorf("table %d has no schema or no name", i+1)
		}
		if strings.ContainsRune(table.String(), 0) {
			return fmt.Errorf("table %q has a NUL character in its name", table)
		}
		if slices.Contains(tables[:i], table) {
			return fmt.Errorf("table %s is chosen twice", table)
		}
	}
	return nil
}

// TableTopic is the topic of one of a source's tables, and the problem that
// stops the table from having it, if any.
type TableTopic struct {
	Table   Table
	Topic   string
	Problem string
}

// Topics returns the topics of a source's tables, in the order of the
// tables, each with the problem that stops it, if any: a name longer than
// Kafka's limit for customers, or one that collides with another table's
// topic or with an existing topic. Existing are the cluster's topics that do
// not belong to this source.
func Topics(source string, tables []Table, existing []string) []TableTopic {
	result := make([]TableTopic, len(tables))
	for i, table := range tables {
		result[i] = TableTopic{Table: table, Topic: TopicName(source, table)}
	}
	for i := range result {
		topic := result[i].Topic
		if len(topic) > topics.MaxNameLength {
			result[i].Problem = fmt.Sprintf("its topic name would be %d characters long, more than the %d allowed", len(topic), topics.MaxNameLength)
			continue
		}
		for j, other := range result {
			if j != i && (other.Topic == topic || topics.Collide(other.Topic, topic)) {
				result[i].Problem = fmt.Sprintf("its topic %s collides with the topic %s of table %s", topic, other.Topic, other.Table)
				break
			}
		}
		if result[i].Problem != "" {
			continue
		}
		for _, other := range existing {
			if other == topic {
				result[i].Problem = fmt.Sprintf("its topic %s exists already", topic)
				break
			}
			if topics.Collide(other, topic) {
				result[i].Problem = fmt.Sprintf("its topic %s collides with the existing topic %s, as dots and underscores count as equal", topic, other)
				break
			}
		}
	}
	return result
}
