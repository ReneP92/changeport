package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// The providers a PostgresSource's database runs on, as internal/cdc knows
// them.
const (
	ProviderGeneric  = "generic"
	ProviderRDS      = "rds"
	ProviderCloudSQL = "cloudsql"
	ProviderAzure    = "azure"
	ProviderSupabase = "supabase"
	ProviderNeon     = "neon"
)

// The TLS modes of a PostgresSource's connection.
const (
	// TLSVerifyFull checks that a trusted authority signed the database's
	// certificate for its host.
	TLSVerifyFull = "verify-full"
	// TLSVerifyCA checks only that the given CA signed it.
	TLSVerifyCA = "verify-ca"
)

// The states a customer sets a PostgresSource to.
const (
	StateRunning = "Running"
	// StateStopped pauses the source: its slot and offsets stay, so it
	// resumes where it stopped, while the database keeps WAL for it.
	StateStopped = "Stopped"
)

// The phases of a PostgresSource.
const (
	// PhasePending: the source waits for its runtime or its checks.
	PhasePending = "Pending"
	// PhaseSnapshotting: existing rows of some tables are being copied,
	// while changes stream.
	PhaseSnapshotting = "Snapshotting"
	// PhaseStreaming: every table's rows are copied, and changes stream.
	PhaseStreaming = "Streaming"
	// PhaseStopped: the source is paused.
	PhaseStopped = "Stopped"
	// PhaseFailed: the source stopped on an error its conditions explain,
	// such as a lost slot.
	PhaseFailed = "Failed"
	// PhaseDeleting: the source is deleted, and its slot is being dropped.
	PhaseDeleting = "Deleting"
)

// The snapshot states of a PostgresSource's table.
const (
	SnapshotPending = "Pending"
	SnapshotRunning = "Running"
	SnapshotDone    = "Done"
	SnapshotFailed  = "Failed"
)

// PostgresPasswordKey is the key of the password in a PostgresSource's
// password Secret.
const PostgresPasswordKey = "password"

// PostgresPasswordSecretType is the type of the Secrets the console writes a
// source's password into, once.
const PostgresPasswordSecretType = "changeport.io/postgres-password" //nolint:gosec // G101: a Secret type, not a credential

// SourceFinalizer keeps a deleted PostgresSource until the source controller
// has dropped its replication slot.
const SourceFinalizer = "changeport.io/drop-slot"

// SlotDroppedAnnotation says, set to "true" on a deleted PostgresSource, that
// the customer dropped its slot themselves, when Changeport cannot reach the
// database to.
const SlotDroppedAnnotation = "changeport.io/slot-dropped"

// The reasons of a PostgresSource's Ready condition.
const (
	// SourceReasonStreaming: changes stream. Ready is True.
	SourceReasonStreaming = "Streaming"
	// SourceReasonWaitingForPassword: the console has not written the
	// password Secret the spec names.
	SourceReasonWaitingForPassword = "WaitingForPassword"
	// SourceReasonStarting: the connector is starting.
	SourceReasonStarting = "Starting"
	// SourceReasonStopped: the source is stopped.
	SourceReasonStopped = "Stopped"
	// SourceReasonFailed: the connector failed; the message says why.
	SourceReasonFailed = "Failed"
	// SourceReasonDeleting: the source is deleted, and its slot is being
	// dropped; the message says what it waits for.
	SourceReasonDeleting = "Deleting"
	// SourceReasonDatabaseUnreachable: the source is deleted, and Changeport
	// cannot reach the database to drop its slot. The message has the SQL
	// for the customer, who then sets SlotDroppedAnnotation.
	SourceReasonDatabaseUnreachable = "DatabaseUnreachable"
)

// PostgresConnection is how Changeport reaches the customer's database.
type PostgresConnection struct {
	// Host is the database's public host name or address, without a port.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9.:-]+$`
	Host string `json:"host"`

	// Port is the database's port.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +kubebuilder:default=5432
	// +optional
	Port int32 `json:"port,omitempty"`

	// Database is the database the source's tables are in. It cannot
	// change: the source's slot and publication belong to it.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="the database cannot change"
	Database string `json:"database"`

	// PasswordSecret names the Secret in the source's namespace that holds
	// the role's password under the key password. The console writes it,
	// and the source controller moves the password into Connect's own
	// Secret and deletes it; naming another Secret gives the role a new
	// password. The role itself is changeport_<name>_<id>.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$`
	PasswordSecret string `json:"passwordSecret"`

	// TLS is how Changeport checks the database's certificate.
	// +kubebuilder:default={}
	// +optional
	TLS PostgresTLS `json:"tls"`
}

// PostgresTLS is how Changeport checks the database's certificate. It always
// connects over TLS.
// +kubebuilder:validation:XValidation:rule="!has(self.mode) || self.mode != 'verify-ca' || has(self.ca)",message="verify-ca needs the database's CA"
type PostgresTLS struct {
	// Mode is verify-full, which checks the certificate's authority and
	// host, or verify-ca, which checks only that the given CA signed it.
	// +kubebuilder:validation:Enum=verify-full;verify-ca
	// +kubebuilder:default=verify-full
	// +optional
	Mode string `json:"mode,omitempty"`

	// CA is the PEM certificates of the authorities that sign the
	// database's certificate. Without it, verify-full trusts the public
	// authorities.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=65536
	// +kubebuilder:validation:XValidation:rule="self.contains('-----BEGIN CERTIFICATE-----')",message="the CA is PEM certificates"
	// +optional
	CA string `json:"ca,omitempty"`
}

// PostgresTable is a table a source syncs into its own topic,
// <source>.<schema>.<table>.
type PostgresTable struct {
	// Schema is the table's schema, as Postgres spells it.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Schema string `json:"schema"`

	// Name is the table's name, as Postgres spells it.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Name string `json:"name"`
}

// PostgresSourceSpec is a customer's Postgres database and the tables
// Changeport syncs from it.
type PostgresSourceSpec struct {
	// ID makes the names the source leaves in the database unique to it:
	// its role, publication and replication slot are
	// changeport_<name>_<id>. The console chooses it, and it cannot change.
	// +kubebuilder:validation:Pattern=`^[a-z0-9]{6}$`
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="the ID cannot change"
	ID string `json:"id"`

	// Provider is where the database runs, which decides the instructions
	// the console shows.
	// +kubebuilder:validation:Enum=generic;rds;cloudsql;azure;supabase;neon
	Provider string `json:"provider"`

	// Connection is how Changeport reaches the database.
	Connection PostgresConnection `json:"connection"`

	// Tables are the tables the source syncs, each into its own topic.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=200
	// +listType=map
	// +listMapKey=schema
	// +listMapKey=name
	Tables []PostgresTable `json:"tables"`

	// RetentionMs is how long the source's topics keep messages, one of the
	// topic retention presets from an hour to 30 days.
	// +kubebuilder:validation:Enum=3600000;21600000;43200000;86400000;259200000;604800000;1209600000;2592000000
	// +kubebuilder:default=604800000
	// +optional
	RetentionMs int64 `json:"retentionMs,omitempty"`

	// State is Running, or Stopped to pause the source.
	// +kubebuilder:validation:Enum=Running;Stopped
	// +kubebuilder:default=Running
	// +optional
	State string `json:"state,omitempty"`
}

// SlotStatus is the health of a source's replication slot, as the source
// controller last read it from pg_replication_slots.
type SlotStatus struct {
	// Name is the slot's name.
	Name string `json:"name"`

	// Active reports whether the connector is reading from the slot.
	Active bool `json:"active"`

	// RetainedWALBytes is the WAL the database keeps for the slot.
	RetainedWALBytes int64 `json:"retainedWALBytes"`

	// WALStatus is Postgres's wal_status of the slot: reserved, extended,
	// unreserved, or lost once the database removed WAL the slot needed.
	// +optional
	WALStatus string `json:"walStatus,omitempty"`

	// SafeWALSizeBytes is how much more WAL the slot may hold back before
	// the database invalidates it; absent without max_slot_wal_keep_size.
	// +optional
	SafeWALSizeBytes *int64 `json:"safeWALSizeBytes,omitempty"`

	// CheckedAt is when the controller read the slot.
	CheckedAt metav1.Time `json:"checkedAt"`
}

// TableStatus is how far one of a source's tables is.
type TableStatus struct {
	// Schema is the table's schema.
	Schema string `json:"schema"`

	// Name is the table's name.
	Name string `json:"name"`

	// Topic is the table's topic.
	Topic string `json:"topic"`

	// Snapshot is Pending, Running, Done or Failed: how far copying the
	// table's existing rows is.
	// +kubebuilder:validation:Enum=Pending;Running;Done;Failed
	Snapshot string `json:"snapshot"`

	// RowsCopied is how many existing rows the snapshot has copied.
	// +optional
	RowsCopied int64 `json:"rowsCopied,omitempty"`

	// RowsEstimated is Postgres's estimate of the table's rows when its
	// snapshot started.
	// +optional
	RowsEstimated int64 `json:"rowsEstimated,omitempty"`
}

// PostgresSourceStatus is how far the source is.
type PostgresSourceStatus struct {
	// ObservedGeneration is the generation of the source the status
	// describes.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Phase is Pending, Snapshotting, Streaming, Stopped, Failed or
	// Deleting.
	// +kubebuilder:validation:Enum=Pending;Snapshotting;Streaming;Stopped;Failed;Deleting
	// +optional
	Phase string `json:"phase,omitempty"`

	// PasswordSecret is the password Secret whose password the source
	// uses, once the controller has moved it.
	// +optional
	PasswordSecret string `json:"passwordSecret,omitempty"`

	// Slot is the health of the source's replication slot.
	// +optional
	Slot *SlotStatus `json:"slot,omitempty"`

	// Tables is how far each table is.
	// +listType=map
	// +listMapKey=schema
	// +listMapKey=name
	// +optional
	Tables []TableStatus `json:"tables,omitempty"`

	// Conditions say what the source waits for, or why it failed.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// PostgresSource streams tables of a customer's Postgres database into
// topics, one per table, over logical replication (docs/cdc.md). It lives in
// the customer's namespace, and its name is its topics' prefix, so it
// follows the rules of cdc.ValidateName.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=pgsource
// +kubebuilder:printcolumn:name="Provider",type=string,JSONPath=`.spec.provider`
// +kubebuilder:printcolumn:name="Host",type=string,JSONPath=`.spec.connection.host`
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.spec.state`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:validation:XValidation:rule="size(self.metadata.name) <= 40 && self.metadata.name.matches('^[a-z]([-a-z0-9]*[a-z0-9])?$')",message="the name is the source's topic prefix: at most 40 lowercase letters, digits and dashes, starting with a letter and ending with a letter or digit"
// +kubebuilder:validation:XValidation:rule="!self.metadata.name.startsWith('changeport') && !self.metadata.name.startsWith('strimzi') && !self.metadata.name.startsWith('connect-')",message="names starting with changeport, strimzi or connect- are reserved for the platform"
type PostgresSource struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PostgresSourceSpec   `json:"spec"`
	Status PostgresSourceStatus `json:"status,omitempty"`
}

// PostgresSourceList is a list of PostgresSources.
//
// +kubebuilder:object:root=true
type PostgresSourceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PostgresSource `json:"items"`
}
