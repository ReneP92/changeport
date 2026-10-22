package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// ClerkOrganizationLabel holds a Customer's Clerk organization ID, so the
	// console can find the Customer of a signed-in user with a label selector.
	// The provisioning controller keeps it equal to spec.clerkOrganizationID.
	ClerkOrganizationLabel = "changeport.io/clerk-organization"

	// Finalizer keeps a deleted Customer until the provisioning controller has
	// deleted its Kafka cluster.
	Finalizer = "changeport.io/deprovision"
)

// The condition types of a Customer, in the order its cluster gets ready.
const (
	// ConditionCertificateIssued is True once cert-manager has issued the
	// certificate of the cluster's public listener. The Kafka cluster is only
	// created after it.
	ConditionCertificateIssued = "CertificateIssued"
	// ConditionKafkaReady is True once Strimzi reports the Kafka cluster ready.
	ConditionKafkaReady = "KafkaReady"
	// ConditionUsersReady is True once Strimzi has created the cluster's Kafka
	// users and their credentials.
	ConditionUsersReady = "UsersReady"
	// ConditionAgentAvailable is True once the Kafka agent next to the cluster
	// is running, or when the platform runs no agent.
	ConditionAgentAvailable = "AgentAvailable"
	// ConditionReady is True when all of the above are, so customers can
	// connect. While it is False, its reason says what the cluster waits for.
	ConditionReady = "Ready"
)

// CustomerSpec is what Changeport runs for a customer.
type CustomerSpec struct {
	// DisplayName is the customer's name as people know it, such as "Acme
	// Corp".
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=100
	DisplayName string `json:"displayName"`

	// Profile sizes the customer's dedicated Kafka cluster. It cannot change:
	// moving a cluster to another profile is a migration, not an edit.
	// +kubebuilder:validation:Enum=dev;dev-ha;production
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="the profile cannot change"
	Profile string `json:"profile"`

	// ClerkOrganizationID is the Clerk organization whose members sign in to
	// the console as this customer, if any.
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^org_[A-Za-z0-9]+$`
	// +optional
	ClerkOrganizationID string `json:"clerkOrganizationID,omitempty"`

	// DeletionProtection, when true, keeps the customer's Kafka cluster when
	// the Customer is deleted: the controller deprovisions it only once
	// protection is turned off.
	// +optional
	DeletionProtection bool `json:"deletionProtection,omitempty"`
}

// CustomerStatus is how far the customer's Kafka cluster is.
type CustomerStatus struct {
	// ObservedGeneration is the generation of the Customer the conditions
	// describe.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// BootstrapAddress is the public address the customer's clients
	// bootstrap from, as host:port.
	// +optional
	BootstrapAddress string `json:"bootstrapAddress,omitempty"`

	// Conditions are CertificateIssued, KafkaReady, UsersReady,
	// AgentAvailable and Ready.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Customer is a customer of Changeport and their dedicated Kafka cluster.
// Its name is the customer ID, which names the namespace customer-<name> and
// the cluster's public names, so it follows the rules of
// provisioning.ValidateCustomerID.
//
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Display name",type=string,JSONPath=`.spec.displayName`
// +kubebuilder:printcolumn:name="Profile",type=string,JSONPath=`.spec.profile`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].reason`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:validation:XValidation:rule="size(self.metadata.name) <= 30 && self.metadata.name.matches('^[a-z]([-a-z0-9]*[a-z0-9])?$')",message="the name is the customer ID: at most 30 lowercase letters, digits and dashes, starting with a letter and ending with a letter or digit"
type Customer struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CustomerSpec   `json:"spec"`
	Status CustomerStatus `json:"status,omitempty"`
}

// CustomerList is a list of Customers.
//
// +kubebuilder:object:root=true
type CustomerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Customer `json:"items"`
}
