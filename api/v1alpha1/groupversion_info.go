// Package v1alpha1 is the changeport.io/v1alpha1 API: Changeport's own
// Kubernetes resources, served by the CRDs in deploy/base/provisioner.
//
// +kubebuilder:object:generate=true
// +groupName=changeport.io
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	// GroupVersion is the API group and version of this package's resources.
	GroupVersion = schema.GroupVersion{Group: "changeport.io", Version: "v1alpha1"}

	// SchemeBuilder registers this package's resources with a scheme.
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

	// AddToScheme adds this package's resources to a scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(GroupVersion, &Customer{}, &CustomerList{})
	metav1.AddToGroupVersion(scheme, GroupVersion)
	return nil
}
