package controller

import (
	"fmt"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ReneP92/changeport/api/v1alpha1"
)

var connectGVK = schema.GroupVersionKind{Group: "kafka.strimzi.io", Version: "v1", Kind: "KafkaConnect"}

// cdcReady waits until the Customer's CDCReady condition has status and its
// message.
func cdcReady(t *testing.T, c client.Client, name string, status metav1.ConditionStatus, message string) {
	t.Helper()
	eventually(t, fmt.Sprintf("CDCReady %s: %s", status, message), func() (bool, string) {
		var customer v1alpha1.Customer
		if err := c.Get(t.Context(), types.NamespacedName{Name: name}, &customer); err != nil {
			return false, err.Error()
		}
		condition := meta.FindStatusCondition(customer.Status.Conditions, v1alpha1.ConditionCDCReady)
		if condition == nil {
			return false, "no CDCReady condition"
		}
		return condition.Status == status && condition.Message == message, fmt.Sprintf("%s %s: %s", condition.Status, condition.Reason, condition.Message)
	})
}

// A customer's first source brings the CDC runtime, which goes with the last.
func TestCustomerSourcesBringTheCDCRuntime(t *testing.T) {
	c := startController(t)
	name := fmt.Sprintf("sources-%d", lifecycleRuns.Add(1))
	customer := &v1alpha1.Customer{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: v1alpha1.CustomerSpec{DisplayName: "Sources", Profile: "dev"}}
	if err := c.Create(t.Context(), customer); err != nil {
		t.Fatal(err)
	}
	namespace := "customer-" + name
	readyReason(t, c, name, "CertificatePending")
	setStatus(t, c, certificateGVK, namespace, "external-listener", func(generation int64) map[string]any {
		return map[string]any{"conditions": []any{map[string]any{
			"type": "Ready", "status": "True", "observedGeneration": generation, "lastTransitionTime": "2026-10-21T00:00:00Z",
		}}}
	})
	readyReason(t, c, name, "KafkaPending")
	cdcReady(t, c, name, metav1.ConditionTrue, "No Postgres CDC sources")

	source := validSource(namespace, "app")
	if err := c.Create(t.Context(), source); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the Connect worker is created", func() (bool, string) {
		_, err := getObject(t, c, connectGVK, namespace, "changeport-connect")
		return err == nil, fmt.Sprint(err)
	})
	cdcReady(t, c, name, metav1.ConditionFalse, "KafkaUser "+namespace+"/changeport-connect: operator has not reconciled the latest spec")
	// The cluster's own readiness does not wait for it.
	readyReason(t, c, name, "KafkaPending")

	if err := c.Delete(t.Context(), source); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the Connect worker is deleted", func() (bool, string) {
		_, err := getObject(t, c, connectGVK, namespace, "changeport-connect")
		return apierrors.IsNotFound(err), fmt.Sprint(err)
	})
	// The provisioner deletes the runtime's objects one after another.
	for _, gvk := range []schema.GroupVersionKind{kafkaUserGVK, deploymentGVK} {
		eventually(t, gvk.Kind+" changeport-registry is deleted", func() (bool, string) {
			_, err := getObject(t, c, gvk, namespace, "changeport-registry")
			return apierrors.IsNotFound(err), fmt.Sprint(err)
		})
	}
	cdcReady(t, c, name, metav1.ConditionTrue, "No Postgres CDC sources")

	if err := c.Delete(t.Context(), customer); err != nil {
		t.Fatal(err)
	}
	readyReason(t, c, name, "Deprovisioning")
	finalizeNamespace(t, namespace)
}
