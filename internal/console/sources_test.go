package console

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/ReneP92/changeport/api/v1alpha1"
)

const consoleUser = "system:serviceaccount:changeport:changeport-console"

// startAPIServer starts a real API server with Changeport's CRDs, the
// console's ClusterRole and its Secrets admission policy, and returns a
// client of the cluster's admin and one of the console's service account.
func startAPIServer(t *testing.T) (admin, console client.Client) {
	t.Helper()
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("KUBEBUILDER_ASSETS is not set; run the tests with just test")
		}
		t.Skip("envtest is not installed; run the tests with just test")
	}
	root := filepath.Join("..", "..")
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join(root, "deploy", "base", "provisioner")},
		ErrorIfCRDPathMissing: true,
	}
	config, err := env.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = env.Stop() })
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	admin, err = client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range []string{"console.yaml", "password-secrets.yaml"} {
		for _, object := range manifests(t, filepath.Join(root, "deploy", "base", "console", file)) {
			switch object.GetKind() {
			case "ClusterRole", "ClusterRoleBinding", "ValidatingAdmissionPolicy", "ValidatingAdmissionPolicyBinding":
				if err := admin.Create(t.Context(), object); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	consoleConfig := rest.CopyConfig(config)
	consoleConfig.Impersonate = rest.ImpersonationConfig{UserName: consoleUser}
	console, err = client.New(consoleConfig, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	return admin, console
}

func manifests(t *testing.T, path string) []*unstructured.Unstructured {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	decoder := utilyaml.NewYAMLOrJSONDecoder(file, 4096)
	var objects []*unstructured.Unstructured
	for {
		object := &unstructured.Unstructured{}
		if err := decoder.Decode(&object.Object); err != nil {
			if errors.Is(err, io.EOF) {
				return objects
			}
			t.Fatal(err)
		}
		if len(object.Object) > 0 {
			objects = append(objects, object)
		}
	}
}

// TestKubeSourcesAsTheConsole runs KubeSources as the console's service
// account, which the admission policy allows only write-once password
// Secrets of sources.
func TestKubeSourcesAsTheConsole(t *testing.T) {
	admin, console := startAPIServer(t)
	ctx := t.Context()
	if err := admin.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "customer-acme"}}); err != nil {
		t.Fatal(err)
	}
	sources := KubeSources{Client: console}
	source := &v1alpha1.PostgresSource{
		ObjectMeta: metav1.ObjectMeta{Name: "shop"},
		Spec: v1alpha1.PostgresSourceSpec{
			ID: "k3x9q2", Provider: "generic",
			Connection: v1alpha1.PostgresConnection{
				Host: "db.example.com", Port: 5432, Database: "shop",
				TLS: v1alpha1.PostgresTLS{Mode: v1alpha1.TLSVerifyFull},
			},
			Tables:      []v1alpha1.PostgresTable{{Schema: "public", Name: "orders"}},
			RetentionMs: 604800000, State: v1alpha1.StateRunning,
		},
	}
	if err := sources.Create(ctx, "acme", source, "Old-p4ssword"); err != nil {
		t.Fatal(err)
	}
	first := source.Spec.Connection.PasswordSecret
	secret := passwordSecret(t, admin, first)
	if string(secret.Data[v1alpha1.PostgresPasswordKey]) != "Old-p4ssword" || len(secret.OwnerReferences) != 1 ||
		secret.OwnerReferences[0].UID != source.UID || secret.Labels["changeport.io/source"] != "shop" {
		t.Errorf("the password Secret %+v, want the password, owned by the source", secret.ObjectMeta)
	}
	if err := sources.Create(ctx, "acme", &v1alpha1.PostgresSource{ObjectMeta: metav1.ObjectMeta{Name: "shop"}, Spec: source.Spec}, "x"); !errors.Is(err, ErrSourceExists) {
		t.Errorf("creating the source again = %v, want ErrSourceExists", err)
	}

	got, err := sources.Get(ctx, "acme", "shop")
	if err != nil || got == nil || got.Spec.Connection.PasswordSecret != first {
		t.Fatalf("Get() = %+v, %v", got, err)
	}
	if list, err := sources.List(ctx, "acme"); err != nil || len(list) != 1 {
		t.Errorf("List() = %d sources, %v", len(list), err)
	}
	if missing, err := sources.Get(ctx, "acme", "Not_A_Name"); missing != nil || err != nil {
		t.Errorf("Get() of an invalid name = %v, %v", missing, err)
	}

	// A new password is a new Secret, which the source then names.
	if err := sources.SetPassword(ctx, got, "New-p4ssword"); err != nil {
		t.Fatal(err)
	}
	second := got.Spec.Connection.PasswordSecret
	if second == first || !strings.HasPrefix(second, passwordSecretPrefix+"shop-") ||
		string(passwordSecret(t, admin, second).Data[v1alpha1.PostgresPasswordKey]) != "New-p4ssword" {
		t.Errorf("after a new password the source names %s", second)
	}
	// A source read before the change is stale.
	got.Spec.State = v1alpha1.StateStopped
	if err := sources.Update(ctx, source); !errors.Is(err, ErrSourceChanged) {
		t.Errorf("updating a stale source = %v, want ErrSourceChanged", err)
	}

	// What the console must not do with Secrets.
	if err := console.Get(ctx, client.ObjectKey{Namespace: "customer-acme", Name: first}, &corev1.Secret{}); !apierrors.IsForbidden(err) {
		t.Errorf("the console reading a password Secret = %v, want forbidden", err)
	}
	if err := console.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "customer-acme", Name: first}}); !apierrors.IsForbidden(err) {
		t.Errorf("the console deleting a password Secret = %v, want forbidden", err)
	}
	for name, secret := range map[string]*corev1.Secret{
		"an Opaque Secret": {
			ObjectMeta: metav1.ObjectMeta{Namespace: "customer-acme", Name: passwordSecretPrefix + "x"},
			Type:       corev1.SecretTypeOpaque, Immutable: ptr.To(true), Data: map[string][]byte{"password": []byte("x")},
		},
		"a mutable Secret": {
			ObjectMeta: metav1.ObjectMeta{Namespace: "customer-acme", Name: passwordSecretPrefix + "x"},
			Type:       v1alpha1.PostgresPasswordSecretType, Data: map[string][]byte{"password": []byte("x")},
		},
		"another name": {
			ObjectMeta: metav1.ObjectMeta{Namespace: "customer-acme", Name: "admin"},
			Type:       v1alpha1.PostgresPasswordSecretType, Immutable: ptr.To(true), Data: map[string][]byte{"password": []byte("x")},
		},
		"another namespace": {
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: passwordSecretPrefix + "x"},
			Type:       v1alpha1.PostgresPasswordSecretType, Immutable: ptr.To(true), Data: map[string][]byte{"password": []byte("x")},
		},
	} {
		// The API server loads a new policy within a few seconds.
		deadline := time.Now().Add(15 * time.Second)
		for {
			err := console.Create(ctx, secret.DeepCopy(), client.DryRunAll)
			if err != nil && strings.Contains(err.Error(), "changeport-console-secrets") {
				break
			}
			if time.Now().After(deadline) {
				t.Errorf("the console creating %s = %v, want the admission policy's refusal", name, err)
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

	deleting, err := sources.Get(ctx, "acme", "shop")
	if err != nil {
		t.Fatal(err)
	}
	// The source controller's finalizer keeps a deleted source until its
	// slot is dropped.
	deleting.Finalizers = []string{v1alpha1.SourceFinalizer}
	if err := admin.Update(ctx, deleting); err != nil {
		t.Fatal(err)
	}
	if err := sources.Delete(ctx, deleting); err != nil {
		t.Fatal(err)
	}
	deleting, err = sources.Get(ctx, "acme", "shop")
	if err != nil || deleting == nil || deleting.DeletionTimestamp == nil {
		t.Fatalf("after Delete(), Get() = %+v, %v", deleting, err)
	}
	if err := sources.ConfirmSlotDropped(ctx, deleting); err != nil {
		t.Fatal(err)
	}
	if confirmed, _ := sources.Get(ctx, "acme", "shop"); confirmed.Annotations[v1alpha1.SlotDroppedAnnotation] != "true" {
		t.Errorf("annotations %v, want the slot confirmed dropped", confirmed.Annotations)
	}
	// Deleting a source that is gone, or replaced, is no error.
	if err := sources.Delete(ctx, deleting); err != nil {
		t.Errorf("deleting the source again = %v", err)
	}
}

func passwordSecret(t *testing.T, admin client.Client, name string) *corev1.Secret {
	t.Helper()
	secret := &corev1.Secret{}
	if err := admin.Get(t.Context(), client.ObjectKey{Namespace: "customer-acme", Name: name}, secret); err != nil {
		t.Fatalf("the password Secret %s: %v", name, err)
	}
	if secret.Type != v1alpha1.PostgresPasswordSecretType || secret.Immutable == nil || !*secret.Immutable || len(secret.Data) != 1 {
		t.Errorf("the password Secret %s is %s, immutable %v, with %d keys", name, secret.Type, secret.Immutable, len(secret.Data))
	}
	return secret
}
