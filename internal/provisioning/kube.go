package provisioning

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
)

// KubeCluster implements Cluster with the client-go dynamic client, so the
// provisioner needs no generated clients for Strimzi's custom resources.
type KubeCluster struct {
	client dynamic.Interface
	mapper meta.RESTMapper
}

var _ Cluster = (*KubeCluster)(nil)

// NewKubeCluster connects to the Kubernetes API described by config.
func NewKubeCluster(config *rest.Config) (*KubeCluster, error) {
	client, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("create dynamic client: %w", err)
	}
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("create discovery client: %w", err)
	}
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(discoveryClient))
	return &KubeCluster{client: client, mapper: mapper}, nil
}

// Apply implements Cluster.
func (k *KubeCluster) Apply(ctx context.Context, obj *unstructured.Unstructured) error {
	resource, err := k.resource(obj.GroupVersionKind(), obj.GetNamespace())
	if err != nil {
		return err
	}
	_, err = resource.Apply(ctx, obj.GetName(), obj, metav1.ApplyOptions{FieldManager: ManagedBy, Force: true})
	return err
}

// Get implements Cluster.
func (k *KubeCluster) Get(ctx context.Context, gvk schema.GroupVersionKind, namespace, name string) (*unstructured.Unstructured, error) {
	resource, err := k.resource(gvk, namespace)
	if err != nil {
		return nil, err
	}
	return resource.Get(ctx, name, metav1.GetOptions{})
}

// Delete implements Cluster.
func (k *KubeCluster) Delete(ctx context.Context, gvk schema.GroupVersionKind, namespace, name string) error {
	resource, err := k.resource(gvk, namespace)
	if err != nil {
		return err
	}
	err = resource.Delete(ctx, name, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

// List implements Cluster.
func (k *KubeCluster) List(ctx context.Context, gvk schema.GroupVersionKind, namespace, labelSelector string) ([]unstructured.Unstructured, error) {
	resource, err := k.resource(gvk, namespace)
	if err != nil {
		return nil, err
	}
	list, err := resource.List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}

func (k *KubeCluster) resource(gvk schema.GroupVersionKind, namespace string) (dynamic.ResourceInterface, error) {
	mapping, err := k.mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if meta.IsNoMatchError(err) {
		return nil, fmt.Errorf("the cluster does not serve %s; is the Strimzi operator installed (make dev-up)? %w", gvk, err)
	}
	if err != nil {
		return nil, fmt.Errorf("map %s to a resource: %w", gvk, err)
	}
	if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
		return k.client.Resource(mapping.Resource).Namespace(namespace), nil
	}
	return k.client.Resource(mapping.Resource), nil
}
