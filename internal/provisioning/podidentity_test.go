package provisioning

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eks"
	"github.com/aws/aws-sdk-go-v2/service/eks/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

// fakeEKS holds Pod Identity associations by ID.
type fakeEKS struct {
	associations map[string]types.PodIdentityAssociation
	calls        []string
	nextID       int
}

func newFakeEKS() *fakeEKS { return &fakeEKS{associations: map[string]types.PodIdentityAssociation{}} }

func (f *fakeEKS) ListPodIdentityAssociations(_ context.Context, in *eks.ListPodIdentityAssociationsInput, _ ...func(*eks.Options)) (*eks.ListPodIdentityAssociationsOutput, error) {
	out := &eks.ListPodIdentityAssociationsOutput{}
	for id, a := range f.associations {
		if aws.ToString(a.Namespace) == aws.ToString(in.Namespace) && aws.ToString(a.ServiceAccount) == aws.ToString(in.ServiceAccount) {
			out.Associations = append(out.Associations, types.PodIdentityAssociationSummary{AssociationId: aws.String(id)})
		}
	}
	return out, nil
}

func (f *fakeEKS) DescribePodIdentityAssociation(_ context.Context, in *eks.DescribePodIdentityAssociationInput, _ ...func(*eks.Options)) (*eks.DescribePodIdentityAssociationOutput, error) {
	a := f.associations[aws.ToString(in.AssociationId)]
	return &eks.DescribePodIdentityAssociationOutput{Association: &a}, nil
}

func (f *fakeEKS) CreatePodIdentityAssociation(_ context.Context, in *eks.CreatePodIdentityAssociationInput, _ ...func(*eks.Options)) (*eks.CreatePodIdentityAssociationOutput, error) {
	f.calls = append(f.calls, "create")
	if aws.ToBool(in.DisableSessionTags) {
		return nil, errors.New("session tags must stay enabled")
	}
	f.nextID++
	id := "a-" + string(rune('0'+f.nextID))
	f.associations[id] = types.PodIdentityAssociation{Namespace: in.Namespace, ServiceAccount: in.ServiceAccount, RoleArn: in.RoleArn}
	return &eks.CreatePodIdentityAssociationOutput{}, nil
}

func (f *fakeEKS) UpdatePodIdentityAssociation(_ context.Context, in *eks.UpdatePodIdentityAssociationInput, _ ...func(*eks.Options)) (*eks.UpdatePodIdentityAssociationOutput, error) {
	f.calls = append(f.calls, "update")
	a := f.associations[aws.ToString(in.AssociationId)]
	a.RoleArn = in.RoleArn
	f.associations[aws.ToString(in.AssociationId)] = a
	return &eks.UpdatePodIdentityAssociationOutput{}, nil
}

func (f *fakeEKS) DeletePodIdentityAssociation(_ context.Context, in *eks.DeletePodIdentityAssociationInput, _ ...func(*eks.Options)) (*eks.DeletePodIdentityAssociationOutput, error) {
	f.calls = append(f.calls, "delete")
	delete(f.associations, aws.ToString(in.AssociationId))
	return &eks.DeletePodIdentityAssociationOutput{}, nil
}

type fakeSTS struct{}

func (fakeSTS) GetCallerIdentity(context.Context, *sts.GetCallerIdentityInput, ...func(*sts.Options)) (*sts.GetCallerIdentityOutput, error) {
	return &sts.GetCallerIdentityOutput{Arn: aws.String("arn:aws:sts::123456789012:assumed-role/Admin/rene")}, nil
}

const brokerRoleARN = "arn:aws:iam::123456789012:role/changeport-prod-kafka-broker"

func TestAWSPodIdentitiesAssociateIsIdempotent(t *testing.T) {
	api := newFakeEKS()
	p := &AWSPodIdentities{EKS: api, STS: fakeSTS{}}
	for range 2 {
		if err := p.Associate(t.Context(), "changeport-prod", "customer-acme", "acme-kafka", "changeport-prod-kafka-broker"); err != nil {
			t.Fatal(err)
		}
	}
	if got := strings.Join(api.calls, ","); got != "create" {
		t.Errorf("calls = %s, want one create", got)
	}
	for _, a := range api.associations {
		if aws.ToString(a.RoleArn) != brokerRoleARN {
			t.Errorf("association role = %s, want %s", aws.ToString(a.RoleArn), brokerRoleARN)
		}
	}
}

func TestAWSPodIdentitiesAssociateFixesTheRole(t *testing.T) {
	api := newFakeEKS()
	api.associations["a-x"] = types.PodIdentityAssociation{
		Namespace: aws.String("customer-acme"), ServiceAccount: aws.String("acme-kafka"), RoleArn: aws.String("arn:aws:iam::123456789012:role/other"),
	}
	p := &AWSPodIdentities{EKS: api, STS: fakeSTS{}}
	if err := p.Associate(t.Context(), "changeport-prod", "customer-acme", "acme-kafka", "changeport-prod-kafka-broker"); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(api.calls, ","); got != "update" || aws.ToString(api.associations["a-x"].RoleArn) != brokerRoleARN {
		t.Errorf("calls = %s, role = %s; want the role updated", got, aws.ToString(api.associations["a-x"].RoleArn))
	}
}

func TestAWSPodIdentitiesDisassociate(t *testing.T) {
	api := newFakeEKS()
	p := &AWSPodIdentities{EKS: api, STS: fakeSTS{}}
	if err := p.Disassociate(t.Context(), "changeport-prod", "customer-acme", "acme-kafka"); err != nil || len(api.calls) != 0 {
		t.Fatalf("Disassociate() without an association = %v, calls %v; want nothing to do", err, api.calls)
	}
	if err := p.Associate(t.Context(), "changeport-prod", "customer-acme", "acme-kafka", "changeport-prod-kafka-broker"); err != nil {
		t.Fatal(err)
	}
	if err := p.Disassociate(t.Context(), "changeport-prod", "customer-acme", "acme-kafka"); err != nil {
		t.Fatal(err)
	}
	if len(api.associations) != 0 {
		t.Errorf("associations left: %v", api.associations)
	}
}

func TestAWSPodIdentitiesRefusesDuplicates(t *testing.T) {
	api := newFakeEKS()
	for _, id := range []string{"a-1", "a-2"} {
		api.associations[id] = types.PodIdentityAssociation{Namespace: aws.String("customer-acme"), ServiceAccount: aws.String("acme-kafka")}
	}
	p := &AWSPodIdentities{EKS: api, STS: fakeSTS{}}
	if err := p.Associate(t.Context(), "changeport-prod", "customer-acme", "acme-kafka", "r"); err == nil {
		t.Error("Associate() with two associations = nil, want an error")
	}
}
