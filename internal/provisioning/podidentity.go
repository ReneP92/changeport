package provisioning

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/eks"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

// PodIdentities manages EKS Pod Identity associations, which give the pods of
// a service account short-lived credentials for an IAM role.
type PodIdentities interface {
	// Associate makes the pods of serviceAccount in namespace assume the role
	// roleName. It is idempotent.
	Associate(ctx context.Context, cluster, namespace, serviceAccount, roleName string) error
	// Disassociate removes the association of serviceAccount, if there is one.
	Disassociate(ctx context.Context, cluster, namespace, serviceAccount string) error
}

// eksAPI and stsAPI are the parts of the AWS SDK clients AWSPodIdentities uses.
type eksAPI interface {
	ListPodIdentityAssociations(context.Context, *eks.ListPodIdentityAssociationsInput, ...func(*eks.Options)) (*eks.ListPodIdentityAssociationsOutput, error)
	DescribePodIdentityAssociation(context.Context, *eks.DescribePodIdentityAssociationInput, ...func(*eks.Options)) (*eks.DescribePodIdentityAssociationOutput, error)
	CreatePodIdentityAssociation(context.Context, *eks.CreatePodIdentityAssociationInput, ...func(*eks.Options)) (*eks.CreatePodIdentityAssociationOutput, error)
	UpdatePodIdentityAssociation(context.Context, *eks.UpdatePodIdentityAssociationInput, ...func(*eks.Options)) (*eks.UpdatePodIdentityAssociationOutput, error)
	DeletePodIdentityAssociation(context.Context, *eks.DeletePodIdentityAssociationInput, ...func(*eks.Options)) (*eks.DeletePodIdentityAssociationOutput, error)
}

type stsAPI interface {
	GetCallerIdentity(context.Context, *sts.GetCallerIdentityInput, ...func(*sts.Options)) (*sts.GetCallerIdentityOutput, error)
}

// AWSPodIdentities implements PodIdentities with the EKS API. Roles are named
// in the caller's own account.
type AWSPodIdentities struct {
	EKS eksAPI
	STS stsAPI
}

var _ PodIdentities = (*AWSPodIdentities)(nil)

// NewAWSPodIdentities connects to EKS in region with the default AWS
// credentials, such as an AWS IAM Identity Center session.
func NewAWSPodIdentities(ctx context.Context, region string) (*AWSPodIdentities, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("load AWS configuration: %w", err)
	}
	return &AWSPodIdentities{EKS: eks.NewFromConfig(cfg), STS: sts.NewFromConfig(cfg)}, nil
}

// Associate implements PodIdentities.
func (a *AWSPodIdentities) Associate(ctx context.Context, cluster, namespace, serviceAccount, roleName string) error {
	roleARN, err := a.roleARN(ctx, roleName)
	if err != nil {
		return err
	}
	id, err := a.find(ctx, cluster, namespace, serviceAccount)
	if err != nil {
		return err
	}
	if id == "" {
		_, err := a.EKS.CreatePodIdentityAssociation(ctx, &eks.CreatePodIdentityAssociationInput{
			ClusterName:    aws.String(cluster),
			Namespace:      aws.String(namespace),
			ServiceAccount: aws.String(serviceAccount),
			RoleArn:        aws.String(roleARN),
			// The kubernetes-namespace session tag limits each customer's
			// brokers to their own prefix of the tiered storage bucket.
			DisableSessionTags: aws.Bool(false),
		})
		if err != nil {
			return fmt.Errorf("create Pod Identity association for %s/%s: %w", namespace, serviceAccount, err)
		}
		return nil
	}
	current, err := a.EKS.DescribePodIdentityAssociation(ctx, &eks.DescribePodIdentityAssociationInput{
		ClusterName: aws.String(cluster), AssociationId: aws.String(id),
	})
	if err != nil {
		return fmt.Errorf("describe Pod Identity association %s: %w", id, err)
	}
	if current.Association != nil && aws.ToString(current.Association.RoleArn) == roleARN {
		return nil
	}
	_, err = a.EKS.UpdatePodIdentityAssociation(ctx, &eks.UpdatePodIdentityAssociationInput{
		ClusterName: aws.String(cluster), AssociationId: aws.String(id), RoleArn: aws.String(roleARN),
		DisableSessionTags: aws.Bool(false),
	})
	if err != nil {
		return fmt.Errorf("update Pod Identity association %s: %w", id, err)
	}
	return nil
}

// Disassociate implements PodIdentities.
func (a *AWSPodIdentities) Disassociate(ctx context.Context, cluster, namespace, serviceAccount string) error {
	id, err := a.find(ctx, cluster, namespace, serviceAccount)
	if err != nil || id == "" {
		return err
	}
	_, err = a.EKS.DeletePodIdentityAssociation(ctx, &eks.DeletePodIdentityAssociationInput{
		ClusterName: aws.String(cluster), AssociationId: aws.String(id),
	})
	if err != nil {
		return fmt.Errorf("delete Pod Identity association %s: %w", id, err)
	}
	return nil
}

// find returns the ID of the association of serviceAccount, or "".
func (a *AWSPodIdentities) find(ctx context.Context, cluster, namespace, serviceAccount string) (string, error) {
	out, err := a.EKS.ListPodIdentityAssociations(ctx, &eks.ListPodIdentityAssociationsInput{
		ClusterName: aws.String(cluster), Namespace: aws.String(namespace), ServiceAccount: aws.String(serviceAccount),
	})
	if err != nil {
		return "", fmt.Errorf("list Pod Identity associations for %s/%s: %w", namespace, serviceAccount, err)
	}
	switch len(out.Associations) {
	case 0:
		return "", nil
	case 1:
		return aws.ToString(out.Associations[0].AssociationId), nil
	default:
		return "", fmt.Errorf("%d Pod Identity associations for %s/%s, want at most one", len(out.Associations), namespace, serviceAccount)
	}
}

// roleARN returns the ARN of roleName in the caller's account and partition.
func (a *AWSPodIdentities) roleARN(ctx context.Context, roleName string) (string, error) {
	identity, err := a.STS.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return "", fmt.Errorf("get AWS caller identity: %w", err)
	}
	caller, err := arn.Parse(aws.ToString(identity.Arn))
	if err != nil {
		return "", fmt.Errorf("parse AWS caller identity: %w", err)
	}
	if caller.AccountID == "" {
		return "", errors.New("AWS caller identity has no account")
	}
	return arn.ARN{Partition: caller.Partition, Service: "iam", AccountID: caller.AccountID, Resource: "role/" + roleName}.String(), nil
}
