#!/usr/bin/env bash
# Checks with the IAM policy simulator that the brokers' role of an AWS
# environment keeps customers apart in the tiered storage bucket: a session
# for customer A's namespace may read, write, delete and list under
# customer-a/ but not under customer B's prefix, and the other way round,
# and a session for any other namespace may do nothing. It asks IAM about
# the deployed role, as EKS Pod Identity tags each broker's session with its
# namespace (terraform/modules/iam). Needs AWS credentials for the
# environment's account, such as an aws sso login session.
# Usage: hack/tiered-storage-isolation-test.sh <dev|prod>
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

env=${1:?usage: hack/tiered-storage-isolation-test.sh <dev|prod>}
[[ $env == dev || $env == prod ]] || die "the environment is dev or prod, not $env"
command -v aws >/dev/null || die "the AWS CLI is needed"
role="changeport-$env-kafka-broker"
bucket="arn:aws:s3:::changeport-$env-tiered-storage"
role_arn=$(aws iam get-role --role-name "$role" --query Role.Arn --output text) || die "no role $role; is $env applied?"

# Prints IAM's decision on action for a session of namespace: allowed,
# implicitDeny or explicitDeny.
# Usage: decision <namespace> <action> <resource> [s3:prefix]
decision() {
  local context=("ContextKeyName=aws:PrincipalTag/kubernetes-namespace,ContextKeyValues=$1,ContextKeyType=string")
  [[ -z ${4:-} ]] || context+=("ContextKeyName=s3:prefix,ContextKeyValues=$4,ContextKeyType=string")
  aws iam simulate-principal-policy --policy-source-arn "$role_arn" --action-names "$2" --resource-arns "$3" \
    --context-entries "${context[@]}" --query 'EvaluationResults[0].EvalDecision' --output text
}

checks=0
expect() {
  local want=$1 got
  got=$(decision "${@:2}")
  [[ $got == "$want" ]] || die "$2 doing $3 on $4${5:+ with prefix $5}: IAM says $got, want $want"
  checks=$((checks + 1))
}

log "Checking that $role keeps customers' tiered storage apart"
for pair in customer-a:customer-b customer-b:customer-a; do
  own=${pair%%:*} other=${pair##*:}
  for action in s3:GetObject s3:PutObject s3:DeleteObject s3:AbortMultipartUpload s3:ListMultipartUploadParts; do
    expect allowed "$own" "$action" "$bucket/$own/topic-0/00000000000000000000.log"
    expect implicitDeny "$own" "$action" "$bucket/$other/topic-0/00000000000000000000.log"
  done
  expect allowed "$own" s3:ListBucket "$bucket" "$own/"
  expect implicitDeny "$own" s3:ListBucket "$bucket" "$other/"
  expect implicitDeny "$own" s3:ListBucket "$bucket" ""
done
# The platform's own namespaces get nothing, not even a prefix of their own.
expect implicitDeny kube-system s3:GetObject "$bucket/kube-system/anything"
expect implicitDeny kube-system s3:PutObject "$bucket/kube-system/anything"
log "Tiered storage isolation test passed: $checks decisions as expected"
