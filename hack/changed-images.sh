#!/usr/bin/env bash
# Prints, as a JSON list, the platform's own images whose sources changed
# between two commits, which .github/workflows/publish-images.yml then builds
# and publishes. Without a first commit, or with one git does not have, it
# prints every image.
#
# The images the EKS platforms run (internal/provisioning/eks-images.json and
# deploy/components/eks-images) change with every publish, so they do not count
# as sources, or every publish would cause another. The provisioner is the
# exception: it embeds the Kafka, Connect, agent and registry gateway images
# it provisions
# customers with, so it is published again when those change, and that
# publish settles.
# Usage: hack/changed-images.sh [<from-commit>] <to-commit>
set -euo pipefail

to=${2:-${1:?usage: hack/changed-images.sh [<from-commit>] <to-commit>}}
from=${2:+$1}
images_file=internal/provisioning/eks-images.json
all='["kafka","connect","agent","registry-gateway","provisioner","console"]'

if [[ -z $from || $from =~ ^0+$ ]] || ! git cat-file -e "$from^{commit}" 2>/dev/null; then
  echo "$all"
  exit
fi

files=$(git diff --name-only "$from" "$to" | grep -v -x -e "$images_file" -e deploy/components/eks-images/kustomization.yaml || true)

# Whether any changed file is under one of the paths.
touches() {
  local path
  for path in "$@"; do
    grep -q -e "^$path" <<<"$files" && return 0
  done
  return 1
}

# The images the provisioner provisions with, in the images file at a commit.
provisioned_images() { git show "$1:$images_file" 2>/dev/null | grep -E '^ *"(kafka|connect|agent|registry-gateway)":' || true; }

go_sources=(cmd/ internal/ go.mod go.sum)
changed=()
touches images/kafka/ && changed+=(kafka)
touches images/connect/ && changed+=(connect)
touches images/agent/ "${go_sources[@]}" && changed+=(agent)
touches images/registry-gateway/ "${go_sources[@]}" && changed+=(registry-gateway)
if touches images/provisioner/ api/ "${go_sources[@]}" ||
  [[ $(provisioned_images "$from") != "$(provisioned_images "$to")" ]]; then
  changed+=(provisioner)
fi
touches images/console/ web/ api/ "${go_sources[@]}" && changed+=(console)

list=""
for image in "${changed[@]}"; do
  list+="${list:+,}\"$image\""
done
echo "[$list]"
