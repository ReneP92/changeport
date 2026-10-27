#!/usr/bin/env bash
# Sets the images the image workflow published as the ones the EKS platforms
# run (go run ./cmd/eksimages), keeping what an open pull request proposes for
# images not published again now. Each file in the directory holds one line,
# <image>=<registry>/changeport/<image>@sha256:<digest>.
# Usage: hack/set-published-images.sh <published-dir> [<commit of the open pull request>]
set -euo pipefail

dir=${1:?usage: hack/set-published-images.sh <published-dir> [<commit>]}
proposal=${2:-}
images_file=internal/provisioning/eks-images.json

images=()
if [[ -n $proposal ]]; then
  for image in kafka connect agent provisioner console; do
    proposed=$(git show "$proposal:$images_file" | jq -r ".$image")
    [[ $proposed == "$(jq -r ".$image" "$images_file")" ]] || images+=("$image=$proposed")
  done
fi
# Later arguments win: what was published now comes last.
while read -r line; do
  [[ -z $line ]] || images+=("$line")
done < <(cat "$dir"/*)
go run ./cmd/eksimages "${images[@]}"
