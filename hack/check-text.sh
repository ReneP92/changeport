#!/usr/bin/env bash
# Fails when a file in the repository contains an em dash (U+2014): the
# project writes a plain dash instead. Checks tracked and new files alike,
# skipping ignored and binary ones.
# Usage: hack/check-text.sh
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

# The em dash from its UTF-8 bytes, so that this file does not contain one.
em_dash=$(printf '\xe2\x80\x94')
if git grep --untracked -n -I -F "$em_dash"; then
  die "the lines above contain an em dash; write a plain dash (-) instead"
fi
log "No em dashes found"
