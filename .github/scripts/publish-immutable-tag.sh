#!/usr/bin/env bash
# Publish the content-addressable immutable image tag :<sha>-<feature>.
#
# Git-SHA tags are pinned by deploys as content-addressable refs, so this
# never overwrites a SHA tag in place. The tag is feature-qualified so two
# features of one commit (ci / testnet / mainnet) get distinct tags and can't
# collide (same scheme as the rome-evm image tags).
#
# A digest mismatch on an existing tag is benign — the same rome-apps commit
# was rebuilt after a mutable dep ref (rome-sdk main / rome-evm master /
# mollusk main) advanced, so the bytes differ. First-write-wins is preserved and the
# rebuild's content stays reachable via the mutable :<tag> and the index
# digest, so we WARN and exit 0 rather than failing the build.
set -euo pipefail

: "${REPO:?REPO required}"
: "${GIT_SHA:?GIT_SHA required}"
: "${ROME_EVM_FEATURE:?ROME_EVM_FEATURE required}"
: "${AMD64_DIGEST:?AMD64_DIGEST required}"
ARM64_DIGEST="${ARM64_DIGEST:-}"

feature_tag="$(printf '%s' "${ROME_EVM_FEATURE}" | tr ',' '_' | tr -cd 'a-zA-Z0-9_.-')"
ref="${REPO}:${GIT_SHA}-${feature_tag}"

built="${AMD64_DIGEST}"
[ -n "${ARM64_DIGEST}" ] && built="${built} ${ARM64_DIGEST}"

if existing="$(docker buildx imagetools inspect "${ref}" \
      --format '{{ range .Manifest.Manifests }}{{ .Digest }} {{ end }}' 2>/dev/null)"; then
  for d in ${built}; do
    if ! printf '%s' "${existing}" | grep -q -- "${d}"; then
      echo "::warning::${ref} already exists with different per-arch digests — keeping the original (first-write-wins, immutable)."
      echo "  existing manifest: ${existing}"
      echo "  this build:        ${built}"
      echo "  Cause: this rome-apps commit was rebuilt after a mutable dependency ref"
      echo "  (rome-sdk main / rome-evm master / mollusk main) advanced. The rebuild's"
      echo "  content is on the mutable :<tag> and at ${REPO}@<index-digest> — pin the"
      echo "  digest if you need it. Compare rome.mollusk.revision on the two images to"
      echo "  tell whether mollusk was the ref that moved."
      exit 0
    fi
  done
  echo "${ref} already exists with matching digests — idempotent, nothing to do."
  exit 0
fi

digests="${REPO}@${AMD64_DIGEST}"
[ -n "${ARM64_DIGEST}" ] && digests="${digests} ${REPO}@${ARM64_DIGEST}"
echo "Publishing immutable ${ref}"
# shellcheck disable=SC2086 # word-split intended: one repo@digest arg per arch
docker buildx imagetools create --tag "${ref}" ${digests}
