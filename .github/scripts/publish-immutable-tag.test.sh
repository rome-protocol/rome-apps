#!/usr/bin/env bash
# Tests for publish-immutable-tag.sh. Stubs `docker` on PATH so no registry
# is touched. Asserts the three publish branches, feature-qualification, and
# that a digest mismatch WARNS (exit 0) instead of failing the build.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
SUT="${HERE}/publish-immutable-tag.sh"
REPO="romeprotocol/rome-apps"

fails=0
pass() { printf 'ok   - %s\n' "$1"; }
fail() { printf 'FAIL - %s\n   %s\n' "$1" "$2"; fails=$((fails + 1)); }

# Build an isolated bin dir with a fake `docker`, run SUT, capture out/rc.
# Usage: run <inspect_rc> <inspect_out> [env assignments...]
run() {
  local inspect_rc="$1" inspect_out="$2"; shift 2
  local tmp; tmp="$(mktemp -d)"
  CREATE_LOG="${tmp}/create.log"; : > "${CREATE_LOG}"
  cat > "${tmp}/docker" <<'FAKE'
#!/usr/bin/env bash
if [ "$1" = "buildx" ] && [ "$2" = "imagetools" ]; then
  case "$3" in
    inspect) [ -n "${FAKE_INSPECT_OUT:-}" ] && printf '%s' "${FAKE_INSPECT_OUT}"; exit "${FAKE_INSPECT_RC:-0}" ;;
    create)  echo "$*" >> "${CREATE_LOG}"; exit 0 ;;
  esac
fi
exit 0
FAKE
  chmod +x "${tmp}/docker"
  OUT="$(env "PATH=${tmp}:${PATH}" "CREATE_LOG=${CREATE_LOG}" \
        "FAKE_INSPECT_RC=${inspect_rc}" "FAKE_INSPECT_OUT=${inspect_out}" \
        "REPO=${REPO}" "$@" bash "${SUT}" 2>&1)"
  RC=$?
  CREATE="$(cat "${CREATE_LOG}")"
  rm -rf "${tmp}"
}

# 1. Tag absent -> publish, exit 0, create carries the feature-qualified tag.
run 1 "" GIT_SHA=deadbeef ROME_EVM_FEATURE=ci AMD64_DIGEST=sha256:aaa ARM64_DIGEST=sha256:bbb
[ "${RC}" -eq 0 ] && case "${CREATE}" in *"--tag ${REPO}:deadbeef-ci"*) pass "absent tag -> publishes :sha-feature";; *) fail "absent tag -> publishes :sha-feature" "rc=${RC} create='${CREATE}'";; esac
[ "${RC}" -ne 0 ] && fail "absent tag -> publishes :sha-feature" "rc=${RC} out='${OUT}'"

# 2. Tag exists, all built digests present -> idempotent skip, no create.
run 0 "sha256:aaa sha256:bbb " GIT_SHA=deadbeef ROME_EVM_FEATURE=ci AMD64_DIGEST=sha256:aaa ARM64_DIGEST=sha256:bbb
if [ "${RC}" -eq 0 ] && [ -z "${CREATE}" ] && printf '%s' "${OUT}" | grep -qi idempotent; then
  pass "matching digests -> idempotent skip"
else
  fail "matching digests -> idempotent skip" "rc=${RC} create='${CREATE}' out='${OUT}'"
fi

# 3. Tag exists with different digests -> WARN, exit 0 (not 1), no overwrite.
run 0 "sha256:zzz " GIT_SHA=deadbeef ROME_EVM_FEATURE=ci AMD64_DIGEST=sha256:aaa ARM64_DIGEST=sha256:bbb
if [ "${RC}" -eq 0 ] && [ -z "${CREATE}" ] && printf '%s' "${OUT}" | grep -q '::warning::'; then
  pass "digest mismatch -> warns, exit 0, no overwrite"
else
  fail "digest mismatch -> warns, exit 0, no overwrite" "rc=${RC} create='${CREATE}' out='${OUT}'"
fi

# 4. Feature qualification: a different feature yields a distinct tag.
run 1 "" GIT_SHA=deadbeef ROME_EVM_FEATURE=mainnet AMD64_DIGEST=sha256:aaa ARM64_DIGEST=sha256:bbb
case "${CREATE}" in *":deadbeef-mainnet"*) pass "feature-qualified tag (mainnet)";; *) fail "feature-qualified tag (mainnet)" "create='${CREATE}'";; esac

# 5. Feature sanitization: commas -> _, drop tag-illegal chars (same scheme as the rome-evm image tags).
run 1 "" GIT_SHA=deadbeef "ROME_EVM_FEATURE=ci,foo/bar" AMD64_DIGEST=sha256:aaa ARM64_DIGEST=sha256:bbb
case "${CREATE}" in *":deadbeef-ci_foobar"*) pass "feature sanitized into tag";; *) fail "feature sanitized into tag" "create='${CREATE}'";; esac

# 6. arm64 absent (amd64-only manifest) still publishes from amd64 alone.
run 1 "" GIT_SHA=deadbeef ROME_EVM_FEATURE=ci AMD64_DIGEST=sha256:aaa ARM64_DIGEST=
case "${CREATE}" in *"${REPO}@sha256:aaa"*) pass "amd64-only publishes";; *) fail "amd64-only publishes" "create='${CREATE}'";; esac

echo "----"
if [ "${fails}" -eq 0 ]; then echo "ALL PASS"; exit 0; else echo "${fails} FAILED"; exit 1; fi
