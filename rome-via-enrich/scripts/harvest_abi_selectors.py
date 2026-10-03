#!/usr/bin/env python3
"""Harvest a selector→signature seed from compiled Solidity ABI artifacts.

Rome ships ~1,193 distinct 4-byte method selectors across its own contract
repos, but `method_decoder.rs`'s hand-typed `SEED_SIGNATURES` only covers 112
of them (Rome precompiles + the standard EVM surface). Every other selector
for OUR OWN contracts needlessly round-trips to 4byte.directory the first
time it's seen on-chain. This script closes that gap by reading Hardhat ABI
artifacts and emitting a committed Rust const the runtime seeds alongside
the hand-typed table — zero external dependency at build/run time.

Usage:
    python3 rome-via-enrich/scripts/harvest_abi_selectors.py \
        --root ..   # path to the directory holding the sibling repos

`--root` is the monorepo root that contains the sibling contract repos as
subdirectories (rome-solidity/, rome-uniswap-v2/, ...). Defaults to walking
up from this script's location (scripts/ -> rome-via-enrich/ -> rome-apps/
-> monorepo root), which is correct for a normal checkout but NOT for a
git-worktree checkout of rome-apps alone (the sibling repos live in the
main checkout, not the worktree) — pass --root explicitly in that case.

Dependency: needs a Keccak-256 (NOT NIST SHA3-256 — different padding).
Uses `Crypto.Hash.keccak` from pycryptodome if available, else `eth_utils`.
Install one of: `pip install pycryptodome` or `pip install eth-utils`.

Not run as part of the Docker build — this is offline/dev+CI tooling.
Re-run it and commit the diff whenever a contract repo's ABI changes.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Iterable, Iterator

# Artifact directories to harvest, relative to the monorepo root.
DEFAULT_ARTIFACT_DIRS = [
    "rome-solidity/artifacts",
    "rome-uniswap-v2/artifacts",
    "rome-uniswap-v3/artifacts",
    "rome-uniswap-v4/artifacts",
    "compound-on-rome-comet/artifacts",
    "aerarium/artifacts",
]

_HAND_SEED_ROW_RE = re.compile(r'\(\s*"(0x[0-9a-fA-F]{8})"\s*,\s*"([^"]+)"\s*\)')


def _keccak256(data: bytes) -> bytes:
    """Keccak-256 (Ethereum's, not NIST SHA3-256 — different padding)."""
    try:
        from Crypto.Hash import keccak  # pycryptodome

        h = keccak.new(digest_bits=256)
        h.update(data)
        return h.digest()
    except ImportError:
        pass
    try:
        from eth_utils import keccak  # eth-utils

        return keccak(data)
    except ImportError:
        pass
    raise SystemExit(
        "no Keccak-256 implementation found — install one of:\n"
        "  pip install pycryptodome\n"
        "  pip install eth-utils"
    )


def canonical_input_type(inp: dict) -> str:
    """Canonical ABI type string for one function input/component.

    Simple types (`address`, `uint256`, `bytes32[]`, ...) are already
    canonical in Hardhat's ABI JSON — returned as-is. Tuple types encode the
    array suffix (if any) on the `type` field itself (e.g. `tuple[]`,
    `tuple[2][]`) with the actual field list under `components`; this
    recurses into components so nested tuples (tuple-of-tuples) work.
    """
    t = inp["type"]
    if t.startswith("tuple"):
        suffix = t[len("tuple") :]  # "", "[]", "[2]", "[2][]", ...
        components = inp.get("components", [])
        inner = ",".join(canonical_input_type(c) for c in components)
        return f"({inner}){suffix}"
    return t


def signature_for(entry: dict) -> str:
    """Canonical `name(argtype,argtype,...)` signature for one ABI function entry."""
    args = ",".join(canonical_input_type(i) for i in entry.get("inputs", []))
    return f'{entry["name"]}({args})'


def selector_for(signature: str) -> str:
    """`0x` + first 4 bytes of keccak256(signature), lowercase hex."""
    digest = _keccak256(signature.encode("ascii"))
    return "0x" + digest[:4].hex()


def iter_abi_files(artifact_root: Path) -> Iterator[Path]:
    """Yield artifact JSON files under `artifact_root`, skipping noise.

    Skips `build-info/` (compiler I/O, not per-contract artifacts) and
    `*.dbg.json` (debug sidecar files with no `abi` key).
    """
    if not artifact_root.is_dir():
        return
    for path in sorted(artifact_root.rglob("*.json")):
        if "build-info" in path.parts:
            continue
        if path.name.endswith(".dbg.json"):
            continue
        yield path


def load_abi_entries(path: Path) -> list[dict]:
    """Load the `function`-type ABI entries from one artifact file.

    Handles both the standard Hardhat artifact shape (`{"abi": [...], ...}`)
    and a bare ABI array. Non-function entries (event/error/constructor/
    fallback/receive) are filtered out; an ABI entry with no explicit `type`
    is a function per the ABI spec default.
    """
    try:
        data = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return []

    if isinstance(data, dict):
        abi = data.get("abi")
        if not isinstance(abi, list):
            return []
    elif isinstance(data, list):
        abi = data
    else:
        return []

    return [e for e in abi if isinstance(e, dict) and e.get("type", "function") == "function"]


def parse_hand_seed_selectors(method_decoder_path: Path) -> set[str]:
    """Extract the selector set from `SEED_SIGNATURES` in `method_decoder.rs`.

    Used to exclude hand-curated selectors from the harvested set so the
    generated file never has a chance to clobber a curated Rome-specific
    signature (e.g. precompile ABIs) with an ABI-derived one on next seed.
    """
    text = method_decoder_path.read_text()
    start = text.index("SEED_SIGNATURES")
    end = text.index("];", start)
    body = text[start:end]
    return {sel.lower() for sel, _sig in _HAND_SEED_ROW_RE.findall(body)}


def harvest(
    artifact_roots: Iterable[Path],
) -> tuple[dict[str, str], list[tuple[str, list[str]]], int]:
    """Walk every artifact root, return (selector -> signature, collisions, files_read).

    Collision policy: when two distinct signatures hash to the same
    selector, the lexicographically smallest signature wins deterministically
    (never silently dropped — the loser is recorded in the collisions list).
    """
    candidates: dict[str, set[str]] = {}
    files_read = 0

    for root in artifact_roots:
        for abi_file in iter_abi_files(root):
            entries = load_abi_entries(abi_file)
            if not entries:
                continue
            files_read += 1
            for entry in entries:
                if "name" not in entry:
                    continue
                sig = signature_for(entry)
                sel = selector_for(sig)
                candidates.setdefault(sel, set()).add(sig)

    resolved: dict[str, str] = {}
    collisions: list[tuple[str, list[str]]] = []
    for sel, sigs in candidates.items():
        if len(sigs) > 1:
            ordered = sorted(sigs)
            collisions.append((sel, ordered))
            resolved[sel] = ordered[0]
        else:
            resolved[sel] = next(iter(sigs))

    return resolved, collisions, files_read


def render_rust(
    selector_to_sig: dict[str, str],
    collisions: list[tuple[str, list[str]]],
    files_read: int,
) -> str:
    lines = [
        "//! Auto-generated by `rome-via-enrich/scripts/harvest_abi_selectors.py`.",
        "//! DO NOT EDIT BY HAND — regenerate via that script and commit the diff.",
        "//!",
        "//! Selector→signature seed harvested from compiled Solidity ABI artifacts",
        "//! across Rome's own contract repos (rome-solidity, rome-uniswap-v2/v3/v4,",
        "//! compound-on-rome-comet, aerarium), so the method decoder resolves our own",
        "//! contract calls without hitting 4byte.directory. Seeded alongside the",
        "//! hand-typed `SEED_SIGNATURES` in `method_decoder.rs::seed_static` — see",
        "//! that module for the runtime wiring. Sorted by selector for stable diffs.",
        f"//! Harvested from {files_read} artifact files, {len(selector_to_sig)} distinct selectors.",
    ]
    if collisions:
        lines.append("//!")
        lines.append(
            f"//! {len(collisions)} selector collision(s) — lexicographically smallest"
        )
        lines.append("//! signature kept, per the harvester's deterministic tie-break:")
        for sel, sigs in sorted(collisions):
            lines.append(f"//!   {sel}: kept {sigs[0]!r}, dropped {sigs[1:]!r}")
    lines.append("pub(crate) const GENERATED_SIGNATURES: &[(&str, &str)] = &[")
    for sel in sorted(selector_to_sig):
        sig = selector_to_sig[sel].replace("\\", "\\\\").replace('"', '\\"')
        lines.append(f'    ("{sel}", "{sig}"),')
    lines.append("];")
    lines.append("")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    default_root = Path(__file__).resolve().parents[3]
    parser.add_argument(
        "--root",
        type=Path,
        default=default_root,
        help=f"monorepo root containing the sibling contract repos (default: {default_root})",
    )
    parser.add_argument(
        "--artifact-dir",
        action="append",
        dest="artifact_dirs",
        help="override the default artifact dir list (repeatable, relative to --root)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "src/workers/method_seed_generated.rs",
        help="output Rust file path",
    )
    parser.add_argument(
        "--hand-seed-file",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "src/workers/method_decoder.rs",
        help="method_decoder.rs to read hand-typed SEED_SIGNATURES from, for exclusion",
    )
    args = parser.parse_args(argv)

    artifact_dirs = args.artifact_dirs or DEFAULT_ARTIFACT_DIRS
    artifact_roots = [args.root / d for d in artifact_dirs]

    for root, rel in zip(artifact_roots, artifact_dirs):
        if not root.is_dir():
            print(f"skip (not found): {rel}", file=sys.stderr)

    selector_to_sig, collisions, files_read = harvest(
        r for r in artifact_roots if r.is_dir()
    )

    hand_seed_path = args.hand_seed_file
    if hand_seed_path.is_file():
        hand_seed = parse_hand_seed_selectors(hand_seed_path)
        before = len(selector_to_sig)
        selector_to_sig = {
            sel: sig for sel, sig in selector_to_sig.items() if sel not in hand_seed
        }
        skipped_hand_seed = before - len(selector_to_sig)
    else:
        print(f"warning: hand seed not found at {hand_seed_path}, not excluding it", file=sys.stderr)
        skipped_hand_seed = 0

    args.out.write_text(render_rust(selector_to_sig, collisions, files_read))

    print(f"artifacts read: {files_read}")
    print(f"distinct selectors emitted: {len(selector_to_sig)}")
    print(f"collisions: {len(collisions)}")
    print(f"skipped (already in hand SEED_SIGNATURES): {skipped_hand_seed}")
    print(f"wrote: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
