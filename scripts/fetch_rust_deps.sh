#!/usr/bin/env bash
set -euo pipefail

echo "==> Setting up Rust toolchain and downloading dependencies"

# Determine repository root and move there
if git rev-parse --show-toplevel >/dev/null 2>&1; then
  REPO_ROOT="$(git rev-parse --show-toplevel)"
else
  REPO_ROOT="$(pwd)"
fi
cd "$REPO_ROOT"

# Install rustup if missing
if ! command -v rustup >/dev/null 2>&1; then
  echo "==> Installing rustup (Rust toolchain manager)"
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
  # shellcheck disable=SC1090
  source "$HOME/.cargo/env"
fi

# Ensure cargo is available in PATH for current shell
if ! command -v cargo >/dev/null 2>&1; then
  # shellcheck disable=SC1090
  source "$HOME/.cargo/env"
fi

# Resolve requested toolchain from rust-toolchain or rust-toolchain.toml, default to stable
TOOLCHAIN="stable"
if [[ -f rust-toolchain ]]; then
  TOOLCHAIN="$(tr -d ' \t\r' < rust-toolchain)"
elif [[ -f rust-toolchain.toml ]]; then
  CHANNEL_LINE="$(grep -E '^\s*channel\s*=' rust-toolchain.toml || true)"
  if [[ -n "${CHANNEL_LINE:-}" ]]; then
    TOOLCHAIN="$(echo "$CHANNEL_LINE" | sed -E 's/.*channel\s*=\s*"?([^"]+)"?.*/\1/')"
  fi
fi

echo "==> Using Rust toolchain: ${TOOLCHAIN}"
rustup toolchain install "${TOOLCHAIN}" --profile minimal --component rustfmt --component clippy || rustup toolchain install "${TOOLCHAIN}" --profile minimal
rustup default "${TOOLCHAIN}" || true

# Speed up git-based crate fetches in some environments
export CARGO_NET_GIT_FETCH_WITH_CLI=true

# Collect all Cargo.toml manifests
declare -a MANIFESTS
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  mapfile -t MANIFESTS < <(git ls-files '**/Cargo.toml')
fi
if [[ ${#MANIFESTS[@]} -eq 0 ]]; then
  while IFS= read -r -d '' f; do MANIFESTS+=("$f"); done < <(find . -type f -name Cargo.toml -print0)
fi

if [[ ${#MANIFESTS[@]} -eq 0 ]]; then
  echo "!! No Cargo.toml found. Run this from the project root." >&2
  exit 1
fi

# Prefetch at workspace root if present
if [[ -f Cargo.toml ]]; then
  echo "==> Prefetching dependencies in workspace root"
  if [[ -f Cargo.lock ]]; then
    cargo fetch --locked
  else
    cargo fetch
  fi
fi

# Prefetch for each crate directory containing a Cargo.toml
for manifest in "${MANIFESTS[@]}"; do
  DIR="$(dirname "$manifest")"
  # Skip root if already fetched
  if [[ "$DIR" == "." ]]; then
    continue
  fi
  
  echo "==> Prefetching dependencies in $DIR"
  pushd "$DIR" >/dev/null
  if [[ -f Cargo.lock ]]; then
    cargo fetch --locked
  else
    cargo fetch
  fi
  popd >/dev/null
done

echo "✅ Rust dependencies downloaded. You can now run: cargo run"

