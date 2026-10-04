#!/usr/bin/env bash
set -euo pipefail

mode=${1:-apply}
if (( $# > 1 )) || [[ "$mode" != apply && "$mode" != check ]]; then
    printf 'Usage: bash scripts/format.sh [apply|check]\n' >&2
    exit 2
fi

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd -- "$repo_root"
exec mvn -B "spotless:$mode"
