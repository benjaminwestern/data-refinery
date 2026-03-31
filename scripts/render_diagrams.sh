#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ASSETS_DIR="$ROOT/assets"

for diagram in \
  "$ASSETS_DIR/flow-overview.d2" \
  "$ASSETS_DIR/flow-ingest.d2" \
  "$ASSETS_DIR/flow-analysis.d2" \
  "$ASSETS_DIR/flow-rewrite.d2"
do
  d2 "$diagram" "${diagram%.d2}.svg"
done
