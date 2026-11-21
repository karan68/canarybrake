#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATIC_DIR="$ROOT/services/copilot-console/static"
TARGET="${1:-}"

case "$TARGET" in
  classic)
    cp "$STATIC_DIR/classic.html" "$STATIC_DIR/index.html"
    echo "✓ Reverted default UI (/) to preserved Classic version (zero restart needed — just refresh browser)."
    ;;
  studio)
    cp "$STATIC_DIR/studio.html" "$STATIC_DIR/index.html"
    echo "✓ Switched default UI (/) to Studio version (zero restart needed — just refresh browser)."
    ;;
  *)
    echo "Usage: $0 [studio|classic]"
    exit 1
    ;;
esac
