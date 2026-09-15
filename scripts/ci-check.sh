#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
  lint)
    exec uv run ruff check .
    ;;
  test)
    exec uv run pytest
    ;;
  *)
    echo "Usage: $0 {lint|test}" >&2
    exit 2
    ;;
esac
