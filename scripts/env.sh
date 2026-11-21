#!/usr/bin/env bash
export CANARYBRAKE_ROOT="${CANARYBRAKE_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
if [ -d "$CANARYBRAKE_ROOT/tools/node/bin" ]; then
  export PATH="$CANARYBRAKE_ROOT/tools/node/bin:$PATH"
fi
if [ -d "$CANARYBRAKE_ROOT/tools/go/bin" ]; then
  export PATH="$CANARYBRAKE_ROOT/tools/go/bin:$PATH"
fi
if [ -d "$CANARYBRAKE_ROOT/.venv/bin" ]; then
  export PATH="$CANARYBRAKE_ROOT/.venv/bin:$PATH"
  export VIRTUAL_ENV="$CANARYBRAKE_ROOT/.venv"
fi
if [ -f "$CANARYBRAKE_ROOT/.env.local" ]; then
  # shellcheck disable=SC1091
  source "$CANARYBRAKE_ROOT/.env.local"
fi
export SYNAPSE_TELEMETRY=0
export SYNAPSE_API_URL="${SYNAPSE_API_URL:-https://api.synapse.2ndbrainlabs.ai}"
if [ -n "${GEMINI_API_KEY:-}" ] && [ -z "${SYNAPSE_LLM_API_KEY:-}" ]; then
  export SYNAPSE_LLM_API_KEY="$GEMINI_API_KEY"
fi
