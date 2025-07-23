#!/bin/bash -e

# Environment ($1 - LANG, $2 - INPUT_PATTERN, $3 - API_KEY)
export LANG="${1:-$LANG}"
export INPUT_PATTERN="${2:-$INPUT_PATTERN}"
export API_KEY="${3:-$API_KEY}"

# Compute absolute path of current dir
CURRENT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -z "$WEB_DIR" ]; then
  WEB_DIR="$(realpath "$CURRENT_DIR/../../../web")"
fi
if [ -z "$WEB_SERVER_CONFIG_PATH" ]; then
  export WEB_SERVER_CONFIG_PATH="$(realpath "$CURRENT_DIR/../../../web-server-config")"
fi

export MODEL_TEMPERATURE=0.5
export MODEL=or@google/gemini-2.5-flash
export INPUT_DIR=$WEB_DIR
python3 -m python.translation.translate_docs
