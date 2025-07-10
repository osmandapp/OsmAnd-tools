#!/bin/bash -e

# Environment ($1 - LANG, $2 - INPUT_PATTERN, $3 - API_KEY)
export LANG="${1:-$LANG}"
export INPUT_PATTERN="${2:-$INPUT_PATTERN}"
export API_KEY="${3:-$API_KEY}"

if [ -z "$WEB_SERVER_CONFIG_PATH" ]; then
  WEB_SERVER_CONFIG_PATH=$(pwd)/web-server-config
fi
if [ -z "$WEB_DIR" ]; then
  WEB_DIR=$(pwd)/web
fi

export MODEL_TEMPERATURE=0.5
export MODEL=or@google/gemini-2.5-flash-preview
export INPUT_DIR=$WEB_DIR
python3 "$WEB_SERVER_CONFIG_PATH/support/translation/translate_docs.py"