# Translation Docs Automation Guide

## Overview

This document provides a step-by-step user guide for technical writer to **preconfigure and run** the `translation_docs.sh` script.
The script automates translation of documentation and builds the site using Docusaurus, ensuring that all operations are repeatable and safe.
This guide explains all **required variables**, their purposes, and configuration examples to cover all usage cases.

**Key Script Behaviors:**

1. **New Language Initialization: ** When translating to a new language for the first time, the script automatically sets up the required `i18n` directory and
   populates it with initial Docusaurus translation files (like `current.json`) based on an English template which than translated to the target language.

2. **Selective Translation**:

- The `INPUT_PATTERN` variable provides a glob pattern for Markdown files (e.g., `'topic/*.md'`) or a path to a specific JSON file (e.g.,
  `web-translation.json`) and accepts only two kind of patterns: Markdown (`*.md*`) and JSON (`*.json`).
- Take in account it's used for looking target files in the `web/main/docs`, `web/main/blog` and `web/map/src/resources/translations` directories only.
- Also, if the `INPUT_PATTERN` variable is set, the script will ignore all settings related with docusaurus setup and translate files that match the specified
  pattern only.

## Prerequisites

Before running the script, ensure you have the following:

- Linux system with `bash`, `git`, `python3`, and `yarn` installed.
- All required Python packages are installed: pyyaml, openai, httpx.
- Permissions to clone repositories and push branches.
- Access to the target GitHub repository (via SSH key).
- An API key for the translation service.

## Required Variables & Arguments

Below are all environment variables, arguments, and their roles:

| Variable / Argument             | Required | Purpose                                                                 | Example                 |
|---------------------------------|----------|-------------------------------------------------------------------------|-------------------------|
| **LANG (argument #1)**          | Yes      | Target language code for translation, or `all` to process all.          | `fr`, `de`, `all`       |
| **INPUT_PATTERN (argument #2)** | No       | Optional file pattern to filter which files are translated.             | `'topic/*.md'`          |
| **API_KEY**                     | Yes      | API key for the LLM.                                                    | `sk-abc123xyz`          |
| **MODEL**                       | No       | Model to use for translation. Defaults to `or@google/gemini-2.5-flash`. |                         |
| **TOOLS_PATH**                  | No       | Path to tools project. Should be clone or pull preliminary.             | `/opt/projects/tools`   |
| **WEB_SERVER_CONFIG_PATH**      | No       | Path to web-server-config project.                                      | `/opt/projects/website` |
| **WEB_DIR**                     | No       | Name of documentation git repo. Default is `web`.                       | `web`                   |
| **FORCE_TRANSLATION**           | No       | Marker to force translation even original file is not changed           | true                    |

> **Note:**
> - `LANG` is first **positional argument** only (not an environment variable).
> - `INPUT_PATTERN` is second **positional argument** and can be set as an environment variable also.
> - All other variables can be set via `export` before running the script.

## Usage Example

### 1. Clone the web-server-config/ and web/ repositories.

```bash
git clone htps://github.com/osmandapp/OsmAnd-tools tools                      # Clone tools

git clone ssh://git@github.com/osmandapp/web-server-config                      # Clone web-server-config
cd web-server-config && git checkout main && git pull       # Optional if you want to use a different branch

git clone htps://github.com/osmandapp/web                                    # Clone web
cd web && git checkout main && git reset --hard && git pull # Optional if you want to use a different branch
```

### 2. Set Required Variables and Run script (example for French translation).

```bash
export API_KEY="your_LLM_api_key_here"                  # LLM api key
export WEB_SERVER_CONFIG_PATH=$(pwd)/web-server-config  # Optional, path to web-server-config
export WEB_REPO=$(pwd)/web                              # Optional, path to web

cd tools                                                      # Set tools as current dir
python/translation/translate_docs.sh fr                       # Example to translate all updated files to French
python/translation/translate_docs.sh --force fr               # Example to translate all files to French even they are not changed
python/translation/translate_docs.sh all web-translation.json # Example to translate only web-translation.json to all current languages
```

### 3. Commit & push changes

git checkout -b "test-fr-translate-2025-06-12_14-32"                # Create and switch to new branch
git add -A
git add -f ./map/src/resources/translations/fr/web-translation.json # Add specific file because web-translation.json is in ignore list
git commit -m "Auto translate fr"
git push --force origin "test-fr-translate-2025-06-12_14-32"

### 4. Clean up after translation (optional)

```bash
#rm -rf web-server-config/
#rm -rf web/
