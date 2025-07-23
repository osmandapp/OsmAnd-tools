import fnmatch
import hashlib
import json
import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import List, Tuple

import yaml

from python.lib.OpenAIClient import OpenAIClient

MODEL = os.getenv('MODEL')
API_KEY = os.getenv('API_KEY')
INPUT_DIR = os.getenv('INPUT_DIR', '')
INPUT_PATTERN = os.getenv('INPUT_PATTERN', '')
LANG = os.getenv('LANG')
IMAGES_EXTS = [".png", ".jpg", ".jpeg", ".gif", ".svg"]
WEB_SERVER_CONFIG_PATH = os.getenv('WEB_SERVER_CONFIG_PATH')

# Validate INPUT_PATTERN
if INPUT_PATTERN:
    try:
        fnmatch.translate(INPUT_PATTERN)
    except Exception as e:
        raise ValueError(f"INPUT_PATTERN '{INPUT_PATTERN}' is not a valid file/directory pattern: {e}")

print(f"LLM: {MODEL}, INPUT_DIR: {INPUT_DIR}, INPUT_PATTERN: {INPUT_PATTERN}, LANG: {LANG}", flush=True)
if not all([MODEL, INPUT_DIR, WEB_SERVER_CONFIG_PATH]):
    raise ValueError("Missing required environment variables (MODEL, INPUT_DIR, WEB_SERVER_CONFIG_PATH)")
if INPUT_PATTERN and not (INPUT_PATTERN.endswith('.json') or '.md' in INPUT_PATTERN):
    raise ValueError("Incorrect INPUT_PATTERN variable. Should be a glob pattern for '*.json' or '*.md*' files.")

script_dir = Path(__file__).parent
with open(script_dir / 'iso_639-1.json', 'r', encoding='utf-8') as f:
    langs = json.load(f)

if LANG == "en" or (LANG != "all" and LANG not in langs):
    raise ValueError(f"Input language '{LANG}' is not 'all' and not in {langs.keys()}")

with open(Path(WEB_SERVER_CONFIG_PATH) / 'llm/translation_prompts.yaml', 'r', encoding='utf-8') as f:
    prompts = yaml.safe_load(f)

llm = OpenAIClient(MODEL, API_KEY)
input_dir = Path(INPUT_DIR)
docs_dir = Path(input_dir, "main/docs")
blog_dir = Path(input_dir, "main/blog")
map_translations_dir = input_dir / "map/src/resources/translations"

_marker = re.compile(r"^\s*source-hash: ([0-9A-Fa-f]+)\s*$", re.I)
HASH_ALGO = "blake2s"


def init_i18n(lang_code: str, i18n_lang_dir: Path):
    def update(name):
        if name in config_data and "languages" in config_data[name]:
            if lang_code not in config_data[name]["languages"]:
                config_data[name]["languages"].append(lang_code)
                with open(translations_config_path, 'w', encoding='utf-8') as f:
                    json.dump(config_data, f, indent=4)
                print(f"Updated {translations_config_path} to include language: {lang_code}", flush=True)

    native_label = langs[lang_code]["nativeName"]
    # Update translations-config.json
    translations_config_path = input_dir / "main/src/translations/translations-config.json"
    if translations_config_path.is_file():
        try:
            with open(translations_config_path, 'r', encoding='utf-8') as f:
                config_data = json.load(f)

            update("android")
            update("ios")
            update("other")
        except json.JSONDecodeError:
            print(f"Error: Could not decode JSON from {translations_config_path}.", flush=True)
        except IOError as e:
            print(f"Error reading or writing {translations_config_path}: {e}", flush=True)
    else:
        print(f"Warning: {translations_config_path} not found. Cannot update languages.", flush=True)

    # Paths for the plugin folders
    docs_dest = i18n_lang_dir / "docusaurus-plugin-content-docs/current"
    blog_dest = i18n_lang_dir / "docusaurus-plugin-content-docs/blog"
    if not os.path.isdir(i18n_lang_dir):
        # Create directories for docs and blog translation
        docs_dest.mkdir(parents=True, exist_ok=True)
        blog_dest.mkdir(parents=True, exist_ok=True)

        # Copy all documentation into the new "current" folder
        if os.path.isdir(docs_dir):
            shutil.copytree(docs_dir, docs_dest, dirs_exist_ok=True)
        else:
            print(f"Warning: {docs_dir} does not exist; skipping docs copy.")

        # Copy all blog content into the new blog folder
        if not os.path.isdir(blog_dir):
            print(f"Warning: {blog_dir} does not exist; skipping blog copy.")
    else:
        print(f"Directory {i18n_lang_dir} already exists.")

    # 2. Update docusaurus.config.js: insert lang into locales array and localeConfigs object
    config_path = os.path.join(input_dir / "main", "docusaurus.config.js")
    if not os.path.isfile(config_path):
        print(f"Could not find {config_path}")
        return False

    with open(config_path, "r", encoding="utf-8") as f:
        content = f.read()

    # --- Update locales array ---
    updated = False
    locales_pattern = re.compile(r"(locales\s*:\s*\[)([^\]]*)(\])", re.MULTILINE)
    m = locales_pattern.search(content)
    if m:
        before, inner, after = m.groups()
        # Check if the lang already exists (e.g., 'fr' or "fr")
        if re.search(rf"['\"]{re.escape(lang_code)}['\"]", inner) is None:
            # Prepare insertion: add ', ' if inner is non-empty, else just the new item
            separator = ", " if inner.strip() else ""
            new_inner = inner + separator + f"'{lang_code}'"
            content = locales_pattern.sub(f"\\1{new_inner}\\3", content, count=1)
            updated = True
    else:
        print("Warning: Could not find a locales array in docusaurus.config.js")
        return False

    # --- Update localeConfigs object ---
    configs_pattern = re.compile(
        r"(localeConfigs\s*:\s*\{\s*)([\s\S]*?)(^[ \t]*\},)",
        re.MULTILINE
    )

    m_cfg = configs_pattern.search(content)
    if m_cfg:
        g1, inner_cfg, g3 = m_cfg.groups()
        # If lang is not already in the inner block, we’ll insert it
        if re.search(rf"{re.escape(lang_code)}\s*:", inner_cfg) is None:
            # Determine the indentation of existing entries by looking at the first indented line
            indent_match = re.search(r"^([ \t]+)\w", inner_cfg, re.MULTILINE)
            indent = indent_match.group(1) if indent_match else "  "

            # Ensure the existing block ends with a comma
            trimmed_inner = inner_cfg.rstrip()
            if not trimmed_inner.endswith(","):
                trimmed_inner += ","

            # Build the new entry line
            new_entry = f"\n{indent}{lang_code}: {{ label: '{native_label}' }},"

            # Reconstruct the inner block, making sure to add a newline after our insertion
            new_inner_cfg = trimmed_inner + new_entry + "\n"

            # Reassemble the full localeConfigs block
            content = configs_pattern.sub(f"\\1{new_inner_cfg}\\3", content, count=1)
            updated = True
    else:
        print("Warning: Could not find a localeConfigs object in docusaurus.config.js")
        return False

    # Write changes back to docusaurus.config.js if needed
    if updated:
        with open(config_path, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"docusaurus.config.js updated with locale '{lang_code}' and label '{native_label}'.")
    else:
        print(f"No updates made to docusaurus.config.js; locale '{lang_code}' may already exist.")
        return False

    return True


def make_sync(original_dir: Path, target_dir: Path, patterns: List[str]) -> None:
    """Synchronizes the target directory with the original directory. """
    if patterns is None:
        patterns = ["*"]
    print(f"Dir {target_dir} is syncing ...")
    # Ensure target directory exists
    target_dir.mkdir(parents=True, exist_ok=True)

    # Sync from original to target
    for item_name in os.listdir(original_dir):
        original_item = original_dir / item_name
        target_item = target_dir / item_name

        if original_item.is_dir():
            make_sync(original_item, target_item, patterns)  # Recurse for directories
        elif original_item.is_file():
            if not any(fnmatch.fnmatch(item_name, pattern) for pattern in patterns):
                continue

            if original_item.suffix.lower() in IMAGES_EXTS:
                make_symlink(original_item, target_item)
                continue

            if not target_item.exists() or digest(original_item) != stored_digest(target_item):
                shutil.copy2(original_item, target_item)
                print(f">> File {original_item.name} is copied.")

    # Clean up items in target that are not in original
    for item_name in os.listdir(target_dir):
        target_item = target_dir / item_name
        original_item = original_dir / item_name

        if not original_item.exists():
            if target_item.is_dir():
                shutil.rmtree(target_item)
                print(f">> Directory {target_item.name} is removed.")
            elif target_item.is_file():
                if not any(fnmatch.fnmatch(item_name, pattern) for pattern in patterns):
                    continue

                target_item.unlink()
                print(f">> File {target_item.name} is removed.")


def make_symlink(original: Path, target: Path) -> None:
    if target.is_symlink():
        return

    if target.exists() and target.is_file() and target.stat().st_size != original.stat().st_size:
        print(f"File {target.name} is skipped because of different size.")
        return

    target.unlink(missing_ok=True)  # delete the file
    link_target = (os.path.relpath(original, target.parent))
    target.symlink_to(link_target)  # create the symlink
    print(f"File {target.name} is made as symlink to {link_target}.")


def digest(path: Path) -> str:
    h = hashlib.new(HASH_ALGO)
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def stored_digest(path: Path):
    try:
        with path.open(encoding="utf-8") as fh:
            if path.suffix == ".json":
                json_code = json.load(fh)
                return None if "sourceHash" not in json_code else json_code.get("sourceHash").get("message")

            i = 0
            for line in fh:
                if i == 2:
                    break
                next_line = line
                i += 1

        m = _marker.match(next_line)
        value = m.groups()[0] if m else None
        return value
    except FileNotFoundError:
        return None


# matcher for single-line ES-module imports
IMPORT_RE = re.compile(r'^\s*import\s+.+?\s+from\s+[\'"].+?[\'"]\s*;?\s*$')


def pull_imports(md_path: Path) -> Tuple[str, List[Tuple[int, str]]]:
    """ Return (markdown_without_imports, list_of_(original_line_index, line_text)).  """
    if not fnmatch.fnmatch(md_path.name, '*.md*'):
        return md_path.read_text(encoding="utf-8"), []

    imports: List[Tuple[int, str]] = []
    body_lines: List[str] = []

    for i, line in enumerate(md_path.read_text(encoding="utf-8").splitlines()):
        if IMPORT_RE.match(line):
            imports.append((i, line.rstrip("\n")))
        else:
            body_lines.append(line)

    return "\n".join(body_lines), imports


def reinsert_imports(content: str, imports: List[Tuple[int, str]]) -> str:
    """
    Re-insert the previously collected import lines. Works even when the file grew/shrank in the meantime.
    """
    if len(imports) == 0:
        return content
    lines = content.splitlines()

    idx = len(lines) - lines[::-1].index('---')
    if idx == len(lines) + 1:
        idx = 0
    for _, text in imports:
        lines.insert(idx, text)
        idx += 1

    if lines[idx].strip() != "":
        lines.insert(idx, "")
    return "\n".join(lines)


def save_dest(path, response, imports, digest_now):
    if path.suffix == ".json":
        json_response = json.loads(response)
        json_response["sourceHash"] = {"message": digest_now}
        response = json.dumps(json_response, indent=2, ensure_ascii=False)
    else:
        if not response.startswith('---'):
            response = f"---\n\n---\n{response}"
        line_break = '' if response.startswith('\n') else '\n'
        response = f"---\nsource-hash: {digest_now}{line_break}{response[4:]}"

    response = reinsert_imports(response, imports)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(response)


def make_translation(prompt: str, src_dir: Path, dest_dir: Path, file_pattern: str) -> None:
    """
    Translate files in src_dir to dest_dir with a given prompt.

    The translation is done by calling the `llm.ask` function with the prompt and the content of the file.
    The translated content is written to the destination file with the same name as the source file.

    :param prompt: The prompt to use for translation.
    :param src_dir: The source directory.
    :param dest_dir: The destination directory.
    :param file_pattern: The file pattern to translate.
    """
    if not src_dir.is_dir() or not src_dir.exists():
        return
    dest_dir.mkdir(parents=True, exist_ok=True)

    count = 0
    print(f"Dir {src_dir} -> {dest_dir} is processing ...")
    for src_path in src_dir.rglob(file_pattern):
        if not src_path.is_file():
            continue

        rel_path = src_path.relative_to(src_dir)
        dest_path = dest_dir / rel_path
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        count += 1
        digest_now = digest(src_path)
        digest_old = stored_digest(dest_path) if dest_path.exists() else None
        if digest_now == digest_old or (digest_old is not None and dest_path == src_path):
            print(f"Translation is skipped. File #{count}: {src_path.name} is unchanged.")
            if dest_path.exists() and os.path.getsize(src_path) > os.path.getsize(dest_path):
                print(f"Warning: src size > dest size ({os.path.getsize(src_path)} > {os.path.getsize(dest_path)})!")
            continue

        content, imports = pull_imports(src_path)  # separate content and imports

        response = llm.ask(prompt, content, 1024 + len(content), 0.0 if dest_dir.suffix == '.json' else -1.0)
        if '```json' in response:
            response = re.sub(r'^```(?:json)?\s*|\s*```$', '', response.strip(), flags=re.DOTALL)

        save_dest(dest_path, response, imports, digest_now)

        print(f"File #{count}: {dest_path.name} is translated.", flush=True)
        if dest_path.exists() and os.path.getsize(src_path) > os.path.getsize(dest_path):
            print(f"Warning: src size > dest size ({os.path.getsize(src_path)} > {os.path.getsize(dest_path)})!")
    print(f"Translation of '{file_pattern}' is finished in {dest_dir.name}. {count} files are processed.", flush=True)


def yarn_install() -> None:
    main_dir = input_dir / "main"
    try:
        print(f"Running 'yarn install' in {main_dir}...", flush=True)
        completed_process_yarn = subprocess.run(["yarn", "install"], cwd=main_dir,
                                                check=True,  # Raises CalledProcessError if command returns a non-zero exit code
                                                capture_output=True, text=True)
        print(f"'yarn install' output:\n{completed_process_yarn.stdout}", flush=True)
        if completed_process_yarn.stderr:
            print(f"'yarn install' errors:\n{completed_process_yarn.stderr}", flush=True)

    except FileNotFoundError:
        print(f"Error: 'yarn' command not found. Please ensure yarn is installed and in your PATH.", flush=True)
        raise
    except subprocess.CalledProcessError as e:
        print(f"Error running 'yarn install' in {main_dir}: {e}", flush=True)
        print(f"Stdout: {e.stdout}", flush=True)
        print(f"Stderr: {e.stderr}", flush=True)
        raise


def create_i18n(i18n_lang_dir: Path, lang_code: str, lang_name: str) -> None:
    i18n_lang_dir.mkdir(parents=True, exist_ok=True)
    print(f"Directory {i18n_lang_dir} created.", flush=True)

    # Run 'npm run write-translations'
    main_dir = input_dir / "main"
    print(f"Running 'npm run write-translations' in {main_dir}...", flush=True)
    try:
        # Correct command: npm run write-translations -- --locale <lang_code>
        completed_process_npm = subprocess.run(["npm", "run", "write-translations", "--", "--locale", lang_code, ], cwd=main_dir, check=True,
                                               capture_output=True, text=True)
        print(f"'npm run write-translations' output:\n{completed_process_npm.stdout}", flush=True)
    except FileNotFoundError:
        print(f"Error: 'npm' command not found. Please ensure npm is installed and in your PATH.", flush=True)
        raise
    except subprocess.CalledProcessError as e:
        print(f"Error running 'npm run write-translations' in {main_dir}: {e}", flush=True)
        print(f"Stdout: {e.stdout}", flush=True)
        print(f"Stderr: {e.stderr}", flush=True)
        raise

    navbar_path = input_dir / "main/i18n/en/docusaurus-theme-classic/navbar.json"
    try:
        if navbar_path.is_file():
            with open(navbar_path, 'r', encoding='utf-8') as f:
                navbar_data = json.load(f)

            key = f"localeDropdown.label.{lang_code}"
            if key not in navbar_data:
                navbar_data[key] = {"message": lang_name, "description": f"Locale dropdown label for {lang_name}"}
                with open(navbar_path, 'w', encoding='utf-8') as f:
                    json.dump(navbar_data, f, indent=2, ensure_ascii=False)
                print(f"Added '{key}' to {navbar_path}", flush=True)

            # Propagate updated navbar.json to all existing i18n folders
            i18n_root_dir = input_dir / "main/i18n"
            for lang_dir in i18n_root_dir.iterdir():
                if not lang_dir.is_dir():
                    continue

                target_navbar = lang_dir / "docusaurus-theme-classic/navbar.json"
                if navbar_path == target_navbar:
                    continue
                try:
                    shutil.copy2(navbar_path, target_navbar)
                except IOError as ce:
                    print(f"Warning: Could not copy {navbar_path} to {target_navbar}: {ce}", flush=True)
            print(f"Copied updated navbar.json to all language folders in {i18n_root_dir}", flush=True)
        else:
            print(f"Warning: {navbar_path} does not exist; cannot update locale dropdown label.", flush=True)
    except (IOError, json.JSONDecodeError) as e:
        print(f"Error updating {navbar_path}: {e}", flush=True)


def process_lang(lang_code: str, lang_name: str, is_update: bool = False) -> None:
    i18n_lang_dir = input_dir / "main/i18n" / lang_code
    root_lang_dir = i18n_lang_dir / "docusaurus-plugin-content-docs"
    docs_lang_dir = root_lang_dir / "current"
    blog_lang_dir = root_lang_dir / "blog"
    if is_update and not i18n_lang_dir.exists():
        return

    print(f"Translation to '{lang_code}' is starting...", flush=True)
    if not INPUT_PATTERN:
        if not i18n_lang_dir.exists():
            create_i18n(i18n_lang_dir, lang_code, lang_name)
        init_i18n(lang_code, i18n_lang_dir)

        extensions = ['*'] if not INPUT_PATTERN else [f'*{ext}' for ext in IMAGES_EXTS]
        make_sync(blog_dir, blog_lang_dir, extensions)
        make_sync(docs_dir, docs_lang_dir, extensions)

        make_translation(prompts['CURRENT_JSON_PROMPT'].format(lang=lang_name), root_lang_dir, root_lang_dir, 'current.json')
        make_translation(prompts['CATEGORY_JSON_PROMPT'].format(lang=lang_name), docs_dir, docs_lang_dir, '_*_.json')
        make_translation(prompts['KEY_VALUE_JSON_PROMPT'].format(lang=lang_name), map_translations_dir / "en", map_translations_dir / lang_code,
                         "web-translation.json")
        make_translation(prompts['MD_PROMPT'].format(lang=lang_name), docs_dir, docs_lang_dir, '*.md*')
    else:
        make_translation(prompts['MD_PROMPT'].format(lang=lang_name), blog_dir, blog_lang_dir, INPUT_PATTERN)
        make_translation(prompts['MD_PROMPT'].format(lang=lang_name), docs_dir, docs_lang_dir, INPUT_PATTERN)
        make_translation(prompts['KEY_VALUE_JSON_PROMPT'].format(lang=lang_name), map_translations_dir / "en", map_translations_dir / lang_code, INPUT_PATTERN)


if __name__ == "__main__":
    yarn_install()
    if LANG and LANG != "all":
        process_lang(LANG, langs[LANG]["name"])
    else:
        for code, lang in langs.items():
            process_lang(code, lang["name"], is_update=True)
