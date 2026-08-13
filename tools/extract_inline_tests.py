#!/usr/bin/env python3
"""Move inline cfg(test) modules into sibling *_tests.rs files."""

from pathlib import Path
import re


def closing_brace(source: str, opening: int) -> int:
    depth = 0
    index = opening
    state = "normal"
    block_depth = 0
    raw_hashes = 0

    while index < len(source):
        char = source[index]
        pair = source[index : index + 2]

        if state == "normal":
            if pair == "//":
                state = "line_comment"
                index += 2
                continue
            if pair == "/*":
                state = "block_comment"
                block_depth = 1
                index += 2
                continue
            if char == '"':
                state = "string"
            elif char == "'":
                closing = source.find("'", index + 1)
                if closing != -1 and closing - index <= 4:
                    state = "char"
            elif char == "r" or (char == "b" and source[index + 1 : index + 2] == "r"):
                raw_start = index + (2 if char == "b" else 1)
                cursor = raw_start
                while cursor < len(source) and source[cursor] == "#":
                    cursor += 1
                if cursor < len(source) and source[cursor] == '"':
                    raw_hashes = cursor - raw_start
                    state = "raw_string"
                    index = cursor
            elif char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    return index
        elif state == "line_comment":
            if char == "\n":
                state = "normal"
        elif state == "block_comment":
            if pair == "/*":
                block_depth += 1
                index += 2
                continue
            if pair == "*/":
                block_depth -= 1
                if block_depth == 0:
                    state = "normal"
                index += 2
                continue
        elif state in ("string", "char"):
            if char == "\\":
                index += 2
                continue
            if (state == "string" and char == '"') or (state == "char" and char == "'"):
                state = "normal"
        elif state == "raw_string":
            terminator = '"' + ("#" * raw_hashes)
            if source.startswith(terminator, index):
                state = "normal"
                index += len(terminator)
                continue

        index += 1

    raise ValueError(f"unclosed module brace at byte {opening}")


pattern = re.compile(
    r"^(?P<indent>[ \t]*)#\[cfg\(test\)\][ \t]*\n"
    r"(?P=indent)mod[ \t]+(?P<name>[A-Za-z0-9_]+)[ \t]*\{",
    re.MULTILINE,
)

for path in sorted(Path("src").rglob("*.rs")):
    source = path.read_text()
    matches = []
    cursor = 0

    while match := pattern.search(source, cursor):
        opening = source.index("{", match.start())
        closing = closing_brace(source, opening)
        matches.append((match.start(), closing + 1, match["indent"], match["name"], opening, closing))
        cursor = closing + 1

    if not matches:
        continue

    replacements = []
    for start_at, end_at, indent, name, opening, closing in matches:
        if name.endswith("_tests"):
            filename = f"{name}.rs"
        elif name in ("tests", "test"):
            filename = f"{path.stem}_tests.rs"
        else:
            filename = f"{name}_tests.rs"
        destination = path.with_name(filename)
        if destination.exists():
            raise FileExistsError(f"refusing to overwrite {destination}")

        body = source[opening + 1 : closing]
        body = re.sub(r"\A\r?\n", "", body)
        body = re.sub(r"\r?\n[ \t]*\Z", "", body) + "\n"
        destination.write_text(body)
        declaration = (
            f'{indent}#[cfg(test)]\n{indent}#[path = "{filename}"]\n'
            f"{indent}mod {name};"
        )
        replacements.append((start_at, end_at, declaration))
        print(f"{path}: {name} -> {destination}")

    for start_at, end_at, declaration in reversed(replacements):
        source = source[:start_at] + declaration + source[end_at:]
    path.write_text(source)
