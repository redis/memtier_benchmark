#!/usr/bin/env python3
"""Check that Claude and Codex load the same repository instructions and skills."""
from pathlib import Path
import re
import sys


def check(root):
    errors = []
    for alias, source in (("CLAUDE.md", "AGENTS.md"), (".agents/skills", ".claude/skills")):
        path, target = root / alias, root / source
        if not target.exists() or not path.is_symlink() or path.resolve() != target.resolve():
            errors.append(f"{alias} must link to {source}")
    skills = root / ".claude/skills"
    manifests = sorted(skills.glob("*/SKILL.md"))
    if not manifests:
        errors.append("No shared skills found")
    for folder in sorted(skills.iterdir()) if skills.is_dir() else []:
        if folder.is_dir() and not folder.name.startswith(".") and not (folder / "SKILL.md").is_file():
            errors.append(f"{folder.relative_to(root)} is missing SKILL.md")
    for manifest in manifests:
        text = manifest.read_text()
        frontmatter = re.match(r"\A---\n(.*?)\n---(?:\n|$)", text, re.S)
        if not frontmatter:
            errors.append(f"{manifest.relative_to(root)} needs YAML frontmatter")
            continue
        header = frontmatter.group(1)
        if not re.search(r"^name: " + re.escape(manifest.parent.name) + r"\s*$", header, re.M):
            errors.append(f"{manifest.relative_to(root)} name must match its directory")
        if not re.search(r"^description:\s*\S", header, re.M):
            errors.append(f"{manifest.relative_to(root)} needs a description")
        for reference in re.findall(r"`(references/[^`]+)`", text):
            if not (manifest.parent / reference).is_file():
                errors.append(f"{manifest.relative_to(root)} has a missing reference: {reference}")
    return errors


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    errors = check(root)
    for error in errors:
        print(error, file=sys.stderr)
    if not errors:
        print("Claude and Codex share instructions, skills, and skill references")
    sys.exit(bool(errors))
