#!/usr/bin/env python3
"""Convert Mintlify .mdx docs to MkDocs Material .md.

One-shot migration helper (run once when replacing Mintlify with MkDocs).
Handles the component subset actually used in moon's docs:

  <Note>/<Warning>/<Tip>/<Info>/<Check>  -> !!! admonitions
  <Steps>/<Step title="X">               -> ### X sequential headings
  <Tabs>/<Tab title="X">                 -> === "X" content tabs (pymdownx.tabbed)
  <AccordionGroup>/<Accordion title="X"> -> ??? note "X" collapsible admonitions
  <Columns>/<CardGroup>/<Card ...>       -> grid cards (md_in_html)
  <Frame>                                -> transparent wrapper

Also: strips Mintlify YAML frontmatter (keeps title/description), injects a
top-level H1 from the frontmatter title when the body has none, and rewrites
Mintlify-absolute internal links (`/configuration`) to relative `.md` paths.

Idempotency / safety: source .mdx files are tracked in git. Re-run after
`git checkout docs/` to start from a pristine tree.
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

DOCS = Path("docs")

ADMONITION = {
    "Note": "note",
    "Tip": "tip",
    "Info": "info",
    "Warning": "warning",
    "Caution": "warning",
    "Danger": "danger",
    "Check": "success",
}
# Container tags that are transparent wrappers — their children render directly.
TRANSPARENT = {"Steps", "Tabs", "AccordionGroup", "Columns", "CardGroup", "Frame"}
# Tags that take a title= attribute and wrap a body.
TITLED = {"Step", "Tab", "Accordion"}
KNOWN = set(ADMONITION) | TRANSPARENT | TITLED | {"Card"}

OPEN_RE = re.compile(r"^(?P<indent>\s*)<(?P<tag>[A-Z][A-Za-z]+)(?P<attrs>(?:\s[^>]*?)?)(?P<sc>/?)>(?P<rest>.*)$")
FENCE_RE = re.compile(r"^\s*(```|~~~)")


def attr(attrs: str, name: str) -> str | None:
    m = re.search(name + r'=(?:"([^"]*)"|\{([^}]*)\})', attrs)
    if not m:
        return None
    return m.group(1) if m.group(1) is not None else m.group(2)


def dedent(lines: list[str]) -> list[str]:
    indents = [len(ln) - len(ln.lstrip()) for ln in lines if ln.strip()]
    cut = min(indents) if indents else 0
    return [ln[cut:] if len(ln) >= cut else ln for ln in lines]


def indent(lines: list[str], n: int) -> list[str]:
    pad = " " * n
    return [(pad + ln if ln.strip() else "") for ln in lines]


def find_close(lines: list[str], start: int, tag: str) -> int:
    """Index of the line closing `tag`, honoring nesting and code fences."""
    depth = 0
    in_fence = False
    open_pat = re.compile(r"<" + tag + r"(\s|>|/)")
    close_pat = re.compile(r"</" + tag + r">")
    for i in range(start, len(lines)):
        if FENCE_RE.match(lines[i]):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        depth += len(open_pat.findall(lines[i]))
        if close_pat.search(lines[i]):
            depth -= len(close_pat.findall(lines[i]))
            if depth <= 0:
                return i
    return len(lines) - 1


def render(lines: list[str]) -> list[str]:
    out: list[str] = []
    i, n = 0, len(lines)
    in_fence = False
    while i < n:
        line = lines[i]
        if FENCE_RE.match(line):
            in_fence = not in_fence
            out.append(line)
            i += 1
            continue
        if in_fence:
            out.append(line)
            i += 1
            continue
        m = OPEN_RE.match(line)
        if not m or m.group("tag") not in KNOWN:
            out.append(line)
            i += 1
            continue

        tag = m.group("tag")
        attrs = m.group("attrs")
        rest = m.group("rest")
        close_tag = f"</{tag}>"

        # Self-closing or inline single-line component.
        if m.group("sc") == "/":
            inner_lines: list[str] = []
            i += 1
        elif close_tag in rest:
            inner_lines = [rest[: rest.index(close_tag)]]
            i += 1
        else:
            j = find_close(lines, i + 1, tag)
            inner_lines = lines[i + 1 : j]
            i = j + 1

        inner = render(dedent(inner_lines))
        out.extend(emit(tag, attrs, inner))
    return out


def emit(tag: str, attrs: str, inner: list[str]) -> list[str]:
    if tag in TRANSPARENT and tag not in ("Columns", "CardGroup"):
        return ["", *inner, ""]
    if tag in ("Columns", "CardGroup"):
        return ['<div class="grid cards" markdown>', "", *inner, "", "</div>", ""]
    if tag in ADMONITION:
        return ["", f"!!! {ADMONITION[tag]}", *indent(inner, 4), ""]
    if tag == "Step":
        title = attr(attrs, "title") or "Step"
        return ["", f"### {title}", "", *inner, ""]
    if tag == "Tab":
        title = attr(attrs, "title") or "Tab"
        return ["", f'=== "{title}"', *indent(inner, 4), ""]
    if tag == "Accordion":
        title = attr(attrs, "title") or "Details"
        return ["", f'??? note "{title}"', *indent(inner, 4), ""]
    if tag == "Card":
        title = attr(attrs, "title") or ""
        href = attr(attrs, "href")
        body = " ".join(s.strip() for s in inner if s.strip())
        head = f"-   [__{title}__]({href})" if href else f"-   __{title}__"
        return [head, *(["    " + body] if body else [])]
    return inner


FM_RE = re.compile(r"^---\n(.*?)\n---\n", re.DOTALL)
LINK_RE = re.compile(r"\]\((/[A-Za-z0-9][A-Za-z0-9/_-]*)(#[^)]*)?\)")


def split_frontmatter(text: str) -> tuple[str | None, str | None, str]:
    m = FM_RE.match(text)
    if not m:
        return None, None, text
    fm = m.group(1)
    title = re.search(r'^title:\s*"?(.*?)"?\s*$', fm, re.MULTILINE)
    desc = re.search(r'^description:\s*"?(.*?)"?\s*$', fm, re.MULTILINE)
    return (
        title.group(1) if title else None,
        desc.group(1) if desc else None,
        text[m.end():],
    )


def build_route_map(files: list[Path]) -> dict[str, Path]:
    routes: dict[str, Path] = {}
    for f in files:
        rel = f.relative_to(DOCS)
        route = "/" + str(rel.with_suffix("")).replace(os.sep, "/")
        routes[route] = rel.with_suffix(".md")
    return routes


def rewrite_links(text: str, current: Path, routes: dict[str, Path], broken: set[str]) -> str:
    cur_dir = current.parent

    def repl(m: re.Match) -> str:
        route, anchor = m.group(1), m.group(2) or ""
        target = routes.get(route)
        if target is None:
            broken.add(route)
            return m.group(0)
        relpath = os.path.relpath(target, cur_dir).replace(os.sep, "/")
        return f"]({relpath}{anchor})"

    return LINK_RE.sub(repl, text)


def convert(path: Path, routes: dict[str, Path], broken: set[str]) -> Path:
    text = path.read_text(encoding="utf-8")
    title, desc, body = split_frontmatter(text)

    body = "\n".join(render(body.splitlines()))
    body = re.sub(r"\n{3,}", "\n\n", body).strip("\n") + "\n"

    has_h1 = False
    in_fence = False
    for ln in body.splitlines():
        if FENCE_RE.match(ln):
            in_fence = not in_fence
            continue
        if not in_fence and re.match(r"^# \S", ln):
            has_h1 = True
            break
    parts: list[str] = []
    if title or desc:
        fm = ["---"]
        if title:
            fm.append(f'title: "{title}"')
        if desc:
            fm.append(f'description: "{desc}"')
        fm.append("---")
        parts.append("\n".join(fm))
    if title and not has_h1:
        parts.append(f"# {title}")
    parts.append(body)
    out_text = "\n\n".join(parts)

    rel = path.relative_to(DOCS).with_suffix(".md")
    out_text = rewrite_links(out_text, rel, routes, broken)

    target = path.with_suffix(".md")
    target.write_text(out_text, encoding="utf-8")
    if path.suffix == ".mdx":
        path.unlink()
    return target


def main(argv: list[str]) -> int:
    if not DOCS.is_dir():
        print("error: run from repo root (docs/ not found)", file=sys.stderr)
        return 1

    all_docs = sorted(set(DOCS.rglob("*.md")) | set(DOCS.rglob("*.mdx")))
    routes = build_route_map(all_docs)

    targets = [Path(a) for a in argv] if argv else all_docs
    broken: set[str] = set()
    for f in targets:
        out = convert(f, routes, broken)
        print(f"  {f}  ->  {out.name}")

    print(f"\nConverted {len(targets)} file(s).")
    if broken:
        print("\nUnresolved internal links (fix manually or add to nav):")
        for b in sorted(broken):
            print(f"  {b}")

    leftovers: list[str] = []
    tag_re = re.compile(r"</?(" + "|".join(KNOWN) + r")[\s/>]")
    for f in DOCS.rglob("*.md"):
        for ln, line in enumerate(f.read_text(encoding="utf-8").splitlines(), 1):
            if tag_re.search(line):
                leftovers.append(f"  {f}:{ln}: {line.strip()[:80]}")
    if leftovers:
        print("\nResidual Mintlify components (check):")
        print("\n".join(leftovers))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
