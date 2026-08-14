"""Markdown-to-WeChat formatting helpers for codex-wechat-bot."""

from __future__ import annotations

import re

_IMAGE_RE = re.compile(r"!\[[^\]]*\]\([^)]*\)")
_CODE_FENCE_OPEN_RE = re.compile(r"^[ \t]*(`{3,})[^`\r\n]*(?:\r?\n)?$")
_INLINE_CODE_RE = re.compile(r"`([^`]*)`")
_LINK_RE = re.compile(r"\[([^\]]*)\]\([^)]*\)")
_BOLD_ITALIC_RE = re.compile(r"(\*{1,3}|_{1,3})(.+?)\1")
_HEADING_RE = re.compile(r"^#{1,6}\s*", re.MULTILINE)
_IMAGE_URL_RE = re.compile(r"!\[[^\]]*\]\((https?://[^)\s]+)\)")


def _strip_inline_markdown(text: str) -> str:
    text = _IMAGE_RE.sub("", text)
    text = _INLINE_CODE_RE.sub(r"\1", text)
    text = _LINK_RE.sub(r"\1", text)
    text = _BOLD_ITALIC_RE.sub(r"\2", text)
    return _HEADING_RE.sub("", text)


def markdown_to_plain_text(text: str) -> str:
    """Strip common Markdown while preserving literal fenced-code content."""

    lines = text.splitlines(keepends=True)
    rendered: list[str] = []
    outside: list[str] = []
    index = 0

    def flush_outside() -> None:
        if outside:
            rendered.append(_strip_inline_markdown("".join(outside)))
            outside.clear()

    while index < len(lines):
        opening = _CODE_FENCE_OPEN_RE.match(lines[index])
        if opening is None:
            outside.append(lines[index])
            index += 1
            continue

        fence_length = len(opening.group(1))
        closing_re = re.compile(
            rf"^[ \t]*`{{{fence_length},}}[ \t]*(?:\r?\n)?$"
        )
        closing = index + 1
        while closing < len(lines) and closing_re.match(lines[closing]) is None:
            closing += 1
        if closing == len(lines):
            # An unmatched opening is ordinary text, not permission to hide
            # the remainder of a user-visible response.
            outside.append(lines[index])
            index += 1
            continue

        flush_outside()
        body = "".join(lines[index + 1 : closing])
        # Keep the code body's terminal line break. It separates any text that
        # follows the closing fence, while the final ``strip`` still removes it
        # when the fenced block ends the message.
        rendered.append(body)
        index = closing + 1

    flush_outside()
    return "".join(rendered).strip()


def extract_image_urls(text: str) -> list[str]:
    """Extract image URLs from markdown image syntax `![alt](url)`."""
    return _IMAGE_URL_RE.findall(text)
