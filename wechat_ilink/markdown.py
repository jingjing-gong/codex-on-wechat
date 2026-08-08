"""Markdown-to-WeChat formatting helpers for codex-wechat-bot."""

from __future__ import annotations

import re

_IMAGE_RE = re.compile(r"!\[[^\]]*\]\([^)]*\)")
_CODE_FENCE_RE = re.compile(r"```.*?```", re.DOTALL)
_INLINE_CODE_RE = re.compile(r"`([^`]*)`")
_LINK_RE = re.compile(r"\[([^\]]*)\]\([^)]*\)")
_BOLD_ITALIC_RE = re.compile(r"(\*{1,3}|_{1,3})(.+?)\1")
_HEADING_RE = re.compile(r"^#{1,6}\s*", re.MULTILINE)
_IMAGE_URL_RE = re.compile(r"!\[[^\]]*\]\((https?://[^)\s]+)\)")


def markdown_to_plain_text(text: str) -> str:
    """Strip common markdown syntax so replies render cleanly as WeChat text."""
    text = _IMAGE_RE.sub("", text)
    text = _CODE_FENCE_RE.sub(lambda m: m.group(0).strip("`"), text)
    text = _INLINE_CODE_RE.sub(r"\1", text)
    text = _LINK_RE.sub(r"\1", text)
    text = _BOLD_ITALIC_RE.sub(r"\2", text)
    text = _HEADING_RE.sub("", text)
    return text.strip()


def extract_image_urls(text: str) -> list[str]:
    """Extract image URLs from markdown image syntax `![alt](url)`."""
    return _IMAGE_URL_RE.findall(text)
