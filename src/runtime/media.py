"""Managed attachment storage and media value objects.

The channel protocol only gives us remote/encrypted references.  Runtime code
must never pass those references around as if they were local files, and binary
payloads should not be put in SQLite.  :class:`AttachmentStore` provides the
small, deliberately boring file-system boundary used by the rest of the
runtime.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import inspect
import json
import mimetypes
import os
import secrets
import stat
import tempfile
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from collections.abc import Mapping
from typing import Any, Awaitable, BinaryIO, Callable


_ATTACHMENT_ROOT_LOCKS_GUARD = threading.Lock()
_ATTACHMENT_ROOT_LOCKS: dict[str, threading.RLock] = {}


def _attachment_root_lock(root: Path) -> threading.RLock:
    """Return the process-local writer lock shared by one managed root."""

    key = str(root)
    with _ATTACHMENT_ROOT_LOCKS_GUARD:
        return _ATTACHMENT_ROOT_LOCKS.setdefault(key, threading.RLock())


class AttachmentError(RuntimeError):
    """Raised when an attachment cannot be accepted or safely accessed."""


@dataclass(frozen=True, slots=True)
class InboundMediaRef:
    """A channel-owned media reference.

    ``remote_id`` and ``encrypted_query_param`` are intentionally opaque.  A
    channel adapter may use them to download the item, but they are not paths
    and are never handed directly to an Agent runtime.
    """

    channel: str
    bot_id: str = ""
    remote_id: str = ""
    mime_type: str = "application/octet-stream"
    filename: str = ""
    size: int = 0
    checksum: str = ""
    encrypted_query_param: str = ""
    encryption_key: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class StoredAttachment:
    """Immutable metadata for a file managed by :class:`AttachmentStore`."""

    attachment_id: str
    path: str
    mime_type: str
    size: int
    checksum: str
    filename: str = ""
    created_at: str = ""

    @property
    def local_path(self) -> Path:
        return Path(self.path)


@dataclass(frozen=True, slots=True)
class RuntimeMediaInput:
    """The safe input representation passed to an Agent runtime."""

    attachment_id: str
    path: str
    mime_type: str
    kind: str = "file"
    filename: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


# These are the only fields that may cross the channel/runtime boundary as
# Agent-facing media context.  In particular, WeChat CDN query parameters and
# AES keys are transport credentials, not attachment metadata.
_CANONICAL_MEDIA_KEYS = frozenset(
    {
        "attachment_id",
        "path",
        "mime_type",
        "kind",
        "filename",
        "size",
        "size_bytes",
        "checksum",
        "candidate_text",
        "confidence",
        "available",
        "error",
        "native_input_available",
    }
)

# Channel adapters may use any of these aliases for an immutable remote
# reference.  Keep the complete set in the digest even when a canonical media
# projection later drops the transport fields and credentials.
WIRE_MEDIA_FIELDS = (
    "kind",
    "type",
    "media_kind",
    "remote_id",
    "media_id",
    "id",
    "url",
    "file_url",
    "media",
    "media_info",
    "encrypted_query_param",
    "encrypt_query_param",
    "download_param",
    "encryption_key",
    "aes_key",
    "aes_key_base64",
    "aes_key_hex",
    "encrypt_type",
    "upload_param",
    "mime_type",
    "mime",
    "content_type",
    "filename",
    "file_name",
    "name",
    "size",
    "size_bytes",
    "checksum",
    "sha256",
)


def _media_field(value: Any, *names: str) -> Any:
    if isinstance(value, Mapping):
        for name in names:
            if name in value:
                return value[name]
        return None
    for name in names:
        if hasattr(value, name):
            return getattr(value, name)
    return None


def wire_media_fingerprint(value: Any) -> str:
    """Return a deterministic, non-secret digest of a channel media ref.

    The digest is stored instead of the raw CDN query/key material.  Hashing
    every supported alias (rather than selecting the first populated alias)
    prevents a replay from changing a secondary wire reference while keeping
    the same canonical value.
    """

    if isinstance(value, Mapping):
        fields = {name: value.get(name) for name in WIRE_MEDIA_FIELDS}
    else:
        fields = {name: _media_field(value, name) for name in WIRE_MEDIA_FIELDS}
        if not any(item is not None for item in fields.values()):
            # Preserve the old non-mapping behavior for opaque protocol
            # objects whose attributes cannot be introspected.
            fields = {"value": str(value or "")}
    encoded = json.dumps(
        fields,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def canonical_media_input(
    value: Any,
    *,
    include_error: bool = True,
    include_candidate: bool = False,
) -> dict[str, Any]:
    """Return a redacted, JSON-safe media snapshot for Agent execution.

    Channel adapters may retain opaque wire diagnostics in the durable
    inbound envelope, but task inputs and SDK prompts must contain only this
    canonical representation.  Unknown fields are intentionally dropped
    rather than copied recursively, which prevents a nested ``metadata``
    object from smuggling encrypted CDN credentials into a prompt.
    """

    # A bare string at a media boundary is an opaque managed-ID reference, not
    # user prose.  Keep it structured and unavailable until an authoritative
    # store row resolves it.
    if isinstance(value, str):
        value = {"attachment_id": value, "kind": "file"}
    kind = str(_media_field(value, "kind", "type", "media_kind") or "file").strip().lower()
    if kind in {"localimage", "local_image"}:
        kind = "image"
    mime = str(_media_field(value, "mime_type", "mime", "content_type") or "").strip().lower()
    filename = str(_media_field(value, "filename", "file_name", "name") or "").strip()
    # Never preserve path components supplied by a channel/user.
    if filename:
        filename = Path(filename).name
    attachment_id = str(_media_field(value, "attachment_id", "id") or "").strip()
    path = str(_media_field(value, "path", "local_path", "file_path") or "").strip()
    size_value = _media_field(value, "size", "size_bytes")
    try:
        size = max(0, int(size_value or 0))
    except (TypeError, ValueError):
        size = 0
    checksum = str(_media_field(value, "checksum", "sha256") or "").strip().lower()
    explicit_available = _media_field(value, "available")
    authoritative = isinstance(value, (StoredAttachment, RuntimeMediaInput)) or (
        explicit_available is True and bool(attachment_id and path and checksum)
    )
    result: dict[str, Any] = {
        "kind": kind,
        "mime_type": mime or "application/octet-stream",
        "filename": filename,
        "size": size,
        "checksum": checksum,
        "available": bool(authoritative),
        "native_input_available": bool(
            kind == "image"
            and mime.startswith("image/")
            and authoritative
            and _media_field(value, "native_input_available") is not False
        ),
    }
    if attachment_id:
        result["attachment_id"] = attachment_id
    if path and authoritative:
        result["path"] = path
    candidate_text = _media_field(value, "candidate_text")
    # Candidate text is a legacy compatibility field. Ordinary channel task
    # inputs intentionally omit it; direct voice ingress uses the normalized
    # envelope text as its instruction instead.
    if include_candidate and kind == "audio" and candidate_text is not None:
        result["candidate_text"] = str(candidate_text)
    confidence = _media_field(value, "confidence")
    if confidence is not None:
        try:
            result["confidence"] = float(confidence)
        except (TypeError, ValueError):
            pass
    if include_error:
        error = _media_field(value, "error", "media_error")
        if error:
            result["error"] = str(error)[:500]
    return result


def canonical_media_inputs(
    values: Any,
    *,
    include_candidate: bool = False,
    include_error: bool = True,
) -> tuple[dict[str, Any], ...]:
    """Normalize a singleton/iterable media value without leaking fields."""

    if values is None:
        return ()
    if isinstance(values, (Mapping, str, bytes, bytearray)):
        values = (values,)
    try:
        return tuple(
            canonical_media_input(
                item,
                include_candidate=include_candidate,
                include_error=include_error,
            )
            for item in values
        )
    except TypeError:
        return (
            canonical_media_input(
                values,
                include_candidate=include_candidate,
                include_error=include_error,
            ),
        )


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sniff_mime(data: bytes, filename: str = "", declared: str = "") -> str:
    """Return a conservative MIME type using magic bytes then filename.

    This intentionally does not trust a channel-provided MIME value on its
    own.  An image filename is also insufficient evidence for native image
    input: unknown bytes named ``*.png`` remain ``application/octet-stream``.
    Other filename guesses remain useful as structured file metadata.
    """

    signatures = (
        (b"\x89PNG\r\n\x1a\n", "image/png"),
        (b"\xff\xd8\xff", "image/jpeg"),
        (b"GIF87a", "image/gif"),
        (b"GIF89a", "image/gif"),
        (b"RIFF", "application/octet-stream"),
        (b"%PDF-", "application/pdf"),
        (b"PK\x03\x04", "application/zip"),
        (b"\x1f\x8b", "application/gzip"),
        (b"ID3", "audio/mpeg"),
        (b"OggS", "audio/ogg"),
        (b"\x00\x00\x00\x18ftyp", "video/mp4"),
        (b"\x00\x00\x00\x20ftyp", "video/mp4"),
    )
    for prefix, mime in signatures:
        if data.startswith(prefix):
            # RIFF has several formats.  Refine common ones without trusting
            # the rest of an unbounded header.
            if prefix == b"RIFF" and len(data) >= 12:
                if data[8:12] == b"WAVE":
                    return "audio/wav"
                if data[8:12] == b"AVI ":
                    return "video/x-msvideo"
                if data[8:12] == b"WEBP":
                    return "image/webp"
            return mime
    guessed, _ = mimetypes.guess_type(filename or "")
    if guessed:
        # A filename can label an otherwise opaque file, but it must not make
        # arbitrary bytes eligible for a native SDK image input.
        if guessed.lower().startswith("image/"):
            return "application/octet-stream"
        return guessed
    # ``declared`` is deliberately only a compatibility input.  A channel
    # declaration is not evidence that arbitrary bytes are a native
    # image/audio/video, and promoting it here would bypass the magic/filename
    # checks above.  Unknown bytes remain opaque structured media.
    return "application/octet-stream"


class AttachmentStore:
    """Store immutable files below a managed root.

    The class is safe to use from the asyncio loop: the public async methods
    perform file I/O in worker threads.  Synchronous counterparts are useful
    to channel download code that already runs in a worker thread.
    """

    def __init__(
        self,
        root: str | os.PathLike[str],
        *,
        max_file_size: int = 50 * 1024 * 1024,
        quota_bytes: int = 512 * 1024 * 1024,
        reference_checker: Callable[[str], bool | Awaitable[bool]] | None = None,
    ) -> None:
        if int(max_file_size) <= 0:
            raise ValueError("max_file_size must be positive")
        if int(quota_bytes) <= 0:
            raise ValueError("quota_bytes must be positive")
        self.root = Path(root).expanduser().resolve()
        self.max_file_size = int(max_file_size)
        self.quota_bytes = int(quota_bytes)
        self.reference_checker = reference_checker
        self.root.mkdir(parents=True, exist_ok=True)
        # All stores for the same root share this lock. Channel ingress and the
        # media worker may hold separate AttachmentStore instances, but their
        # quota check and publication still need one process-local boundary.
        self._lock = _attachment_root_lock(self.root)
        self._metadata: dict[str, StoredAttachment] = {}
        # Keep the complete logical reference identity.  Two projections may
        # point at the same attachment under different roles/ordinals; a
        # cleanup must retain the file until every one is removed.
        # Reference identity mirrors the SQLite ``attachment_refs`` primary
        # key.  Keep it as a set: replaying the same projection must be
        # idempotent and must not require multiple compensating removals
        # before cleanup can reclaim a file.
        self._refs: set[tuple[str, str, str, str, int]] = set()

    def _safe_path(self, attachment_id: str) -> Path:
        if not attachment_id or attachment_id in {".", ".."}:
            raise AttachmentError("invalid attachment id")
        # IDs are generated by us, but reject path syntax even if a caller
        # supplies an ID from an external record.
        if Path(attachment_id).name != attachment_id or os.sep in attachment_id:
            raise AttachmentError("attachment id must be a single path component")
        candidate = self.root / attachment_id
        if candidate.is_symlink():
            raise AttachmentError("attachment path may not be a symlink")
        path = candidate.resolve()
        try:
            path.relative_to(self.root)
        except ValueError as exc:
            raise AttachmentError("attachment path escapes managed root") from exc
        return path

    def _used_bytes(self) -> int:
        total = 0
        paths: set[Path] = {item.local_path for item in self._metadata.values()}
        # Include files left by a previous process even when metadata is
        # loaded from SQLite lazily.  Temporary dot-files are ignored.
        try:
            paths.update(path for path in self.root.iterdir() if not path.name.startswith("."))
        except OSError:
            pass
        for path in paths:
            try:
                if path.is_file() and not path.is_symlink():
                    total += path.stat().st_size
            except FileNotFoundError:
                pass
        return total

    def put_bytes(
        self,
        data: bytes,
        *,
        filename: str = "",
        mime_type: str = "",
        attachment_id: str | None = None,
    ) -> StoredAttachment:
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError("attachment data must be bytes-like")
        payload = bytes(data)
        if len(payload) > self.max_file_size:
            raise AttachmentError("attachment exceeds maximum file size")
        with self._lock:
            aid = attachment_id or secrets.token_hex(16)
            path = self._safe_path(aid)
            if path.exists() or aid in self._metadata:
                raise AttachmentError(f"attachment already exists: {aid}")
            # Check identity before quota.  Deterministic ingress retries need
            # to discover an existing publication and verify it without being
            # rejected merely because that publication already fills quota.
            if self._used_bytes() + len(payload) > self.quota_bytes:
                raise AttachmentError("attachment quota exceeded")
            digest = hashlib.sha256(payload).hexdigest()
            # Write in the same directory and atomically publish.  A closed
            # descriptor is always cleaned up if a write or replace fails.
            fd, temporary = tempfile.mkstemp(prefix=f".{aid}.", dir=self.root)
            try:
                with os.fdopen(fd, "wb") as handle:
                    handle.write(payload)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.chmod(temporary, 0o600)
                # Publish without replacement. Two processes can race even
                # though same-process stores share the root lock; an immutable
                # attachment ID must never let the later writer replace bytes
                # that an earlier writer may already have registered in SQLite.
                try:
                    os.link(temporary, path)
                except FileExistsError as exc:
                    raise AttachmentError(
                        f"attachment already exists: {aid}"
                    ) from exc
            finally:
                try:
                    os.unlink(temporary)
                except FileNotFoundError:
                    pass
            record = StoredAttachment(
                attachment_id=aid,
                path=str(path),
                mime_type=sniff_mime(payload, filename, mime_type),
                size=len(payload),
                checksum=digest,
                filename=Path(filename).name if filename else "",
                created_at=_now(),
            )
            self._metadata[aid] = record
            return record

    def put_bytes_idempotent(
        self,
        data: bytes,
        *,
        filename: str = "",
        mime_type: str = "",
        attachment_id: str,
    ) -> StoredAttachment:
        """Publish deterministic content or verify an existing publication.

        Inbound channel redelivery uses a stable attachment ID.  A process can
        therefore crash after the atomic file replace but before SQLite
        registration.  Retrying is safe only when the already-published bytes
        match the redownloaded payload; never adopt an existing file by
        recomputing its checksum from that file alone.
        """

        payload = bytes(data)
        expected_checksum = hashlib.sha256(payload).hexdigest()
        try:
            return self.put_bytes(
                payload,
                filename=filename,
                mime_type=mime_type,
                attachment_id=attachment_id,
            )
        except AttachmentError as exc:
            if "already exists" not in str(exc):
                raise
        path = self._safe_path(attachment_id)
        if path.is_symlink() or not path.is_file():
            raise AttachmentError("existing attachment path is not a managed file")
        existing = path.read_bytes()
        if len(existing) != len(payload) or hashlib.sha256(existing).hexdigest() != expected_checksum:
            raise AttachmentError("existing attachment content conflicts with redelivery")
        record = StoredAttachment(
            attachment_id=attachment_id,
            path=str(path),
            mime_type=sniff_mime(payload, filename, mime_type),
            size=len(payload),
            checksum=expected_checksum,
            filename=Path(filename).name if filename else "",
            created_at=_now(),
        )
        with self._lock:
            cached = self._metadata.get(attachment_id)
            if cached is not None and (
                cached.path != record.path
                or cached.size != record.size
                or cached.checksum != record.checksum
            ):
                raise AttachmentError("attachment metadata conflicts with redelivery")
            self._metadata[attachment_id] = cached or record
            return cached or record

    async def aput_bytes_idempotent(self, data: bytes, **kwargs: Any) -> StoredAttachment:
        return await asyncio.to_thread(self.put_bytes_idempotent, data, **kwargs)

    def put_file(
        self,
        source: str | os.PathLike[str] | BinaryIO,
        *,
        filename: str = "",
        mime_type: str = "",
        attachment_id: str | None = None,
    ) -> StoredAttachment:
        if hasattr(source, "read"):
            data = source.read()
        else:
            data = Path(source).read_bytes()
        return self.put_bytes(
            data,
            filename=filename or (Path(source).name if isinstance(source, (str, os.PathLike)) else ""),
            mime_type=mime_type,
            attachment_id=attachment_id,
        )

    # Friendly aliases used by channel adapters and callers.
    put = put_bytes
    store = put_bytes

    async def aput_bytes(self, data: bytes, **kwargs: Any) -> StoredAttachment:
        return await asyncio.to_thread(self.put_bytes, data, **kwargs)

    async def aput_file(self, source: Any, **kwargs: Any) -> StoredAttachment:
        return await asyncio.to_thread(self.put_file, source, **kwargs)

    def remember(self, attachment: StoredAttachment | Mapping[str, Any] | Any) -> StoredAttachment:
        """Load authoritative metadata without deriving it from local bytes.

        SQLite callers use this after restart so :meth:`read_bytes` compares
        the current file with the checksum committed when it was accepted.
        """

        if isinstance(attachment, StoredAttachment):
            item = attachment
        else:
            getter = attachment.get if isinstance(attachment, Mapping) else None

            def value(name: str, default: Any = "") -> Any:
                if getter is not None:
                    return getter(name, default)
                return getattr(attachment, name, default)

            item = StoredAttachment(
                attachment_id=str(value("attachment_id") or ""),
                path=str(value("path") or ""),
                mime_type=str(value("mime_type", "application/octet-stream") or "application/octet-stream"),
                size=int(value("size", value("size_bytes", 0)) or 0),
                checksum=str(value("checksum") or ""),
                filename=Path(str(value("filename") or "")).name,
                created_at=str(value("created_at") or ""),
            )
        path = self._safe_path(item.attachment_id)
        if not item.path or path != Path(item.path).expanduser().resolve(strict=False):
            raise AttachmentError("attachment path is not managed")
        if not item.checksum:
            raise AttachmentError("authoritative attachment checksum is required")
        if item.size < 0:
            raise AttachmentError("attachment size is invalid")
        with self._lock:
            existing = self._metadata.get(item.attachment_id)
            if existing is not None and existing != item:
                raise AttachmentError("attachment metadata is immutable")
            self._metadata[item.attachment_id] = item
        return item

    load_metadata = remember

    def get(self, attachment_id: str) -> StoredAttachment:
        with self._lock:
            item = self._metadata.get(attachment_id)
            if item is None:
                # Reconstruct only when the ID is safe.  Metadata is normally
                # loaded by the SQLite store.  A store configured with durable
                # reference checking must fail closed here: reconstructing a
                # checksum from possibly tampered bytes would make those bytes
                # appear authoritative after a restart.
                if self.reference_checker is not None:
                    raise AttachmentError("authoritative attachment metadata is unavailable")
                # Standalone use retains the conservative path-safe fallback.
                path = self._safe_path(attachment_id)
                if not path.is_file() or path.is_symlink():
                    raise AttachmentError("attachment not found")
                payload = path.read_bytes()
                item = StoredAttachment(
                    attachment_id=attachment_id,
                    path=str(path),
                    mime_type=sniff_mime(payload),
                    size=len(payload),
                    checksum=hashlib.sha256(payload).hexdigest(),
                    created_at="",
                )
                self._metadata[attachment_id] = item
            path = self._safe_path(item.attachment_id)
            if path.is_symlink() or path != Path(item.path).resolve():
                raise AttachmentError("attachment path is not managed")
            if not path.is_file():
                raise AttachmentError("attachment file is missing")
            return item

    async def aget(self, attachment_id: str) -> StoredAttachment:
        return await asyncio.to_thread(self.get, attachment_id)

    def read_bytes(self, attachment_id: str) -> bytes:
        item = self.get(attachment_id)
        data = Path(item.path).read_bytes()
        if len(data) != item.size:
            raise AttachmentError("attachment size mismatch")
        if hashlib.sha256(data).hexdigest() != item.checksum:
            raise AttachmentError("attachment checksum mismatch")
        return data

    async def aread_bytes(self, attachment_id: str) -> bytes:
        return await asyncio.to_thread(self.read_bytes, attachment_id)

    def add_ref(self, owner_kind: str, owner_id: str, attachment_id: str, role: str = "", ordinal: int = 0) -> None:
        self.get(attachment_id)
        with self._lock:
            key = (owner_kind, owner_id, attachment_id, role, int(ordinal))
            self._refs.add(key)

    def remove_ref(self, owner_kind: str, owner_id: str, attachment_id: str, role: str = "", ordinal: int = 0) -> None:
        with self._lock:
            key = (owner_kind, owner_id, attachment_id, role, int(ordinal))
            self._refs.discard(key)

    def cleanup(self, *, attachment_id: str | None = None) -> list[str]:
        """Delete files using process-local references only.

        A store configured with a durable reference checker must use
        :meth:`acleanup`; calling this synchronous compatibility API would skip
        the authoritative SQLite check and is therefore rejected.
        """

        if self.reference_checker is not None:
            raise AttachmentError(
                "durable reference checking requires async attachment cleanup"
            )
        return self._cleanup_local(attachment_id=attachment_id)

    def _candidate_ids(self, attachment_id: str | None = None) -> list[str]:
        """Return known metadata IDs plus files left by an earlier process."""

        if attachment_id:
            return [attachment_id]
        with self._lock:
            candidates = set(self._metadata)
        try:
            candidates.update(
                path.name
                for path in self.root.iterdir()
                if not path.name.startswith(".") and path.is_file()
            )
        except OSError:
            pass
        return sorted(candidates)

    def _cleanup_local(self, *, attachment_id: str | None = None) -> list[str]:
        """Delete candidates after the caller has checked durable references."""

        deleted: list[str] = []
        with self._lock:
            candidates = self._candidate_ids(attachment_id)
            for aid in candidates:
                if not aid or any(key[2] == aid for key in self._refs):
                    continue
                item = self._metadata.get(aid)
                path = self._safe_path(aid)
                if item is not None and path != Path(item.path).resolve():
                    raise AttachmentError("attachment path is not managed")
                existed = path.is_file()
                if item is None and not existed:
                    continue
                try:
                    path.unlink()
                except FileNotFoundError:
                    pass
                self._metadata.pop(aid, None)
                deleted.append(aid)
        return deleted

    async def acleanup(self, *, attachment_id: str | None = None) -> list[str]:
        """Delete files that have neither local nor durable references.

        ``reference_checker`` is normally ``SQLiteStore.attachment_referenced``.
        It is awaited on the event loop before filesystem work is delegated to
        a worker thread.  Checker errors propagate so cleanup fails closed.
        """

        candidates = self._candidate_ids(attachment_id)
        deleted: list[str] = []
        # SQLiteStore exposes an atomic cleanup reservation when its bound
        # ``attachment_referenced`` method is supplied as the checker.  Use
        # that capability when available; a plain callback keeps the legacy
        # best-effort path below for backend compatibility.
        checker_owner = getattr(self.reference_checker, "__self__", None)
        claim_cleanup = getattr(checker_owner, "claim_attachment_cleanup", None)
        finish_cleanup = getattr(checker_owner, "finish_attachment_cleanup", None)
        for aid in candidates:
            if not aid:
                continue
            with self._lock:
                if any(key[2] == aid for key in self._refs):
                    continue
            if claim_cleanup is not None and finish_cleanup is not None:
                try:
                    reserved = claim_cleanup(aid, allow_orphan=True)
                except TypeError:
                    # Compatibility backend with an older reservation shape.
                    reserved = claim_cleanup(aid)
                if inspect.isawaitable(reserved):
                    reserved = await reserved
                if not reserved:
                    continue
                try:
                    removed = await asyncio.to_thread(
                        self._cleanup_local,
                        attachment_id=aid,
                    )
                except BaseException:
                    released = finish_cleanup(aid, deleted=False)
                    if inspect.isawaitable(released):
                        await released
                    raise
                released = finish_cleanup(aid, deleted=bool(removed))
                if inspect.isawaitable(released):
                    released = await released
                if not released:
                    # The reservation was lost (for example, recovery ran in
                    # another process).  Do not report a deletion whose
                    # durable state cannot be reconciled.
                    raise AttachmentError(
                        f"attachment cleanup lease was lost: {aid}"
                    )
                deleted.extend(removed)
                continue
            if self.reference_checker is not None:
                referenced = self.reference_checker(aid)
                if inspect.isawaitable(referenced):
                    referenced = await referenced
                if referenced:
                    continue
                # Re-check immediately before the unlink for callback-backed
                # stores that do not expose an atomic reservation.  This does
                # not claim the stronger SQLite path above, but closes the
                # common check/retry window for simple backends.
                referenced = self.reference_checker(aid)
                if inspect.isawaitable(referenced):
                    referenced = await referenced
                if referenced:
                    continue
            removed = await asyncio.to_thread(
                self._cleanup_local,
                attachment_id=aid,
            )
            deleted.extend(removed)
        return deleted

    cleanup_async = acleanup


class ManagedImageOutputPublisher:
    """Promote one trusted runtime image output into durable managed media.

    Codex image-generation items contain either an absolute ``savedPath`` or a
    ``data:image/...;base64`` result.  Neither value is a durable attachment:
    paths can disappear after the turn and embedding the data URL in an Agent
    event would put binary data in SQLite.  This publisher copies verified
    image bytes into :class:`AttachmentStore`, registers their immutable
    metadata, and returns only the managed attachment ID.

    The attachment ID is derived from the immutable task execution and SDK
    item identity.  A crash after filesystem publication but before SQLite
    registration is therefore repaired idempotently on replay, while changed
    bytes under the same item identity are rejected.
    """

    _IMAGE_MIME_TYPES = frozenset(
        {"image/png", "image/jpeg", "image/gif", "image/webp"}
    )

    def __init__(
        self,
        attachment_store: AttachmentStore,
        metadata_store: Any,
        *,
        workspace_root: str | os.PathLike[str],
    ) -> None:
        self.attachment_store = attachment_store
        self.metadata_store = metadata_store
        self.workspace_root = Path(workspace_root).expanduser().resolve()
        try:
            workspace_details = self.workspace_root.stat()
        except OSError as exc:
            raise AttachmentError("generated-image workspace is unavailable") from exc
        if not stat.S_ISDIR(workspace_details.st_mode):
            raise AttachmentError("generated-image workspace is not a directory")
        self._workspace_identity = (
            workspace_details.st_dev,
            workspace_details.st_ino,
        )

    @staticmethod
    def _field(value: Any, name: str, default: Any = None) -> Any:
        if isinstance(value, Mapping):
            return value.get(name, default)
        return getattr(value, name, default)

    @classmethod
    def attachment_id(
        cls,
        task: Any,
        *,
        source_item_id: str = "",
        source_item_ordinal: int | None = None,
    ) -> str:
        """Return the stable managed identity for one completed SDK item."""

        task_id = str(cls._field(task, "task_id", "") or "").strip()
        execution_id = str(cls._field(task, "execution_id", "") or "").strip()
        item_id = str(source_item_id or "").strip()
        if not task_id or not execution_id:
            raise AttachmentError(
                "generated image requires a durable task execution identity"
            )
        if not item_id and source_item_ordinal is None:
            raise AttachmentError(
                "generated image requires an SDK item identity or ordinal"
            )
        item_identity = item_id or f"ordinal:{int(source_item_ordinal)}"
        identity = "\x1f".join((task_id, execution_id, item_identity))
        return "codex-image-" + uuid.uuid5(
            uuid.NAMESPACE_URL,
            "codex-wechat:generated-image:" + identity,
        ).hex

    def _decode_data_image(self, value: str) -> tuple[bytes, str] | None:
        raw = str(value or "").strip()
        if not raw.lower().startswith("data:image/"):
            return None
        header, separator, encoded = raw.partition(",")
        if not separator or not header.lower().endswith(";base64"):
            raise AttachmentError("generated image data URL must use base64 encoding")
        mime_type = header[5:-7].strip().lower()
        if mime_type == "image/jpg":
            mime_type = "image/jpeg"
        if mime_type not in self._IMAGE_MIME_TYPES:
            raise AttachmentError(
                f"unsupported generated image MIME type: {mime_type or 'unknown'}"
            )
        # Reject an oversized encoded value before allocating its decoded form.
        encoded_limit = ((self.attachment_store.max_file_size + 2) // 3) * 4
        if len(encoded) > encoded_limit + 4:
            raise AttachmentError("attachment exceeds maximum file size")
        try:
            payload = base64.b64decode(encoded, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise AttachmentError("generated image data URL is invalid") from exc
        if len(payload) > self.attachment_store.max_file_size:
            raise AttachmentError("attachment exceeds maximum file size")
        return payload, mime_type

    def _read_saved_image(self, value: str) -> tuple[bytes, str]:
        """Read a regular file beneath the workspace without following links.

        Resolve-by-name followed by a second pathname ``open`` has a classic
        check/use race: a writable intermediate directory can be swapped for
        a symlink after containment validation.  Walk from an already-opened
        workspace descriptor instead, applying ``O_NOFOLLOW`` to every path
        component.  Holding each parent descriptor also keeps renames from
        redirecting the next lookup.
        """

        raw = str(value or "").strip()
        candidate = Path(raw).expanduser()
        if not raw or not candidate.is_absolute():
            raise AttachmentError(
                "generated image savedPath must be an absolute workspace path"
            )
        try:
            # ``abspath`` removes ``.``/``..`` without dereferencing a symlink;
            # every actual lookup is performed through the descriptor walk.
            normalized = Path(os.path.abspath(os.fspath(candidate)))
            relative = normalized.relative_to(self.workspace_root)
        except (OSError, RuntimeError, ValueError) as exc:
            raise AttachmentError(
                "generated image savedPath is outside the configured workspace"
            ) from exc
        if not relative.parts:
            raise AttachmentError("generated image output is not a regular file")

        base_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
        nofollow = getattr(os, "O_NOFOLLOW", 0)
        directory_only = getattr(os, "O_DIRECTORY", 0)
        nonblocking = getattr(os, "O_NONBLOCK", 0)
        if (
            not nofollow
            or not directory_only
            or not nonblocking
            or os.open not in getattr(os, "supports_dir_fd", set())
        ):
            raise AttachmentError(
                "secure generated-image path traversal is unavailable"
            )
        directory_flags = (
            base_flags | nofollow | directory_only | nonblocking
        )
        directory_descriptors: list[int] = []
        descriptor: int | None = None
        try:
            root_descriptor = os.open(self.workspace_root, directory_flags)
            directory_descriptors.append(root_descriptor)
            root_details = os.fstat(root_descriptor)
            if (
                root_details.st_dev,
                root_details.st_ino,
            ) != self._workspace_identity:
                raise AttachmentError("generated-image workspace changed")

            parent_descriptor = root_descriptor
            for component in relative.parts[:-1]:
                next_descriptor = os.open(
                    component,
                    directory_flags,
                    dir_fd=parent_descriptor,
                )
                directory_descriptors.append(next_descriptor)
                parent_descriptor = next_descriptor
            descriptor = os.open(
                relative.parts[-1],
                base_flags | nofollow | nonblocking,
                dir_fd=parent_descriptor,
            )
        except AttachmentError:
            raise
        except (OSError, ValueError) as exc:
            raise AttachmentError("generated image file is unavailable") from exc
        finally:
            for directory_descriptor in reversed(directory_descriptors):
                os.close(directory_descriptor)
        if descriptor is None:
            raise AttachmentError("generated image file is unavailable")
        try:
            before = os.fstat(descriptor)
            if not stat.S_ISREG(before.st_mode):
                raise AttachmentError("generated image output is not a regular file")
            if before.st_size > self.attachment_store.max_file_size:
                raise AttachmentError("attachment exceeds maximum file size")
            chunks: list[bytes] = []
            remaining = self.attachment_store.max_file_size + 1
            while remaining > 0:
                chunk = os.read(descriptor, min(1024 * 1024, remaining))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            payload = b"".join(chunks)
            after = os.fstat(descriptor)
        finally:
            os.close(descriptor)
        if len(payload) > self.attachment_store.max_file_size:
            raise AttachmentError("attachment exceeds maximum file size")
        if (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ) or len(payload) != int(before.st_size):
            raise AttachmentError("generated image changed while it was read")
        return payload, normalized.name

    async def __call__(
        self,
        task: Any,
        *,
        source_item_id: str = "",
        source_item_ordinal: int | None = None,
        saved_path: str = "",
        result: str = "",
    ) -> str:
        attachment_id = self.attachment_id(
            task,
            source_item_id=source_item_id,
            source_item_ordinal=source_item_ordinal,
        )
        target = self._field(task, "reply_target", None)
        route = {
            "channel": str(self._field(target, "channel", "") or ""),
            "bot_id": str(self._field(target, "bot_id", "") or ""),
            "external_user_id": str(
                self._field(target, "external_user_id", "") or ""
            ),
            "session_id": str(
                self._field(target, "session_id", "default") or "default"
            ),
        }
        if not route["channel"] or not route["bot_id"] or not route["external_user_id"]:
            raise AttachmentError("generated image reply target is incomplete")
        agent_id = str(self._field(task, "agent_id", "") or "").strip()
        if not agent_id:
            raise AttachmentError("generated image has no owning Agent")
        decoded = self._decode_data_image(result)
        if decoded is not None:
            payload, declared_mime = decoded
            extension = {
                "image/png": ".png",
                "image/jpeg": ".jpg",
                "image/gif": ".gif",
                "image/webp": ".webp",
            }[declared_mime]
            filename = f"generated-{attachment_id[-12:]}{extension}"
        else:
            payload, filename = await asyncio.to_thread(
                self._read_saved_image, saved_path
            )
            declared_mime = ""

        actual_mime = sniff_mime(payload, filename, declared_mime).lower()
        if actual_mime not in self._IMAGE_MIME_TYPES:
            raise AttachmentError("generated output is not a supported image")
        if declared_mime and actual_mime != declared_mime:
            raise AttachmentError("generated image MIME does not match its bytes")

        getter = getattr(self.metadata_store, "get_attachment", None)
        existing = getter(attachment_id) if getter is not None else None
        if inspect.isawaitable(existing):
            existing = await existing
        if existing is not None:
            # SQLite metadata is authoritative after a restart.  Load it before
            # verifying the replay bytes so a modified managed file cannot be
            # blessed with a newly computed checksum.
            self.attachment_store.remember(existing)
        stored = await self.attachment_store.aput_bytes_idempotent(
            payload,
            filename=filename,
            mime_type=actual_mime,
            attachment_id=attachment_id,
        )
        if existing is not None:
            return stored.attachment_id

        metadata = {
            "filename": stored.filename,
            "owner_agent_id": agent_id,
            **route,
            "task_id": str(self._field(task, "task_id", "") or ""),
            "execution_id": str(self._field(task, "execution_id", "") or ""),
            "source_item_id": str(source_item_id or ""),
            "source_item_ordinal": source_item_ordinal,
        }
        register = getattr(self.metadata_store, "register_attachment", None) or getattr(
            self.metadata_store, "add_attachment", None
        )
        if register is None:
            raise AttachmentError("runtime store cannot register managed attachments")
        registration = register(
            stored,
            kind="image",
            metadata=metadata,
        )
        if inspect.isawaitable(registration):
            await registration
        return stored.attachment_id


# ``MediaStore`` is a useful compatibility name for callers that think in
# terms of media rather than attachments.
MediaStore = AttachmentStore


__all__ = [
    "AttachmentError",
    "AttachmentStore",
    "InboundMediaRef",
    "ManagedImageOutputPublisher",
    "MediaStore",
    "RuntimeMediaInput",
    "StoredAttachment",
    "WIRE_MEDIA_FIELDS",
    "canonical_media_input",
    "canonical_media_inputs",
    "sniff_mime",
    "wire_media_fingerprint",
]
