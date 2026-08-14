"""Download message media via WeChat iLink CDN for codex-wechat-bot."""

from __future__ import annotations

import base64
import binascii
import hashlib
import os
import time
from dataclasses import dataclass
from urllib.parse import quote

import requests
from Crypto.Cipher import AES

from .client import Client
from .types import GetUploadURLRequest

CDN_BASE_URL = "https://novac2c.cdn.weixin.qq.com/c2c"

# Keep the protocol helper bounded even when a caller does not provide its
# own attachment-store limit.  The channel adapter passes its configured
# plaintext limit explicitly; this default protects standalone callers too.
DEFAULT_MAX_DOWNLOAD_SIZE = 50 * 1024 * 1024
CDN_CONNECT_TIMEOUT_SECONDS = 10.0
CDN_READ_TIMEOUT_SECONDS = 10.0
CDN_TOTAL_TIMEOUT_SECONDS = 60.0


@dataclass
class UploadedFile:
    """Result of a CDN upload."""

    download_param: str  # encrypted query param for download
    aes_key_hex: str  # hex-encoded AES key
    file_size: int  # plaintext size
    cipher_size: int  # ciphertext size


def _pkcs7_pad(data: bytes, block_size: int = AES.block_size) -> bytes:
    pad_len = block_size - (len(data) % block_size)
    return data + bytes([pad_len]) * pad_len


def _pkcs7_unpad(data: bytes) -> bytes:
    if not data:
        return data
    pad_len = data[-1]
    if pad_len == 0 or pad_len > AES.block_size or pad_len > len(data):
        raise ValueError("invalid PKCS7 padding")
    if data[-pad_len:] != bytes([pad_len]) * pad_len:
        raise ValueError("invalid PKCS7 padding")
    return data[:-pad_len]


def _aes_ecb_padded_size(plaintext_len: int) -> int:
    """Ciphertext size after PKCS7 padding (always adds 1..block_size bytes)."""
    pad_len = AES.block_size - (plaintext_len % AES.block_size)
    return plaintext_len + pad_len


def encrypt_aes_ecb(plaintext: bytes, key: bytes) -> bytes:
    """Encrypt data using AES-128-ECB with PKCS7 padding."""
    cipher = AES.new(key, AES.MODE_ECB)
    return cipher.encrypt(_pkcs7_pad(plaintext))


def decrypt_aes_ecb(ciphertext: bytes, key: bytes) -> bytes:
    """Decrypt AES-128-ECB data and remove PKCS7 padding."""
    if not ciphertext:
        raise ValueError("ciphertext is empty")
    if len(ciphertext) % AES.block_size != 0:
        raise ValueError("ciphertext is not a multiple of block size")
    cipher = AES.new(key, AES.MODE_ECB)
    return _pkcs7_unpad(cipher.decrypt(ciphertext))


def aes_key_to_base64(hex_key: str) -> str:
    """Convert a hex AES key to base64 format for message items."""
    return base64.b64encode(hex_key.encode()).decode()


def _decode_aes_key(aes_key_base64: str) -> bytes:
    """Decode and validate iLink's base64(hex(AES-128-key)) representation."""

    try:
        encoded_hex = base64.b64decode(aes_key_base64, validate=True)
        hex_key = encoded_hex.decode("ascii")
        if len(hex_key) != 32:
            raise ValueError
        key = bytes.fromhex(hex_key)
    except (binascii.Error, TypeError, UnicodeError, ValueError) as exc:
        raise ValueError("invalid AES-128 key") from exc
    if len(key) != 16:
        raise ValueError("invalid AES-128 key")
    return key


def upload_file_to_cdn(
    client: Client, data: bytes, to_user_id: str, media_type: int
) -> UploadedFile:
    """Encrypt and upload a file to the WeChat CDN."""
    filekey = os.urandom(16)
    aeskey = os.urandom(16)
    filekey_hex = filekey.hex()
    aeskey_hex = aeskey.hex()

    # MD5 is mandated by the CDN wire protocol and is not used as a security
    # primitive.  Mark that explicitly so uploads also work in FIPS builds.
    raw_md5 = hashlib.md5(data, usedforsecurity=False).hexdigest()
    cipher_size = _aes_ecb_padded_size(len(data))

    upload_req = GetUploadURLRequest(
        filekey=filekey_hex,
        media_type=media_type,
        to_user_id=to_user_id,
        rawsize=len(data),
        rawfilemd5=raw_md5,
        filesize=cipher_size,
        no_need_thumb=True,
        aeskey=aeskey_hex,
    )
    upload_resp = client.get_upload_url(upload_req)
    upload_ret = int(getattr(upload_resp, "ret", 0) or 0)
    upload_errcode = int(getattr(upload_resp, "errcode", 0) or 0)
    if upload_ret != 0 or upload_errcode != 0:
        raise RuntimeError(
            "getuploadurl failed: "
            f"ret={upload_ret} errcode={upload_errcode} "
            f"errmsg={getattr(upload_resp, 'errmsg', '')}"
        )

    encrypted = encrypt_aes_ecb(data, aeskey)

    # Prefer the server-provided full URL, fall back to param-based construction.
    cdn_url = (upload_resp.upload_full_url or "").strip()
    if not cdn_url:
        if not upload_resp.upload_param:
            raise RuntimeError(
                "getuploadurl returned no upload URL (need upload_full_url or upload_param)"
            )
        cdn_url = (
            f"{CDN_BASE_URL}/upload?encrypted_query_param={quote(upload_resp.upload_param, safe='')}"
            f"&filekey={quote(filekey_hex, safe='')}"
        )

    download_param = _upload_to_cdn(encrypted, cdn_url)
    return UploadedFile(
        download_param=download_param,
        aes_key_hex=aeskey_hex,
        file_size=len(data),
        cipher_size=cipher_size,
    )


def download_file_from_cdn(
    encrypt_query_param: str,
    aes_key_base64: str,
    *,
    max_size: int = DEFAULT_MAX_DOWNLOAD_SIZE,
) -> bytes:
    """Download and decrypt a bounded file from the WeChat CDN.

    ``requests.Response.content`` eagerly buffers an untrusted response.  CDN
    references are channel-controlled input, so stream the ciphertext and
    reject it before allocating more than the configured plaintext limit (plus
    one PKCS#7 block).  A final plaintext check also covers a response whose
    length is exactly on the ciphertext bound but has non-minimal padding.
    """
    try:
        plaintext_limit = int(max_size)
    except (TypeError, ValueError) as exc:
        raise ValueError("max_size must be a positive integer") from exc
    if plaintext_limit <= 0:
        raise ValueError("max_size must be a positive integer")

    # Validate channel metadata before initiating a potentially large network
    # transfer.  AES.new() would otherwise reject a malformed key only after
    # the complete ciphertext had been downloaded.
    aes_key = _decode_aes_key(aes_key_base64)
    if not str(encrypt_query_param or ""):
        raise ValueError("encrypted query parameter must not be empty")

    download_url = f"{CDN_BASE_URL}/download?encrypted_query_param={quote(encrypt_query_param, safe='')}"
    # PKCS#7 adds between one and one full block of ciphertext bytes.  The
    # response is rejected as soon as either its declared or observed size is
    # beyond that upper bound.
    max_cipher_size = _aes_ecb_padded_size(plaintext_limit)
    started_at = time.monotonic()

    def ensure_deadline() -> None:
        if time.monotonic() - started_at >= CDN_TOTAL_TIMEOUT_SECONDS:
            raise TimeoutError("CDN download exceeded total timeout")

    resp = requests.get(
        download_url,
        timeout=(CDN_CONNECT_TIMEOUT_SECONDS, CDN_READ_TIMEOUT_SECONDS),
        stream=True,
    )
    try:
        ensure_deadline()
        resp.raise_for_status()
        headers = getattr(resp, "headers", {}) or {}
        content_length = headers.get("Content-Length")
        declared_length: int | None = None
        if content_length not in (None, ""):
            try:
                declared_length = int(content_length)
                if declared_length < 0:
                    raise ValueError("invalid CDN Content-Length")
                if declared_length > max_cipher_size:
                    raise ValueError("CDN response exceeds attachment size limit")
            except (TypeError, ValueError) as exc:
                # Preserve our explicit limit error; malformed lengths are
                # rejected rather than treated as an unbounded response.
                if isinstance(exc, ValueError) and str(exc).startswith("CDN response"):
                    raise
                raise ValueError("invalid CDN Content-Length") from exc

        chunks: list[bytes] = []
        total = 0
        iterator = getattr(resp, "iter_content", None)
        if callable(iterator):
            pieces = iterator(chunk_size=64 * 1024)
        else:
            # Compatibility for very small test doubles and non-requests
            # adapters.  Real requests responses always expose iter_content;
            # still enforce the bound before accepting a fallback body.
            pieces = (getattr(resp, "content", b""),)
        for piece in pieces:
            ensure_deadline()
            if not piece:
                continue
            chunk = bytes(piece)
            total += len(chunk)
            if total > max_cipher_size:
                raise ValueError("CDN response exceeds attachment size limit")
            chunks.append(chunk)
        ensure_deadline()
        if declared_length is not None and total != declared_length:
            raise ValueError("CDN response length does not match Content-Length")
        encrypted = b"".join(chunks)
    finally:
        close = getattr(resp, "close", None)
        if callable(close):
            close()

    plaintext = decrypt_aes_ecb(encrypted, aes_key)
    if len(plaintext) > plaintext_limit:
        raise ValueError("CDN plaintext exceeds attachment size limit")
    return plaintext


def _upload_to_cdn(encrypted: bytes, cdn_url: str) -> str:
    resp = requests.post(
        cdn_url,
        data=encrypted,
        headers={"Content-Type": "application/octet-stream"},
        timeout=60,
    )
    try:
        resp.raise_for_status()
        download_param = resp.headers.get("X-Encrypted-Param")
        if not download_param:
            raise RuntimeError("CDN upload: missing X-Encrypted-Param header")
        return download_param
    finally:
        close = getattr(resp, "close", None)
        if callable(close):
            close()
