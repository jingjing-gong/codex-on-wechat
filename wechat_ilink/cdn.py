"""Download message media via WeChat iLink CDN for codex-wechat-bot."""

from __future__ import annotations

import base64
import hashlib
import os
from dataclasses import dataclass
from urllib.parse import quote

import requests
from Crypto.Cipher import AES

from .client import Client
from .types import GetUploadURLRequest

CDN_BASE_URL = "https://novac2c.cdn.weixin.qq.com/c2c"


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
    if pad_len == 0 or pad_len > AES.block_size:
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
    if len(ciphertext) % AES.block_size != 0:
        raise ValueError("ciphertext is not a multiple of block size")
    cipher = AES.new(key, AES.MODE_ECB)
    return _pkcs7_unpad(cipher.decrypt(ciphertext))


def aes_key_to_base64(hex_key: str) -> str:
    """Convert a hex AES key to base64 format for message items."""
    return base64.b64encode(hex_key.encode()).decode()


def upload_file_to_cdn(
    client: Client, data: bytes, to_user_id: str, media_type: int
) -> UploadedFile:
    """Encrypt and upload a file to the WeChat CDN."""
    filekey = os.urandom(16)
    aeskey = os.urandom(16)
    filekey_hex = filekey.hex()
    aeskey_hex = aeskey.hex()

    raw_md5 = hashlib.md5(data).hexdigest()
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
    if upload_resp.ret != 0:
        raise RuntimeError(
            f"getuploadurl failed: ret={upload_resp.ret} errmsg={upload_resp.errmsg}"
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


def download_file_from_cdn(encrypt_query_param: str, aes_key_base64: str) -> bytes:
    """Download and decrypt a file from the WeChat CDN."""
    # AES key is stored as: base64(hex-string) -> decode to hex string -> decode to raw bytes.
    aes_key_hex_bytes = base64.b64decode(aes_key_base64)
    aes_key = bytes.fromhex(aes_key_hex_bytes.decode())

    download_url = f"{CDN_BASE_URL}/download?encrypted_query_param={quote(encrypt_query_param, safe='')}"
    resp = requests.get(download_url, timeout=60)
    resp.raise_for_status()
    return decrypt_aes_ecb(resp.content, aes_key)


def _upload_to_cdn(encrypted: bytes, cdn_url: str) -> str:
    resp = requests.post(
        cdn_url,
        data=encrypted,
        headers={"Content-Type": "application/octet-stream"},
        timeout=60,
    )
    resp.raise_for_status()
    download_param = resp.headers.get("X-Encrypted-Param")
    if not download_param:
        raise RuntimeError("CDN upload: missing X-Encrypted-Param header")
    return download_param
