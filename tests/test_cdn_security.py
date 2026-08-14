"""Security regressions for the WeChat CDN crypto/download boundary."""

from __future__ import annotations

import pytest

from wechat_ilink import cdn


class _Response:
    def __init__(self, chunks: list[bytes], *, content_length: int | None = None):
        self._chunks = chunks
        self.headers = (
            {"Content-Length": str(content_length)}
            if content_length is not None
            else {}
        )
        self.closed = False

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, *, chunk_size: int):
        assert chunk_size > 0
        yield from self._chunks

    def close(self) -> None:
        self.closed = True


def test_cdn_upload_closes_response_on_invalid_metadata(monkeypatch):
    response = _Response([])
    monkeypatch.setattr(cdn.requests, "post", lambda *_args, **_kwargs: response)

    with pytest.raises(RuntimeError, match="missing X-Encrypted-Param"):
        cdn._upload_to_cdn(b"ciphertext", "https://cdn.example/upload")

    assert response.closed is True


def test_pkcs7_unpad_rejects_inconsistent_padding():
    with pytest.raises(ValueError, match="invalid PKCS7 padding"):
        cdn._pkcs7_unpad(b"payload\x02")


def test_decrypt_rejects_empty_ciphertext():
    with pytest.raises(ValueError, match="ciphertext is empty"):
        cdn.decrypt_aes_ecb(b"", bytes(range(16)))


@pytest.mark.parametrize(
    "encoded_key",
    ["not-base64!", cdn.aes_key_to_base64("00" * 15), cdn.aes_key_to_base64("zz" * 16)],
)
def test_cdn_download_rejects_invalid_key_before_network(monkeypatch, encoded_key):
    def unexpected_get(*_args, **_kwargs):
        raise AssertionError("invalid key must be rejected before download")

    monkeypatch.setattr(cdn.requests, "get", unexpected_get)

    with pytest.raises(ValueError, match="invalid AES-128 key"):
        cdn.download_file_from_cdn("query", encoded_key)


def test_cdn_download_streams_and_enforces_plaintext_limit(monkeypatch):
    key = bytes(range(16))
    encrypted = cdn.encrypt_aes_ecb(b"hello", key)
    response = _Response([encrypted[:5], encrypted[5:]])
    calls: dict[str, object] = {}

    def fake_get(_url: str, **kwargs):
        calls.update(kwargs)
        return response

    monkeypatch.setattr(cdn.requests, "get", fake_get)
    result = cdn.download_file_from_cdn(
        "query", cdn.aes_key_to_base64(key.hex()), max_size=5
    )

    assert result == b"hello"
    assert calls["stream"] is True
    assert response.closed is True


def test_cdn_download_rejects_oversized_declared_response(monkeypatch):
    response = _Response([], content_length=32)
    monkeypatch.setattr(cdn.requests, "get", lambda _url, **_kwargs: response)

    with pytest.raises(ValueError, match="exceeds attachment size limit"):
        cdn.download_file_from_cdn(
            "query", cdn.aes_key_to_base64(bytes(range(16)).hex()), max_size=8
        )
    assert response.closed is True


def test_cdn_download_rejects_oversized_stream(monkeypatch):
    # No Content-Length: the bound must still be enforced while iterating.
    response = _Response([b"x" * 17, b"y"])
    monkeypatch.setattr(cdn.requests, "get", lambda _url, **_kwargs: response)

    with pytest.raises(ValueError, match="exceeds attachment size limit"):
        cdn.download_file_from_cdn(
            "query", cdn.aes_key_to_base64(bytes(range(16)).hex()), max_size=1
        )
    assert response.closed is True


def test_cdn_download_rejects_truncated_declared_response(monkeypatch):
    key = bytes(range(16))
    encrypted = cdn.encrypt_aes_ecb(b"hello", key)
    response = _Response([encrypted], content_length=len(encrypted) * 2)
    monkeypatch.setattr(cdn.requests, "get", lambda _url, **_kwargs: response)

    with pytest.raises(ValueError, match="does not match Content-Length"):
        cdn.download_file_from_cdn(
            "query", cdn.aes_key_to_base64(key.hex()), max_size=64
        )
    assert response.closed is True


def test_cdn_download_has_a_total_deadline(monkeypatch):
    response = _Response([b"x"])
    monkeypatch.setattr(cdn.requests, "get", lambda _url, **_kwargs: response)
    ticks = iter((0.0, cdn.CDN_TOTAL_TIMEOUT_SECONDS))
    monkeypatch.setattr(cdn.time, "monotonic", lambda: next(ticks))

    with pytest.raises(TimeoutError, match="total timeout"):
        cdn.download_file_from_cdn(
            "query", cdn.aes_key_to_base64(bytes(range(16)).hex()), max_size=1
        )
    assert response.closed is True
