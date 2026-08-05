"""wechat_ilink support for codex-wechat-bot.

This talks to WeChat through the unofficial, reverse-engineered iLink bot
protocol (`ilinkai.weixin.qq.com`). It logs a personal WeChat account in as a
"bot" via QR-code scan, then long-polls for messages and sends replies
(including encrypted media) over plain HTTPS.

For personal/educational use only — this is not an official WeChat API.
"""

from .auth import (
    accounts_dir,
    credentials_path,
    delete_all_credentials,
    fetch_qrcode,
    load_all_credentials,
    normalize_account_id,
    poll_qr_status,
    save_credentials,
)
from .cdn import (
    UploadedFile,
    aes_key_to_base64,
    decrypt_aes_ecb,
    download_file_from_cdn,
    encrypt_aes_ecb,
    upload_file_to_cdn,
)
from .client import Client, ILinkError
from .markdown import extract_image_urls, markdown_to_plain_text
from .monitor import Monitor, format_message_summary
from .sender import new_client_id, send_text_reply, send_typing_state
from .types import (
    Credentials,
    ImageItem,
    MessageItem,
    QRCodeResponse,
    QRStatusResponse,
    TextItem,
    WeixinMessage,
)

__all__ = [
    "Client",
    "ILinkError",
    "Credentials",
    "QRCodeResponse",
    "QRStatusResponse",
    "WeixinMessage",
    "MessageItem",
    "TextItem",
    "ImageItem",
    "Monitor",
    "format_message_summary",
    "fetch_qrcode",
    "poll_qr_status",
    "save_credentials",
    "load_all_credentials",
    "delete_all_credentials",
    "accounts_dir",
    "credentials_path",
    "normalize_account_id",
    "send_text_reply",
    "send_typing_state",
    "new_client_id",
    "markdown_to_plain_text",
    "extract_image_urls",
    "upload_file_to_cdn",
    "download_file_from_cdn",
    "encrypt_aes_ecb",
    "decrypt_aes_ecb",
    "aes_key_to_base64",
    "UploadedFile",
]
