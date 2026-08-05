"""Minimal example: log in via QR code, then echo back any text message received.

Usage:
    pip install -e .
    python examples/echo_bot.py
"""

from __future__ import annotations

import logging
import threading
from pathlib import Path

import qrcode

from wechat_ilink import (
    Client,
    Monitor,
    fetch_qrcode,
    format_message_summary,
    load_all_credentials,
    poll_qr_status,
    save_credentials,
    send_text_reply,
)
from wechat_ilink.types import (
    ITEM_TYPE_TEXT,
    MESSAGE_STATE_FINISH,
    MESSAGE_TYPE_USER,
    WeixinMessage,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)


def _render_qrcode(login_url: str) -> None:
    """Print an ASCII QR code to the terminal and save a PNG for viewing."""
    qr_obj = qrcode.QRCode(border=1)
    qr_obj.add_data(login_url)
    qr_obj.make(fit=True)
    qr_obj.print_ascii(invert=True)

    img_path = Path(__file__).parent / "login_qrcode.png"
    qr_obj.make_image().save(img_path)
    print(f"QR code image saved to: {img_path}")


def login() -> Client:
    existing = load_all_credentials()
    if existing:
        print(f"using saved credentials for {existing[0].ilink_user_id}")
        return Client(existing[0])

    qr = fetch_qrcode()
    print("scan this QR code with WeChat to log in:")
    # qrcode_img_content is the actual login URL to encode as a QR image;
    # qr.qrcode is just the opaque token used for polling status below.
    _render_qrcode(qr.qrcode_img_content)

    creds = poll_qr_status(qr.qrcode, on_status=lambda s: print(f"status: {s}"))
    save_credentials(creds)
    print("login successful, credentials saved")
    return Client(creds)


def handle_message(client: Client, msg: WeixinMessage) -> None:
    print(format_message_summary(msg))
    if (
        msg.message_type != MESSAGE_TYPE_USER
        or msg.message_state != MESSAGE_STATE_FINISH
    ):
        return

    text = ""
    for item in msg.item_list:
        if item.type == ITEM_TYPE_TEXT and item.text_item:
            text = item.text_item.text
            break
    if not text:
        return

    send_text_reply(client, msg.from_user_id, f"echo: {text}", msg.context_token)


def main() -> None:
    client = login()
    monitor = Monitor(client, handle_message)
    stop_event = threading.Event()
    try:
        monitor.run(stop_event)
    except KeyboardInterrupt:
        stop_event.set()


if __name__ == "__main__":
    main()
