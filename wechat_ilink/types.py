"""Pydantic models for the WeChat iLink API used by codex-wechat-bot.

Field aliases preserve the upstream JSON shape so payloads round-trip without
extra mapping.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

# Message types
MESSAGE_TYPE_NONE = 0
MESSAGE_TYPE_USER = 1
MESSAGE_TYPE_BOT = 2

# Message states
MESSAGE_STATE_NEW = 0
MESSAGE_STATE_GENERATING = 1
MESSAGE_STATE_FINISH = 2

# Item types
ITEM_TYPE_NONE = 0
ITEM_TYPE_TEXT = 1
ITEM_TYPE_IMAGE = 2
ITEM_TYPE_VOICE = 3
ITEM_TYPE_FILE = 4
ITEM_TYPE_VIDEO = 5

# CDN media types
CDN_MEDIA_TYPE_IMAGE = 1
CDN_MEDIA_TYPE_VIDEO = 2
CDN_MEDIA_TYPE_FILE = 3

# Typing status
TYPING_STATUS_TYPING = 1
TYPING_STATUS_CANCEL = 2


class ILinkModel(BaseModel):
    """Base model: ignores unknown fields from the API instead of erroring."""

    model_config = ConfigDict(extra="ignore")


class BaseInfo(ILinkModel):
    channel_version: str | None = None


class Credentials(ILinkModel):
    """Login session data persisted in the local codex-wechat-bot account store."""

    bot_token: str = ""
    ilink_bot_id: str = ""
    baseurl: str = ""
    ilink_user_id: str = ""


class QRCodeResponse(ILinkModel):
    """Response from get_bot_qrcode."""

    ret: int = 0
    errcode: int = 0
    errmsg: str = ""
    qrcode: str = ""
    qrcode_img_content: str = ""


class QRStatusResponse(ILinkModel):
    """Response from get_qrcode_status."""

    ret: int = 0
    errcode: int = 0
    errmsg: str = ""
    status: str = ""
    bot_token: str = ""
    ilink_bot_id: str = ""
    baseurl: str = ""
    ilink_user_id: str = ""


class TextItem(ILinkModel):
    text: str = ""


class MediaInfo(ILinkModel):
    """CDN media reference for uploaded/downloaded files."""

    encrypt_query_param: str = ""
    aes_key: str = ""  # base64-encoded
    encrypt_type: int = 0  # 1 = AES-128-ECB


class VoiceItem(ILinkModel):
    media: MediaInfo | None = None
    voice_size: int = 0
    encode_type: int = 0  # 1=pcm 2=adpcm 3=feature 4=speex 5=amr 6=silk 7=mp3
    bits_per_sample: int = 0
    sample_rate: int = 0  # Hz
    playtime: int = 0  # duration in milliseconds
    text: str = ""  # speech-to-text transcription from WeChat


class ImageItem(ILinkModel):
    url: str = ""
    media: MediaInfo | None = None
    mid_size: int = 0  # ciphertext size


class VideoItem(ILinkModel):
    media: MediaInfo | None = None
    video_size: int = 0


class FileItem(ILinkModel):
    media: MediaInfo | None = None
    file_name: str = ""
    len: str = ""  # plaintext size as string


class MessageItem(ILinkModel):
    type: int = ITEM_TYPE_NONE
    text_item: TextItem | None = None
    image_item: ImageItem | None = None
    voice_item: VoiceItem | None = None
    video_item: VideoItem | None = None
    file_item: FileItem | None = None


class WeixinMessage(ILinkModel):
    """A message received from (or destined to) WeChat."""

    seq: int = 0
    message_id: int = 0
    from_user_id: str = ""
    to_user_id: str = ""
    message_type: int = MESSAGE_TYPE_NONE
    message_state: int = MESSAGE_STATE_NEW
    item_list: list[MessageItem] = Field(default_factory=list)
    context_token: str = ""


class GetUpdatesRequest(ILinkModel):
    get_updates_buf: str = ""
    base_info: BaseInfo = Field(default_factory=BaseInfo)


class GetUpdatesResponse(ILinkModel):
    ret: int = 0
    errcode: int = 0
    errmsg: str = ""
    msgs: list[WeixinMessage] = Field(default_factory=list)
    get_updates_buf: str = ""
    longpolling_timeout_ms: int = 0


class GetUploadURLRequest(ILinkModel):
    filekey: str = ""
    media_type: int = 0
    to_user_id: str = ""
    rawsize: int = 0
    rawfilemd5: str = ""
    filesize: int = 0
    no_need_thumb: bool = False
    aeskey: str = ""
    base_info: BaseInfo = Field(default_factory=BaseInfo)


class GetUploadURLResponse(ILinkModel):
    ret: int = 0
    errcode: int = 0
    errmsg: str = ""
    upload_param: str = ""
    upload_full_url: str = ""


class SendMsg(ILinkModel):
    """Message payload for sending."""

    from_user_id: str = ""
    to_user_id: str = ""
    client_id: str = ""
    message_type: int = MESSAGE_TYPE_BOT
    message_state: int = MESSAGE_STATE_FINISH
    item_list: list[MessageItem] = Field(default_factory=list)
    context_token: str = ""


class SendMessageRequest(ILinkModel):
    msg: SendMsg
    base_info: BaseInfo = Field(default_factory=BaseInfo)


class SendMessageResponse(ILinkModel):
    ret: int = 0
    errcode: int = 0
    errmsg: str = ""


class GetConfigRequest(ILinkModel):
    ilink_user_id: str = ""
    context_token: str | None = None
    base_info: BaseInfo = Field(default_factory=BaseInfo)


class GetConfigResponse(ILinkModel):
    ret: int = 0
    errcode: int = 0
    errmsg: str = ""
    typing_ticket: str = ""


class SendTypingRequest(ILinkModel):
    ilink_user_id: str = ""
    typing_ticket: str = ""
    status: int = TYPING_STATUS_TYPING
    base_info: BaseInfo = Field(default_factory=BaseInfo)


class SendTypingResponse(ILinkModel):
    ret: int = 0
    errcode: int = 0
    errmsg: str = ""
