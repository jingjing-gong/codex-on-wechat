"""Pinned lark-cli bot identity response contract tests."""

from __future__ import annotations

from copy import deepcopy

import pytest

from src.lark_contract import (
    LarkBotIdentityContractError,
    VerifiedLarkBotIdentity,
    parse_composite_verified_lark_bot_identity,
    parse_verified_lark_bot_identity,
)


def _flat_identity() -> dict:
    return {
        "appId": "cli_aa1fc22eccb81cbd",
        "brand": "feishu",
        "defaultAs": "bot",
        "identity": "bot",
        "verified": True,
        "identities": {
            "bot": {
                "status": "ready",
                "available": True,
                "verified": True,
                "openId": "ou_verified_bot_1234",
                "appName": "PersonalAgent",
            },
            "user": {
                "status": "not_configured",
                "available": False,
                "verified": False,
            },
        },
    }


def _explicit_bot() -> dict:
    return {
        "appId": "cli_aa1fc22eccb81cbd",
        "available": True,
        "brand": "feishu",
        "defaultAs": "auto",
        "identity": "bot",
        "identitySource": "flag",
        "profile": "cli_aa1fc22eccb81cbd",
        "tokenStatus": "ready",
    }


def test_accepts_real_pinned_flat_verified_bot_identity() -> None:
    assert parse_verified_lark_bot_identity(_flat_identity()) == (
        VerifiedLarkBotIdentity(
            app_id="cli_aa1fc22eccb81cbd",
            brand="feishu",
            open_id="ou_verified_bot_1234",
        )
    )


def test_accepts_existing_wrapped_verified_bot_identity() -> None:
    payload = _flat_identity()
    payload.pop("identity")
    payload.pop("verified")

    assert parse_verified_lark_bot_identity(
        {"ok": True, "identity": "bot", "data": payload}
    ).open_id == "ou_verified_bot_1234"


def test_composite_accepts_ready_bot_when_auth_status_selected_user() -> None:
    status = _flat_identity()
    status["identity"] = "user"

    assert parse_composite_verified_lark_bot_identity(
        status,
        _explicit_bot(),
    ) == VerifiedLarkBotIdentity(
        app_id="cli_aa1fc22eccb81cbd",
        brand="feishu",
        open_id="ou_verified_bot_1234",
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("identity", "user"),
        ("identitySource", "auto"),
        ("available", False),
        ("tokenStatus", "expired"),
        ("appId", "cli_conflicting_app"),
        ("brand", "lark"),
    ],
)
def test_composite_rejects_unproven_or_conflicting_explicit_bot(
    field: str,
    value: object,
) -> None:
    status = _flat_identity()
    status["identity"] = "user"
    selected = _explicit_bot()
    selected[field] = value

    with pytest.raises(LarkBotIdentityContractError):
        parse_composite_verified_lark_bot_identity(status, selected)


@pytest.mark.parametrize(
    "case",
    [
        "missing-identity",
        "user-identity",
        "missing-top-verification",
        "false-top-verification",
        "string-top-verification",
        "not-ready",
        "false-availability",
        "false-bot-verification",
        "conflicting-app-alias",
        "conflicting-open-alias",
    ],
)
def test_rejects_unverified_or_conflicting_flat_identity(case: str) -> None:
    value = deepcopy(_flat_identity())
    bot = value["identities"]["bot"]
    if case == "missing-identity":
        value.pop("identity")
    elif case == "user-identity":
        value["identity"] = "user"
    elif case == "missing-top-verification":
        value.pop("verified")
    elif case == "false-top-verification":
        value["verified"] = False
    elif case == "string-top-verification":
        value["verified"] = "true"
    elif case == "not-ready":
        bot["status"] = "unavailable"
    elif case == "false-availability":
        bot["available"] = False
    elif case == "false-bot-verification":
        bot["verified"] = False
    elif case == "conflicting-app-alias":
        value["app_id"] = "cli_conflicting_app"
    elif case == "conflicting-open-alias":
        bot["open_id"] = "ou_conflicting_bot_1234"

    with pytest.raises(LarkBotIdentityContractError):
        parse_verified_lark_bot_identity(value)


@pytest.mark.parametrize(
    "value",
    [
        {
            "ok": True,
            "identity": "bot",
            "appId": "cli_outer_payload",
            "data": {},
        },
        {
            "ok": True,
            "identity": "bot",
            "data": {"ok": True, "data": {}},
        },
        {
            "ok": True,
            "identity": "bot",
            "data": {"identity": "user", "verified": True},
        },
    ],
)
def test_rejects_mixed_or_nested_identity_contracts(value: dict) -> None:
    with pytest.raises(LarkBotIdentityContractError):
        parse_verified_lark_bot_identity(value)


@pytest.mark.parametrize(
    ("key", "status"),
    [
        ("code", "0"),
        ("code", 0.0),
        ("code", False),
        ("code", 1),
        ("errcode", "0"),
        ("errcode", 0.0),
        ("errcode", True),
        ("errcode", -1),
    ],
)
def test_rejects_noncanonical_or_failed_status_codes(key: str, status: object) -> None:
    value = _flat_identity()
    value[key] = status

    with pytest.raises(LarkBotIdentityContractError):
        parse_verified_lark_bot_identity(value)


@pytest.mark.parametrize("key", ["code", "errcode"])
def test_accepts_only_integer_zero_status_code(key: str) -> None:
    value = _flat_identity()
    value[key] = 0

    assert parse_verified_lark_bot_identity(value).open_id == "ou_verified_bot_1234"
