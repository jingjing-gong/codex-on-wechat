"""Owner-only administration for canonical principal account mappings.

The mapping key is the exact authenticated transport triple
``(channel, bot_id, external_user_id)``.  This CLI deliberately does not
change conversation routing or infer identities from message text; it only
maintains the existing ``principals`` and ``principal_accounts`` records while
the runtime database and affected channel account are exclusively owned.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sqlite3
import sys
from collections.abc import Awaitable, Callable, Mapping, Sequence
from pathlib import Path
from typing import Any, TextIO, TypeVar

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.runtime.models import (  # noqa: E402
    PrincipalAccountRecord,
    PrincipalRecord,
    text_to_datetime,
)
from src.runtime.sqlite_store import SQLiteStore  # noqa: E402
from src.runtime.store import StoreError  # noqa: E402
from src.runtime.supervisor import (  # noqa: E402
    SupervisorAccountSetOwnership,
    SupervisorOwnershipConflict,
    SupervisorOwnershipError,
)


T = TypeVar("T")

_PRINCIPAL_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,255}$")
_LARK_OPEN_ID = re.compile(r"^ou_[A-Za-z0-9_-]{4,256}$")
_MAX_ACCOUNT_COMPONENT = 1_024
_MAX_IDENTIFIER_KIND = 128
_MAX_DISPLAY_NAME = 256


class PrincipalAccountAdminError(RuntimeError):
    """A safe, operator-facing principal mapping failure."""


def durable_database_path() -> Path:
    return Path(
        os.environ.get(
            "CODEX_WECHAT_DB",
            str(Path.home() / ".codex-wechat-bot" / "runtime.sqlite3"),
        )
    ).expanduser().resolve()


def normalize_principal_id(value: str) -> str:
    principal_id = str(value or "").strip()
    if not _PRINCIPAL_ID.fullmatch(principal_id):
        raise PrincipalAccountAdminError(
            "principal ID must start with a letter or digit and contain only "
            "letters, digits, '.', '_', ':', '@', '/' or '-' "
            "(maximum 256 characters)"
        )
    return principal_id


def _account_component(
    value: str,
    label: str,
    *,
    maximum: int = _MAX_ACCOUNT_COMPONENT,
    lowercase: bool = False,
) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise PrincipalAccountAdminError(f"{label} is required")
    if len(normalized) > maximum:
        raise PrincipalAccountAdminError(
            f"{label} exceeds the {maximum}-character limit"
        )
    if any(
        ord(character) < 0x20 or ord(character) == 0x7F
        for character in normalized
    ):
        raise PrincipalAccountAdminError(f"{label} contains a control character")
    return normalized.lower() if lowercase else normalized


def _display_name(value: str) -> str:
    if not value:
        return ""
    return _account_component(
        value,
        "display name",
        maximum=_MAX_DISPLAY_NAME,
    )


def _configured_by() -> str:
    return (
        f"local-owner:{os.getuid()}"
        if hasattr(os, "getuid")
        else "local-owner"
    )


class PrincipalAccountAdmin:
    """Synchronous local-owner wrapper around durable principal APIs."""

    def __init__(
        self,
        *,
        database: Path | None = None,
        store_factory: Callable[[Path], Any] = SQLiteStore,
        output: TextIO | None = None,
    ) -> None:
        self.database = (database or durable_database_path()).expanduser().resolve()
        self.store_factory = store_factory
        self.output = output or sys.stdout

    async def _with_store(self, operation: Callable[[Any], Awaitable[T]]) -> T:
        store = self.store_factory(self.database)
        try:
            initialize = getattr(store, "initialize", None)
            if not callable(initialize):
                raise PrincipalAccountAdminError(
                    "runtime store does not provide initialize()"
                )
            result = initialize()
            if hasattr(result, "__await__"):
                await result
            return await operation(store)
        finally:
            close = getattr(store, "close", None)
            if callable(close):
                result = close()
                if hasattr(result, "__await__"):
                    await result

    def _run_store(self, operation: Callable[[Any], Awaitable[T]]) -> T:
        return asyncio.run(self._with_store(operation))

    @staticmethod
    async def _maybe_await(value: T | Awaitable[T]) -> T:
        if hasattr(value, "__await__"):
            return await value  # type: ignore[misc]
        return value  # type: ignore[return-value]

    @staticmethod
    def _method(store: Any, name: str) -> Callable[..., Any]:
        method = getattr(store, name, None)
        if not callable(method):
            raise PrincipalAccountAdminError(
                f"runtime store does not provide {name}(); apply schema migrations"
            )
        return method

    @staticmethod
    def _field(value: Any, name: str, default: Any = None) -> Any:
        if isinstance(value, Mapping):
            return value.get(name, default)
        return getattr(value, name, default)

    def map(
        self,
        principal_id: str,
        channel: str,
        bot_id: str,
        external_user_id: str,
        *,
        identifier_kind: str = "external_user_id",
        display_name: str = "",
    ) -> Any:
        """Map one exact authenticated channel account to a principal."""

        pid = normalize_principal_id(principal_id)
        channel_value = _account_component(channel, "channel", lowercase=True)
        bot_value = _account_component(bot_id, "bot ID")
        user_value = _account_component(external_user_id, "external user ID")
        kind_value = _account_component(
            identifier_kind,
            "identifier kind",
            maximum=_MAX_IDENTIFIER_KIND,
        )
        name_value = _display_name(display_name)
        if channel_value in {"lark", "feishu"} and (
            kind_value != "open_id" or not _LARK_OPEN_ID.fullmatch(user_value)
        ):
            raise PrincipalAccountAdminError(
                "Lark principal mappings require identifier kind open_id and "
                "a stable ou_ external user ID"
            )

        with SupervisorAccountSetOwnership(
            self.database,
            accounts=((channel_value, bot_value),),
        ):

            async def map_account(store: Any) -> Any:
                get_principal = self._method(store, "get_principal")
                principal = await self._maybe_await(get_principal(pid))
                if principal is None:
                    create = self._method(store, "create_principal")
                    principal = await self._maybe_await(
                        create(
                            principal_id=pid,
                            display_name=name_value,
                            enabled=True,
                        )
                    )
                elif name_value and str(
                    self._field(principal, "display_name", "")
                ) != name_value:
                    update = self._method(store, "update_principal")
                    await self._maybe_await(
                        update(pid, display_name=name_value)
                    )

                mapper = self._method(store, "map_principal_account")
                return await self._maybe_await(
                    mapper(
                        principal_id=pid,
                        channel=channel_value,
                        bot_id=bot_value,
                        external_user_id=user_value,
                        identifier_kind=kind_value,
                        configured_by=_configured_by(),
                    )
                )

            result = self._run_store(map_account)

        self.output.write(
            f"mapped {channel_value}/{bot_value}/{user_value} to {pid}\n"
        )
        return result

    def unmap(self, channel: str, bot_id: str, external_user_id: str) -> bool:
        """Retire the active mapping for one exact authenticated account."""

        channel_value = _account_component(channel, "channel", lowercase=True)
        bot_value = _account_component(bot_id, "bot ID")
        user_value = _account_component(external_user_id, "external user ID")

        with SupervisorAccountSetOwnership(
            self.database,
            accounts=((channel_value, bot_value),),
        ):

            async def unmap_account(store: Any) -> bool:
                unmap = self._method(store, "unmap_principal_account")
                return bool(
                    await self._maybe_await(
                        unmap(
                            channel=channel_value,
                            bot_id=bot_value,
                            external_user_id=user_value,
                        )
                    )
                )

            removed = self._run_store(unmap_account)

        if not removed:
            raise PrincipalAccountAdminError(
                "no active principal mapping for "
                f"{channel_value}/{bot_value}/{user_value}"
            )
        self.output.write(
            f"unmapped {channel_value}/{bot_value}/{user_value}\n"
        )
        return True

    def adopt(
        self,
        principal_id: str,
        agent_id: str,
        channel: str,
        bot_id: str,
        external_user_id: str,
        *,
        session_id: str = "default",
    ) -> Mapping[str, Any]:
        """Adopt one existing direct transport history as canonical anchor."""

        pid = normalize_principal_id(principal_id)
        agent_value = _account_component(agent_id, "Agent ID")
        channel_value = _account_component(channel, "channel", lowercase=True)
        bot_value = _account_component(bot_id, "bot ID")
        user_value = _account_component(external_user_id, "external user ID")
        session_value = _account_component(session_id or "default", "session ID")

        with SupervisorAccountSetOwnership(
            self.database,
            accounts=((channel_value, bot_value),),
        ):

            async def adopt_conversation(store: Any) -> Mapping[str, Any]:
                resolve_account = self._method(store, "resolve_principal_account")
                account = await self._maybe_await(
                    resolve_account(
                        channel=channel_value,
                        bot_id=bot_value,
                        external_user_id=user_value,
                    )
                )
                if account is None or str(
                    self._field(account, "principal_id", "")
                ) != pid:
                    raise PrincipalAccountAdminError(
                        "account is not actively mapped to the requested principal"
                    )
                find_conversation = self._method(
                    store,
                    "get_transport_conversation_id",
                )
                conversation_id = await self._maybe_await(
                    find_conversation(
                        channel=channel_value,
                        bot_id=bot_value,
                        external_user_id=user_value,
                        session_id=session_value,
                        agent_id=agent_value,
                    )
                )
                if not conversation_id:
                    raise PrincipalAccountAdminError(
                        "no existing direct conversation for that account/Agent/session"
                    )
                bind = self._method(store, "bind_principal_conversation")
                return await self._maybe_await(
                    bind(
                        principal_id=pid,
                        agent_id=agent_value,
                        session_id=session_value,
                        conversation_id=str(conversation_id),
                        configured_by=_configured_by(),
                    )
                )

            result = self._run_store(adopt_conversation)

        self.output.write(
            f"adopted {channel_value}/{bot_value}/{user_value} history "
            f"for {pid}/{agent_value}/{session_value}\n"
        )
        return result

    def _read_only_connection(self) -> sqlite3.Connection:
        if not self.database.exists():
            raise PrincipalAccountAdminError(
                "runtime database does not exist; start ./cow once to initialize it"
            )
        if not self.database.is_file():
            raise PrincipalAccountAdminError(
                "runtime database is not a regular file"
            )
        try:
            connection = sqlite3.connect(
                f"{self.database.as_uri()}?mode=ro",
                uri=True,
                timeout=1.0,
            )
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA query_only=ON")
            tables = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            missing = {"principals", "principal_accounts"} - tables
            if missing:
                raise PrincipalAccountAdminError(
                    "runtime database is missing principal mapping schema"
                )
            return connection
        except PrincipalAccountAdminError:
            if "connection" in locals():
                connection.close()
            raise
        except sqlite3.Error as exc:
            if "connection" in locals():
                connection.close()
            raise PrincipalAccountAdminError(
                "could not open runtime principal mappings read-only"
            ) from exc

    def list(
        self,
        principal_id: str | None = None,
    ) -> list[tuple[PrincipalRecord, PrincipalAccountRecord | None]]:
        """List canonical principals and their active account mappings."""

        selected = normalize_principal_id(principal_id) if principal_id else None
        connection = self._read_only_connection()
        try:
            parameters: list[Any] = []
            where = ""
            if selected is not None:
                where = " WHERE p.principal_id=?"
                parameters.append(selected)
            values = connection.execute(
                "SELECT p.principal_id, p.display_name, p.enabled, "
                "p.metadata_json, p.created_at AS principal_created_at, "
                "p.updated_at AS principal_updated_at, "
                "pa.principal_account_id, pa.channel, pa.bot_id, "
                "pa.external_user_id, pa.identifier_kind, "
                "pa.mapping_revision, pa.active, pa.configured_by, "
                "pa.created_at AS account_created_at, pa.retired_at "
                "FROM principals AS p "
                "LEFT JOIN principal_accounts AS pa "
                "ON pa.principal_id=p.principal_id AND pa.active=1"
                + where
                + " ORDER BY p.principal_id, pa.created_at, "
                "pa.principal_account_id",
                parameters,
            ).fetchall()
            rows: list[tuple[PrincipalRecord, PrincipalAccountRecord | None]] = []
            for value in values:
                try:
                    metadata = json.loads(str(value["metadata_json"] or "{}"))
                except (TypeError, json.JSONDecodeError) as exc:
                    raise PrincipalAccountAdminError(
                        "principal contains malformed metadata JSON"
                    ) from exc
                if not isinstance(metadata, Mapping):
                    raise PrincipalAccountAdminError(
                        "principal contains invalid metadata"
                    )
                principal = PrincipalRecord(
                    principal_id=str(value["principal_id"]),
                    display_name=str(value["display_name"] or ""),
                    enabled=bool(value["enabled"]),
                    metadata=dict(metadata),
                    created_at=text_to_datetime(value["principal_created_at"]),
                    updated_at=text_to_datetime(value["principal_updated_at"]),
                )
                account: PrincipalAccountRecord | None = None
                if value["principal_account_id"] is not None:
                    account = PrincipalAccountRecord(
                        principal_account_id=str(value["principal_account_id"]),
                        principal_id=principal.principal_id,
                        channel=str(value["channel"]),
                        bot_id=str(value["bot_id"]),
                        external_user_id=str(value["external_user_id"]),
                        identifier_kind=str(value["identifier_kind"]),
                        mapping_revision=int(value["mapping_revision"]),
                        active=bool(value["active"]),
                        configured_by=str(value["configured_by"]),
                        created_at=text_to_datetime(value["account_created_at"]),
                        retired_at=text_to_datetime(value["retired_at"]),
                        principal_enabled=principal.enabled,
                    )
                rows.append((principal, account))
        except PrincipalAccountAdminError:
            raise
        except sqlite3.Error as exc:
            raise PrincipalAccountAdminError(
                "could not read canonical principal mappings"
            ) from exc
        finally:
            connection.close()

        if selected is not None and not rows:
            raise PrincipalAccountAdminError(
                f"unknown canonical principal: {selected}"
            )
        if not rows:
            self.output.write("no canonical principals configured\n")
            return []

        self.output.write(
            "PRINCIPAL\tSTATE\tCHANNEL\tBOT ID\tEXTERNAL USER ID\t"
            "IDENTIFIER KIND\tREVISION\n"
        )
        for principal, account in rows:
            self.output.write(
                f"{principal.principal_id}\t"
                f"{'enabled' if principal.enabled else 'disabled'}\t"
                f"{account.channel if account else ''}\t"
                f"{account.bot_id if account else ''}\t"
                f"{account.external_user_id if account else ''}\t"
                f"{account.identifier_kind if account else ''}\t"
                f"{account.mapping_revision if account else ''}\n"
            )
        return rows


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="./cow principal",
        description=(
            "Manage owner-configured canonical mappings for exact channel "
            "account identities."
        ),
    )
    commands = parser.add_subparsers(dest="command", required=True)
    list_command = commands.add_parser(
        "list", help="list canonical principals and active mappings"
    )
    list_command.add_argument("principal_id", nargs="?")
    map_command = commands.add_parser(
        "map", help="map an exact channel/bot/user identity to a principal"
    )
    map_command.add_argument("principal_id")
    map_command.add_argument("channel")
    map_command.add_argument("bot_id")
    map_command.add_argument("external_user_id")
    map_command.add_argument(
        "--identifier-kind",
        default="external_user_id",
        help="audit label for the external identifier namespace",
    )
    map_command.add_argument("--display-name", default="")
    unmap_command = commands.add_parser(
        "unmap", help="retire one exact channel/bot/user mapping"
    )
    unmap_command.add_argument("channel")
    unmap_command.add_argument("bot_id")
    unmap_command.add_argument("external_user_id")
    adopt_command = commands.add_parser(
        "adopt",
        help="adopt an existing direct account history for a principal Agent session",
    )
    adopt_command.add_argument("principal_id")
    adopt_command.add_argument("agent_id")
    adopt_command.add_argument("channel")
    adopt_command.add_argument("bot_id")
    adopt_command.add_argument("external_user_id")
    adopt_command.add_argument("--session-id", default="default")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    admin = PrincipalAccountAdmin()
    try:
        if arguments.command == "list":
            admin.list(arguments.principal_id)
        elif arguments.command == "map":
            admin.map(
                arguments.principal_id,
                arguments.channel,
                arguments.bot_id,
                arguments.external_user_id,
                identifier_kind=arguments.identifier_kind,
                display_name=arguments.display_name,
            )
        elif arguments.command == "unmap":
            admin.unmap(
                arguments.channel,
                arguments.bot_id,
                arguments.external_user_id,
            )
        elif arguments.command == "adopt":
            admin.adopt(
                arguments.principal_id,
                arguments.agent_id,
                arguments.channel,
                arguments.bot_id,
                arguments.external_user_id,
                session_id=arguments.session_id,
            )
        else:  # pragma: no cover - argparse owns command validation
            raise PrincipalAccountAdminError(
                f"unknown principal command: {arguments.command}"
            )
    except SupervisorOwnershipConflict as exc:
        print(
            f"error: {exc}; stop the running ./cow supervisor and retry",
            file=sys.stderr,
        )
        return 1
    except SupervisorOwnershipError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except (PrincipalAccountAdminError, StoreError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised through launcher tests
    raise SystemExit(main())


__all__ = [
    "PrincipalAccountAdmin",
    "PrincipalAccountAdminError",
    "build_parser",
    "durable_database_path",
    "main",
    "normalize_principal_id",
]
