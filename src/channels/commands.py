"""Shared command registry and channel capability policy.

Command parsing lives in :mod:`src.channels.models`, while this module owns the
runtime-facing command effect router.  It also contains the immutable middle
layer shared by channel adapters: public command metadata, deterministic help
rendering, and the small capability policy used to decide which commands a
transport may expose.

The default policy deliberately matches the historical WeChat registry byte
for byte.  Other adapters must select their channel explicitly; Lark/Feishu do
not expose WeChat's deferred-reply quota command.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import inspect
import json
import logging
from pathlib import Path
import re
import shlex
import subprocess
from types import MappingProxyType
from typing import Any, Callable, Mapping, Sequence
import unicodedata
import uuid

from pydantic import ValidationError

from .models import ChannelCommand, InboundEnvelope, utc_now
from src.runtime.identity import scoped_id
from src.runtime.models import USER_REPLY_FORMAT_AGENT_PREFIX_V1
from src.runtime.roles import (
    RoleValidationError,
    is_default_role_token,
    normalize_role_text,
    validate_role_snapshot,
)
from src.runtime.shell import run_bounded_shell_process
from src.runtime.skills import format_skills_markdown
from src.runtime.store import (
    QueueFullError,
    WORKING_DIRECTORY_RESPONSE_MAX_CHARS,
    format_working_directory_response,
)

logger = logging.getLogger(__name__)


DEFERRED_REPLY_QUOTA_CAPABILITY = "deferred_reply_quota"
LARK_BOT_ONBOARDING_CAPABILITY = "lark_bot_onboarding"


@dataclass(frozen=True, slots=True)
class CommandRegistryEntry:
    """One public command row and any help-hidden compatibility aliases."""

    name: str | None
    syntax: str
    description: str
    hidden_aliases: tuple[str, ...] = ()
    required_capabilities: frozenset[str] = frozenset()


@dataclass(frozen=True, slots=True)
class CommandRegistryGroup:
    """An ordered help section in the immutable command registry."""

    title: str
    entries: tuple[CommandRegistryEntry, ...]


@dataclass(frozen=True, slots=True)
class ChannelCommandPolicy:
    """Capabilities granted to commands exposed by one channel adapter."""

    channel: str
    capabilities: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        channel = str(self.channel or "").strip().lower()
        if not channel:
            raise ValueError("command policy channel is required")
        capabilities = frozenset(
            str(value or "").strip().lower()
            for value in self.capabilities
            if str(value or "").strip()
        )
        object.__setattr__(self, "channel", channel)
        object.__setattr__(self, "capabilities", capabilities)

    def allows(self, entry: CommandRegistryEntry) -> bool:
        """Return whether all capabilities required by ``entry`` are present."""

        return entry.required_capabilities.issubset(self.capabilities)


COMMAND_REGISTRY = (
    CommandRegistryGroup(
        "Conversation",
        (
            CommandRegistryEntry("help", "/help", "Show this help"),
            CommandRegistryEntry(
                "clear",
                "/clear",
                "Clear the active conversation and start a fresh thread",
            ),
            CommandRegistryEntry("reset", "/reset", "Alias for `/clear`"),
            CommandRegistryEntry(
                "compact",
                "/compact",
                "Compact the active conversation while preserving context",
            ),
            CommandRegistryEntry(
                "cd",
                "/cd [path]",
                "Show or change the current Agent's working directory",
            ),
            CommandRegistryEntry(
                "sh",
                "/sh <command>",
                "Run a bounded shell command in the current Agent's workspace",
            ),
            CommandRegistryEntry(
                "skills",
                "/skills",
                "List enabled skills",
                hidden_aliases=("listskill", "listskills"),
            ),
            CommandRegistryEntry(
                None,
                "$<skill> <task description>",
                "Run a task with a selected skill",
            ),
        ),
    ),
    CommandRegistryGroup(
        "Tasks",
        (
            CommandRegistryEntry("status", "/status", "Show active tasks"),
            CommandRegistryEntry("tasks", "/tasks [limit]", "List your tasks"),
            CommandRegistryEntry(
                "retry",
                "/retry <task-id>",
                "Explicitly retry a failed or orphaned task",
            ),
            CommandRegistryEntry(
                "cancel",
                "/cancel [task-id]",
                "Cancel a task, or the current running task when omitted",
            ),
            CommandRegistryEntry(
                "report",
                "/report <message>",
                "Record an operator report in the persistent log",
            ),
            CommandRegistryEntry(
                "cron",
                "/cron <add|list|delete|help> ...",
                "Schedule durable Agent work and return its result",
            ),
        ),
    ),
    CommandRegistryGroup(
        "Agents",
        (
            CommandRegistryEntry("agents", "/agents", "List Agents"),
            CommandRegistryEntry(
                "agent",
                "/agent [agent-id] [profile]",
                "Show, switch, or create the front Agent",
            ),
            CommandRegistryEntry(
                "delagent",
                "/delagent <agent-id> [force]",
                "Delete a dynamic Agent; force cancels unfinished work",
            ),
            CommandRegistryEntry(
                "ask",
                "/ask <agent-id> <prompt>",
                "Send a correlated Agent request",
            ),
        ),
    ),
    CommandRegistryGroup(
        "Agent Configuration",
        (
            CommandRegistryEntry(
                "system",
                "/system [default|<role>]",
                "Show, set, or clear the current Agent's role",
            ),
            CommandRegistryEntry(
                "mode",
                "/mode [chat|plan|review|execute]",
                "Show or set the operating mode",
            ),
            CommandRegistryEntry(
                "modes",
                "/modes",
                "List operating modes and mark the current mode",
            ),
            CommandRegistryEntry(
                "model",
                "/model [<model-id> [<effort|default>]|effort <effort|default>]",
                "Show or set the model and reasoning effort",
            ),
            CommandRegistryEntry(
                "models",
                "/models",
                "List models and their supported reasoning efforts",
            ),
            CommandRegistryEntry(
                "notify",
                "/notify [on|off]",
                "Show or set notifications",
            ),
        ),
    ),
    CommandRegistryGroup(
        "Delivery",
        (
            CommandRegistryEntry(
                "inbox",
                "/inbox [agent-id|all]",
                "Present unseen notifications",
            ),
            CommandRegistryEntry(
                "recv",
                "/recv",
                "Receive the next replies deferred by WeChat's ten-message quota",
                required_capabilities=frozenset(
                    {DEFERRED_REPLY_QUOTA_CAPABILITY}
                ),
            ),
        ),
    ),
)


# Keep channel-only commands out of ``COMMAND_REGISTRY`` itself.  That tuple,
# ``MVP_COMMANDS``, and the default rendered help are long-standing WeChat
# compatibility exports.  Adapter extensions are composed only when an
# explicit channel policy asks for them, so adding a Lark control command does
# not change any WeChat command name, help byte, or unknown-command behavior.
LARK_COMMAND_REGISTRY_EXTENSION = (
    CommandRegistryGroup(
        "Lark",
        (
            CommandRegistryEntry(
                "lark",
                "/lark add [profile]",
                "Add a Lark/Feishu bot (owner only)",
                required_capabilities=frozenset(
                    {LARK_BOT_ONBOARDING_CAPABILITY}
                ),
            ),
        ),
    ),
)


WECHAT_COMMAND_POLICY = ChannelCommandPolicy(
    channel="wechat",
    capabilities=frozenset({DEFERRED_REPLY_QUOTA_CAPABILITY}),
)
LARK_COMMAND_POLICY = ChannelCommandPolicy(
    channel="lark",
    capabilities=frozenset({LARK_BOT_ONBOARDING_CAPABILITY}),
)
DEFAULT_COMMAND_POLICY = WECHAT_COMMAND_POLICY
COMMAND_POLICIES: Mapping[str, ChannelCommandPolicy] = MappingProxyType(
    {
        "wechat": WECHAT_COMMAND_POLICY,
        "lark": LARK_COMMAND_POLICY,
        # Feishu is the regional brand of the same adapter.  Normalized ingress
        # uses ``lark``, but accepting the alias keeps configuration/help tools
        # from accidentally exposing a WeChat-only command.
        "feishu": LARK_COMMAND_POLICY,
    }
)


def command_policy_for(channel: str | None = None) -> ChannelCommandPolicy:
    """Return the immutable command policy for ``channel``.

    An omitted channel retains the historical WeChat behavior.  An unknown,
    explicitly named channel receives the conservative shared command set with
    no transport-specific capabilities.
    """

    canonical = str(channel or "").strip().lower()
    if not canonical:
        return DEFAULT_COMMAND_POLICY
    policy = COMMAND_POLICIES.get(canonical)
    if policy is not None:
        return policy
    return ChannelCommandPolicy(channel=canonical)


def _command_registry_indexes(
    registry: Sequence[CommandRegistryGroup] = COMMAND_REGISTRY,
) -> tuple[frozenset[str], Mapping[str, CommandRegistryEntry]]:
    """Validate ``registry`` and return all slash names and aliases."""

    names: dict[str, CommandRegistryEntry] = {}
    syntaxes: set[str] = set()
    for group in registry:
        for entry in group.entries:
            if entry.syntax in syntaxes:
                raise RuntimeError(f"duplicate public command syntax: {entry.syntax}")
            syntaxes.add(entry.syntax)
            if entry.name is None:
                if entry.hidden_aliases:
                    raise RuntimeError("a pseudo-command cannot have slash aliases")
            elif entry.syntax.split(maxsplit=1)[0] != f"/{entry.name}":
                raise RuntimeError(
                    f"command name and public syntax conflict: {entry.name}"
                )
            entry_names = (() if entry.name is None else (entry.name,)) + tuple(
                entry.hidden_aliases
            )
            for name in entry_names:
                canonical = str(name or "").strip().lower()
                if not canonical or canonical in names:
                    raise RuntimeError(f"duplicate or empty command name: {name}")
                names[canonical] = entry
    return frozenset(names), MappingProxyType(names)


MVP_COMMANDS, _COMMAND_ENTRY_BY_NAME = _command_registry_indexes()
MVP_COMMAND_NAMES = frozenset(f"/{name}" for name in MVP_COMMANDS)
_command_registry_indexes((*COMMAND_REGISTRY, *LARK_COMMAND_REGISTRY_EXTENSION))
_, _LARK_COMMAND_ENTRY_BY_NAME = _command_registry_indexes(
    LARK_COMMAND_REGISTRY_EXTENSION
)
_CHANNEL_COMMAND_REGISTRY_EXTENSIONS: Mapping[
    str, tuple[CommandRegistryGroup, ...]
] = MappingProxyType({"lark": LARK_COMMAND_REGISTRY_EXTENSION})


def _command_registry_extensions(
    policy: ChannelCommandPolicy,
    registry: Sequence[CommandRegistryGroup],
) -> tuple[CommandRegistryGroup, ...]:
    """Return adapter additions without changing custom registry callers."""

    if registry is not COMMAND_REGISTRY:
        return ()
    return _CHANNEL_COMMAND_REGISTRY_EXTENSIONS.get(policy.channel, ())


def filter_command_registry(
    registry: Sequence[CommandRegistryGroup] = COMMAND_REGISTRY,
    *,
    channel: str | None = None,
    policy: ChannelCommandPolicy | None = None,
) -> tuple[CommandRegistryGroup, ...]:
    """Return ordered registry groups supported by one channel policy."""

    selected_policy = policy or command_policy_for(channel)
    extensions = _command_registry_extensions(selected_policy, registry)
    # Validate the complete composed registry before filtering.  A hidden
    # duplicate or malformed unsupported command is still a programming error.
    _command_registry_indexes((*registry, *extensions))
    groups: list[CommandRegistryGroup] = []
    for group in (*registry, *extensions):
        entries = tuple(
            entry for entry in group.entries if selected_policy.allows(entry)
        )
        if entries:
            groups.append(CommandRegistryGroup(group.title, entries))
    return tuple(groups)


def command_names_for_channel(
    channel: str | None = None,
    *,
    policy: ChannelCommandPolicy | None = None,
) -> frozenset[str]:
    """Return supported slash command names, including hidden aliases."""

    names, _entries = _command_registry_indexes(
        filter_command_registry(channel=channel, policy=policy)
    )
    return names


def command_supported(
    name: str,
    *,
    channel: str | None = None,
    policy: ChannelCommandPolicy | None = None,
) -> bool:
    """Return whether ``name`` is registered and allowed for one channel."""

    canonical = str(name or "").strip().lower().removeprefix("/")
    selected_policy = policy or command_policy_for(channel)
    entry = _COMMAND_ENTRY_BY_NAME.get(canonical)
    if entry is None and selected_policy.channel == "lark":
        entry = _LARK_COMMAND_ENTRY_BY_NAME.get(canonical)
    if entry is None:
        return False
    return selected_policy.allows(entry)


def _render_command_help(
    registry: Sequence[CommandRegistryGroup] = COMMAND_REGISTRY,
    *,
    channel: str | None = None,
    policy: ChannelCommandPolicy | None = None,
) -> str:
    """Render deterministic Markdown help for one channel policy."""

    groups = filter_command_registry(
        registry,
        channel=channel,
        policy=policy,
    )
    lines = ["## Commands"]
    for group in groups:
        lines.extend(("", f"### {group.title}"))
        lines.extend(
            f"- `{entry.syntax}` - {entry.description}" for entry in group.entries
        )
    return "\n".join(lines) + "\n"


def command_help_for_channel(channel: str | None = None) -> str:
    """Return deterministic help for ``channel``."""

    return _render_command_help(channel=channel)


def _command_usage(name: str) -> str:
    """Return the historical deterministic usage string for one command."""

    canonical = str(name or "").strip().lower()
    entry = _COMMAND_ENTRY_BY_NAME.get(canonical)
    if entry is None:
        return f"usage: /{canonical}" if canonical else "usage: /"
    syntax = entry.syntax
    if canonical != entry.name:
        _slash, separator, tail = syntax.partition(" ")
        syntax = f"/{canonical}" + (separator + tail if separator else "")
    return f"usage: {syntax}"


def unsupported_command_response(name: str) -> str:
    """Return the non-disclosing response for unknown/unsupported commands."""

    canonical = str(name or "").strip().lower().removeprefix("/")
    token = f"/{canonical}" if canonical else "/"
    return f"unknown command: {token}. try /help"


COMMAND_HELP = _render_command_help()


DEFAULT_AGENT_ID = "codex"
DEFAULT_SESSION_ID = "default"

ACTIVE_TASK_STATES = frozenset({"queued", "claimed", "running", "cancel_requested"})
RETRYABLE_TASK_STATES = frozenset({"failed", "orphaned", "interrupted"})
TERMINAL_TASK_STATES = frozenset({"completed", "failed", "interrupted", "cancelled", "canceled"})

_MAX_COMMAND_MARKDOWN = 6000
_MAX_SHELL_OUTPUT = 6000
_MAX_SHELL_COMMAND_MARKDOWN = 1500
_SHELL_TIMEOUT = 30
_LIST_TRUNCATION_MARKER = "_... (list truncated)_"
_CONTENT_TRUNCATION_MARKER = "... (content truncated)"
_MAX_PUBLIC_ID = 256
_MAX_PUBLIC_TEXT = WORKING_DIRECTORY_RESPONSE_MAX_CHARS
_MAX_EFFORT_NAME = 96
_MAX_EFFORT_LIST = 1024
_WECHAT_REPLY_TEXT_LIMIT = 3_000
_REPLY_CONTINUATION_SUFFIX = "\n\nReply /recv to continue."

def run_shell_command(
    command: str,
    *,
    cwd: str | Path | None = None,
    timeout: int = _SHELL_TIMEOUT,
    max_output: int = _MAX_SHELL_OUTPUT,
) -> str:
    """Execute a bounded shell command for the legacy ``/sh`` command.

    This intentionally mirrors the original bot helper.  The durable router
    invokes it in a worker thread so a command cannot block the owner loop.
    """

    completed = run_bounded_shell_process(
        command,
        cwd=cwd or Path(__file__).resolve().parents[2],
        timeout=timeout,
        max_output=max_output,
    )
    return f"exit code: {completed.returncode}\n{completed.output}"

class CommandResponse(str):
    """String-compatible command result carrying projection metadata."""

    def __new__(
        cls,
        value: str,
        presentation_ids: Sequence[str] = (),
        *,
        response_fragments: Sequence[Mapping[str, Any]] = (),
    ) -> "CommandResponse":
        result = str.__new__(cls, value)
        result.presentation_ids = tuple(str(item) for item in presentation_ids if item)
        result.response_fragments = tuple(
            dict(fragment) for fragment in response_fragments
        )
        return result

def command_delivery_id(envelope: InboundEnvelope) -> str:
    """Return the stable outbox ID for one inbound control command."""

    return scoped_id(
        "command",
        (
            envelope.channel,
            envelope.bot_id,
            envelope.external_user_id,
            envelope.session_id or DEFAULT_SESSION_ID,
            envelope.external_message_id,
        ),
    )

def legacy_command_delivery_id(envelope: InboundEnvelope) -> str:
    """Return the persisted pre-v1 command identity for upgrade recovery."""

    identity = ":".join(
        (
            envelope.channel,
            envelope.bot_id,
            envelope.external_user_id,
            envelope.session_id or DEFAULT_SESSION_ID,
            envelope.external_message_id,
        )
    )
    return f"command:{identity}"

def command_delivery_id_candidates(envelope: InboundEnvelope) -> tuple[str, ...]:
    """Return canonical then legacy command IDs in lookup preference order."""

    return tuple(
        dict.fromkeys(
            (command_delivery_id(envelope), legacy_command_delivery_id(envelope))
        )
    )

def command_request_id(envelope: InboundEnvelope) -> str:
    """Return an idempotency key for a command-created Agent task.

    A process can crash after durable ingress but before the command response
    is projected.  Replaying ``/ask`` must therefore identify the same logical
    task instead of enqueueing a second request.
    """

    return _command_request_id_for_delivery(
        command_delivery_id(envelope), envelope.text
    )

def command_task_id(envelope: InboundEnvelope) -> str:
    """Return the stable task identity for work created by one command.

    The task ID has to be known before ``/ask`` reserves its acknowledgement:
    the acknowledgement includes that ID and is committed in the same SQLite
    transaction that publishes the queued task.  Deriving it from the framed
    command request identity also makes a direct router replay deterministic.
    """

    return str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"codex-wechat:command-task:{command_request_id(envelope)}",
        )
    )

def command_client_id(envelope: InboundEnvelope) -> str:
    """Return the immutable primary iLink ID for a command response."""

    delivery_id = command_delivery_id(envelope)
    identity = delivery_id.removeprefix("command:")
    return str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"codex-wechat:command:{identity}",
        )
    )

def command_initial_reply(
    envelope: InboundEnvelope,
    content: str,
    *,
    agent_id: str,
) -> dict[str, Any]:
    """Build WeChat's canonical slot-reserving command-reply projection.

    ``create_user_outbox`` derives ``outbox:<delivery-id>`` when the gateway
    later replays this projection for its immediate send.  Persisting that
    exact source key here ensures both paths resolve to one candidate, slot,
    outbox row, and primary wire identity.  Peer adapters must publish their
    command acknowledgement through their own slotless account outbox.
    """

    delivery_id = command_delivery_id(envelope)
    return {
        "target": envelope.reply_target.to_dict(),
        "source_key": f"outbox:{delivery_id}",
        "content": str(content),
        "outbox_id": delivery_id,
        "client_id": command_client_id(envelope),
        "from_user_id": envelope.bot_id,
        "agent_id": str(agent_id or envelope.agent_id),
    }


def _slotful_command_initial_reply(
    envelope: InboundEnvelope,
    content: str,
    *,
    agent_id: str,
) -> dict[str, Any] | None:
    """Return an atomic initial projection only for slot-quota transports."""

    policy = command_policy_for(envelope.channel)
    if DEFERRED_REPLY_QUOTA_CAPABILITY not in policy.capabilities:
        return None
    return command_initial_reply(envelope, content, agent_id=agent_id)

def _command_request_id_for_delivery(delivery_id: str, command_text: str) -> str:
    material = "\x1f".join((delivery_id, str(command_text or "").strip()))
    return str(
        uuid.uuid5(uuid.NAMESPACE_URL, f"codex-wechat:command-request:{material}")
    )

def command_request_id_candidates(envelope: InboundEnvelope) -> tuple[str, ...]:
    """Return request keys corresponding to canonical and legacy command IDs."""

    return tuple(
        dict.fromkeys(
            _command_request_id_for_delivery(candidate, envelope.text)
            for candidate in command_delivery_id_candidates(envelope)
        )
    )

def _value(record: Any, *names: str, default: Any = None) -> Any:
    if isinstance(record, Mapping):
        for name in names:
            if name in record:
                return record[name]
        return default
    for name in names:
        if hasattr(record, name):
            return getattr(record, name)
    return default

async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value

async def _invoke_compatible(
    target: Any,
    names: Sequence[str],
    *,
    positional: Sequence[Any] = (),
    keyword: Mapping[str, Any] | None = None,
) -> Any:
    """Call the first supported public method while tolerating narrow fakes.

    The small amount of signature adaptation keeps the channel layer usable
    with the store protocol as well as with a TaskManager facade.  It does not
    inspect or mutate runtime-owned dictionaries.
    """

    keyword = dict(keyword or {})
    for name in names:
        method = getattr(target, name, None)
        if method is None:
            continue
        try:
            signature = inspect.signature(method)
        except (TypeError, ValueError):
            return await _maybe_await(method(*positional, **keyword))
        accepts_var_kw = any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in signature.parameters.values()
        )
        if accepts_var_kw:
            filtered = keyword
        else:
            filtered = {
                key: value
                for key, value in keyword.items()
                if key in signature.parameters
            }
        try:
            signature.bind(*positional, **filtered)
        except TypeError:
            # A compatibility call may provide a value both positionally and
            # under its conventional keyword name (or the target may expose a
            # keyword-only parameter).  Retry with keyword arguments alone
            # before deciding that this method is unsupported.
            if positional:
                retry_keywords = dict(filtered)
                parameter_names = [
                    parameter.name
                    for parameter in signature.parameters.values()
                    if parameter.kind
                    in {
                        inspect.Parameter.POSITIONAL_ONLY,
                        inspect.Parameter.POSITIONAL_OR_KEYWORD,
                        inspect.Parameter.KEYWORD_ONLY,
                    }
                ]
                for parameter_name, value in zip(parameter_names, positional):
                    retry_keywords.setdefault(parameter_name, value)
                try:
                    signature.bind(**retry_keywords)
                except TypeError:
                    continue
                return await _maybe_await(method(**retry_keywords))
            continue
        return await _maybe_await(method(*positional, **filtered))
    joined = ", ".join(names)
    raise AttributeError(f"{type(target).__name__} implements none of: {joined}")

def _task_id(result: Any) -> str:
    return str(_value(result, "task_id", "id", default="") or "")


def _task_dedupe_key(result: Any) -> str:
    return str(_value(result, "dedupe_key", default="") or "")


def _is_duplicate(result: Any) -> bool:
    duplicate = _value(result, "duplicate", "is_duplicate", default=None)
    if duplicate is not None:
        return bool(duplicate)
    status = str(_value(result, "status", "inbound_status", default="")).lower()
    return status == "duplicate"


def _is_accepted(result: Any) -> bool:
    accepted = _value(result, "accepted", "created", default=None)
    if accepted is not None:
        return bool(accepted) or _is_duplicate(result)
    status = str(_value(result, "status", "inbound_status", default="")).lower()
    if status:
        return status in {
            "accepted",
            "duplicate",
            "stored",
            "task_queued",
            "queued",
        }
    return result is not False and result is not None


def _format_task(record: Any) -> str:
    task_id = str(_value(record, "task_id", "id", default="?") or "?")
    status = str(_value(record, "status", "state", default="unknown") or "unknown")
    agent_id = str(_value(record, "agent_id", default="") or "")
    attempts = _value(record, "attempts", "attempt", default=None)
    error = _bounded_public_error(
        _value(record, "last_error", "error", default=""),
        max_length=_MAX_PUBLIC_TEXT,
    )
    suffix = f" agent={agent_id}" if agent_id else ""
    if attempts is not None:
        suffix += f" attempts={attempts}"
    if error:
        suffix += f" error={error}"
    return f"{task_id} {status}{suffix}"


_CRON_COMMAND_HELP = """## Cron schedules

- You can also ask naturally, for example: `每天工作日上午9点帮我总结待办`.
  The Agent shows a normalized draft first; no job exists until you send a
  later message that is exactly `确认` or `confirm`. Send `取消` or `cancel`
  instead to discard the pending draft. Drafts expire after 15 minutes.
- `/cron add at <ISO datetime> [--tz <IANA timezone>] -- <prompt>`
- `/cron add every <duration> [--tz <IANA timezone>] -- <prompt>`
- `/cron add cron <minute> <hour> <day-of-month> <month> <day-of-week> [--tz <IANA timezone>] -- <prompt>`
- `/cron list`
- `/cron delete <job-id>`
- `/cron help`

The default timezone is `Asia/Shanghai`. Durations use forms such as `30m`,
`2h`, or `1h30m`. Jobs survive restarts. After downtime, each due job catches
up once at most and then advances to its first future occurrence. Agent mode,
model, role, policy, and working directory are frozen at creation. Revoking
the owner mapping, origin bot, or target Agent disables the schedule. At fire
time the prompt runs without being echoed; only the Agent result or a safe
failure notice is returned through the bot and chat that created the job.
"""


def _cron_add_parts(command: ChannelCommand) -> tuple[str, str] | None:
    """Split `/cron add ... -- prompt` without consuming `--tz`."""

    message = _command_message(command)
    match = re.match(r"(?is)^add(?:\s+)(.*)\Z", message)
    if match is None:
        return None
    body = match.group(1)
    split = re.split(r"\s+--\s+", body, maxsplit=1)
    if len(split) != 2:
        return None
    schedule, prompt = (value.strip() for value in split)
    if not schedule or not prompt:
        return None
    return schedule, prompt


def _cron_job_id_for_command(command_id: str, envelope: InboundEnvelope) -> str:
    """Return a compact replay-stable ID for one durable add command."""

    source = str(command_id or command_delivery_id(envelope))
    return f"cron-{uuid.uuid5(uuid.NAMESPACE_URL, source).hex}"


def _format_cron_job(record: Any) -> str:
    job_id = _bounded_public_value(
        _value(record, "job_id", "id", default="?") or "?",
        max_length=_MAX_PUBLIC_ID,
    )
    agent_id = _bounded_public_value(
        _value(record, "agent_id", default="?") or "?",
        max_length=_MAX_PUBLIC_ID,
    )
    kind = _bounded_public_value(
        _value(record, "schedule_kind", "kind", default="?") or "?",
        max_length=_MAX_PUBLIC_ID,
    )
    expression = _bounded_public_value(
        _value(record, "schedule_expression", "expression", default="?") or "?",
        max_length=_MAX_PUBLIC_TEXT,
    )
    timezone_name = _bounded_public_value(
        _value(record, "timezone_name", "timezone", default="Asia/Shanghai")
        or "Asia/Shanghai",
        max_length=_MAX_PUBLIC_ID,
    )
    next_fire = _bounded_public_value(
        _value(record, "next_fire_at", default="-") or "-",
        max_length=_MAX_PUBLIC_ID,
    )
    prompt = _bounded_public_value(
        _value(record, "prompt", default="") or "",
        max_length=240,
    )
    prompt_suffix = f" · {prompt}" if prompt else ""
    return (
        f"- **`{job_id}`** · `{kind} {expression}` · tz `{timezone_name}` "
        f"· next `{next_fire}` · Agent `{agent_id}`{prompt_suffix}"
    )


def _format_cron_jobs(records: Sequence[Any]) -> str:
    if not records:
        return "cron jobs: (none)"
    entries = [(_format_cron_job(record), False) for record in records]
    return _bounded_catalog_markdown(("## Cron jobs", ""), entries)


def _task_state(record: Any) -> str:
    """Return a normalized task state for command guards and formatting."""

    value = _value(record, "status", "state", default="")
    value = getattr(value, "value", value)
    return str(value or "").strip().lower()


def _command_message(command: ChannelCommand) -> str:
    """Return the untruncated text after a slash-command token."""

    raw = str(command.raw or "").lstrip()
    match = re.match(r"/\S+(?:\s+(.*))?\Z", raw, flags=re.DOTALL)
    if match is not None:
        return str(match.group(1) or "").strip()
    return command.argument.strip()


def _normalized_public_text(value: Any) -> str:
    """Flatten public text and remove invisible control obfuscation."""

    without_escapes = _ANSI_ESCAPE_PATTERN.sub("", str(value or ""))
    cleaned = "".join(
        " "
        if character.isspace()
        else ""
        if unicodedata.category(character).startswith("C")
        else character
        for character in without_escapes
    )
    return " ".join(cleaned.split()).replace("`", "'")


def _bounded_public_value(value: Any, *, max_length: int) -> str:
    """Normalize one public catalog field without allowing it to own a reply."""

    normalized = _normalized_public_text(value)
    if len(normalized) <= max_length:
        return normalized
    return normalized[: max(0, max_length - 3)].rstrip() + "..."


_ANSI_ESCAPE_PATTERN = re.compile(
    r"(?:\x1b\][^\x07]*(?:\x07|\x1b\\)|"
    r"\x1b\[[0-?]*[ -/]*[@-~]|"
    r"\x9b[0-?]*[ -/]*[@-~]|"
    r"\x1b[@-Z\\-_])"
)
_PUBLIC_ERROR_URL_PATTERN = re.compile(
    r"(?i)\b[a-z][a-z0-9+.-]*://[^\s,)\]}>\"']+"
)
_PUBLIC_ERROR_AUTH_SCHEME_PATTERN = re.compile(
    r'''(?i)(?<![\w-])(?:"(?:bearer|basic)"|'(?:bearer|basic)'|'''
    r'''(?:bearer|basic))(?:\s*[:=]\s*|\s+)'''
    r'''(?:"[^\"]*"|'[^']*'|\[[^\]]*\]|<[^>]*>|\([^)]*\)|'''
    r'''\{[^}]*\}|[^\s,;)\]}>]+)'''
)
_PUBLIC_ERROR_ASSIGNMENT_PATTERN = re.compile(
    r"(?i)(?<![\w-])[\"']?"
    r"((?:[a-z0-9][a-z0-9_-]{0,63})?(?:"
    r"api[_-]?key|authorization|credential|password|secret|token))"
    r"[\"']?\s*[:=]\s*"
    r'''(?:"[^\"]*"|'[^']*'|\[[^\]]*\]|<[^>]*>|\([^)]*\)|'''
    r'''\{[^}]*\}|[^\s,;)\]}>]+)'''
)
_PUBLIC_ERROR_HOST_PATTERN = re.compile(
    r"(?i)(?<![\w@/.-])(?:"
    r"\[(?=[0-9a-f:.%]*:)[0-9a-f:.]+(?:%[a-z0-9_.-]+)?\]|"
    r"localhost|"
    r"(?:[a-z0-9](?:[a-z0-9-]{0,62}[a-z0-9])?\.)+[a-z]{2,63}|"
    r"(?:\d{1,3}\.){3}\d{1,3}"
    r")(?::\d{1,5})?(?:/[^\s,)\]}>\"']*)?"
)


def _bounded_public_error(value: Any, *, max_length: int) -> str:
    """Render an actionable exception without exposing transport secrets."""

    normalized = _normalized_public_text(value)
    normalized = _PUBLIC_ERROR_URL_PATTERN.sub("<redacted-url>", normalized)
    normalized = _PUBLIC_ERROR_AUTH_SCHEME_PATTERN.sub(
        "<redacted-authorization>", normalized
    )
    normalized = _PUBLIC_ERROR_ASSIGNMENT_PATTERN.sub(
        lambda match: f"{match.group(1)}=<redacted>", normalized
    )
    normalized = _PUBLIC_ERROR_HOST_PATTERN.sub(
        "<redacted-host>", normalized
    )
    return _bounded_public_value(normalized, max_length=max_length)


def _working_directory_path(
    value: Any,
    *,
    fallback: str | Path | None = None,
) -> tuple[str | Path, str]:
    """Return a validated facade path and its display representation."""

    raw_path = _value(value, "path", default=None)
    if raw_path is None and not isinstance(value, Mapping):
        raw_path = value
    if raw_path is None:
        raw_path = fallback
    if not isinstance(raw_path, (str, Path)):
        raise ValueError("working directory response has no path")
    display_path = str(raw_path)
    if not display_path.strip():
        raise ValueError("working directory response has no path")
    return raw_path, display_path


def _format_working_directory(path: Any) -> str:
    """Render one bounded, single-line working-directory acknowledgement."""

    return format_working_directory_response(path)


def _format_model_capability_error(action: str, exc: ValueError) -> str:
    """Return one bounded line for an expected model capability rejection."""

    detail = _bounded_public_error(exc, max_length=_MAX_PUBLIC_TEXT)
    return f"cannot {action} model: {detail or 'invalid model selection'}"


def _bounded_catalog_markdown(
    prefix: Sequence[str],
    entries: Sequence[tuple[str, bool]],
) -> str:
    """Render complete catalog entries while retaining the current selection."""

    lines = [*prefix, *(line for line, _current in entries)]
    rendered = "\n".join(lines)
    if len(rendered) <= _MAX_COMMAND_MARKDOWN:
        return rendered

    current_index = next(
        (index for index, (_line, current) in enumerate(entries) if current),
        None,
    )
    retained: list[str] = []
    retained_indexes: set[int] = set()
    for index, (line, _current) in enumerate(entries):
        proposed = [*retained, line]
        proposed_indexes = retained_indexes | {index}
        tail = [_LIST_TRUNCATION_MARKER]
        if current_index is not None and current_index not in proposed_indexes:
            tail.append(entries[current_index][0])
        candidate = "\n".join((*prefix, *proposed, *tail))
        if len(candidate) > _MAX_COMMAND_MARKDOWN:
            break
        retained = proposed
        retained_indexes = proposed_indexes

    tail = [_LIST_TRUNCATION_MARKER]
    if current_index is not None and current_index not in retained_indexes:
        tail.append(entries[current_index][0])
    return "\n".join((*prefix, *retained, *tail))


def _format_agent(record: Any, *, active: bool = False) -> str:
    agent_id = _bounded_public_value(
        _value(record, "agent_id", "id", default="?") or "?",
        max_length=_MAX_PUBLIC_ID,
    )
    display = _bounded_public_value(
        _value(record, "display_name", "name", default=agent_id) or agent_id,
        max_length=_MAX_PUBLIC_TEXT,
    )
    summary = _bounded_public_value(
        _value(record, "summary", default="") or "",
        max_length=_MAX_PUBLIC_TEXT,
    )
    marker = " **(current)**" if active else ""
    suffix = f": {summary}" if summary else ""
    process_suffix = ""
    if bool(_value(record, "process_isolated", default=False)):
        pid = _bounded_public_value(
            _value(record, "pid", default=None) or "pending",
            max_length=_MAX_PUBLIC_ID,
        )
        generation = _bounded_public_value(
            _value(record, "generation", default=None) or "pending",
            max_length=_MAX_PUBLIC_ID,
        )
        health = _bounded_public_value(
            _value(record, "health", default="unknown") or "unknown",
            max_length=_MAX_PUBLIC_ID,
        )
        process_suffix = (
            f" · process pid `{pid}`, generation `{generation}`, health `{health}`"
        )
    return f"- **`{agent_id}`**{marker} - {display}{suffix}{process_suffix}"


def _format_agents_markdown(records: Sequence[Any], *, active_agent: str) -> str:
    prefix = ["## Agents", ""]
    entries: list[tuple[str, bool]] = []
    current_listed = False
    normalized_active = str(active_agent or "").casefold()
    for record in records:
        record_id = str(_value(record, "agent_id", "id", default="") or "")
        is_current = (
            not current_listed
            and bool(record_id)
            and record_id.casefold() == normalized_active
        )
        if is_current:
            current_listed = True
        entries.append((_format_agent(record, active=is_current), is_current))
    if active_agent and not current_listed:
        agent_id = _bounded_public_value(active_agent, max_length=_MAX_PUBLIC_ID)
        entries.append(
            (f"- **`{agent_id}`** **(current)** **(unavailable)**", True)
        )
    elif not entries:
        entries.append(("_No Agents are available._", False))
    return _bounded_catalog_markdown(prefix, entries)


def _markdown_fence(content: Any, *, language: str = "text") -> str:
    """Wrap arbitrary output in a code fence longer than any embedded run."""

    value = str(content)
    longest = 0
    current = 0
    for character in value:
        if character == "`":
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    fence = "`" * max(3, longest + 1)
    return f"{fence}{language}\n{value}\n{fence}"


def _bounded_markdown_fence(
    content: Any,
    *,
    language: str,
    max_length: int,
) -> str:
    """Return a complete dynamic fence no longer than ``max_length``."""

    value = str(content)
    rendered = _markdown_fence(value, language=language)
    if len(rendered) <= max_length:
        return rendered

    def truncated(prefix_length: int) -> str:
        prefix = value[:prefix_length]
        separator = "" if not prefix or prefix.endswith("\n") else "\n"
        return _markdown_fence(
            prefix + separator + _CONTENT_TRUNCATION_MARKER,
            language=language,
        )

    low = 0
    high = len(value)
    while low < high:
        middle = (low + high + 1) // 2
        if len(truncated(middle)) <= max_length:
            low = middle
        else:
            high = middle - 1
    return truncated(low)


_SYSTEM_ROLE_RESPONSE_PREFIX = "system role:\n"


def _system_role_fragment_specs(content: str) -> tuple[dict[str, str], ...]:
    """Render role text as independently valid WeChat Markdown fragments.

    Reply-candidate fragmentation normally slices an opaque string every
    3,000 characters.  That is unsafe for a fenced role: fragment one can lose
    its closing fence while fragment two starts without an opening fence.  A
    role can also contain arbitrarily long backtick runs, so a fixed fence is
    insufficient.  Partition the *content* losslessly and choose a safe fence
    for each piece instead.
    """

    remaining = str(content)
    fragments: list[dict[str, str]] = []
    while remaining:
        heading = _SYSTEM_ROLE_RESPONSE_PREFIX if not fragments else ""
        available = _WECHAT_REPLY_TEXT_LIMIT - len(heading)
        if len(fragments) + 1 >= 10:
            available -= len(_REPLY_CONTINUATION_SUFFIX)
        low = 1
        high = len(remaining)
        best = 0
        while low <= high:
            middle = (low + high) // 2
            rendered = _markdown_fence(remaining[:middle], language="text")
            if len(rendered) <= available:
                best = middle
                low = middle + 1
            else:
                high = middle - 1
        if best <= 0:  # Defensive: even one scalar easily fits in 3,000.
            raise ValueError("system role cannot be represented as a reply fragment")
        piece = remaining[:best]
        fragments.append(
            {
                "kind": "text",
                "content": heading + _markdown_fence(piece, language="text"),
            }
        )
        remaining = remaining[best:]
    return tuple(fragments)


def _system_role_command_response(content: str) -> CommandResponse:
    """Return the complete receipt text plus its lossless wire presentation."""

    canonical = str(content)
    return CommandResponse(
        _SYSTEM_ROLE_RESPONSE_PREFIX + _markdown_fence(canonical, language="text"),
        response_fragments=_system_role_fragment_specs(canonical),
    )


def _system_role_fragments_from_response(
    response: str,
) -> tuple[dict[str, str], ...]:
    """Rebuild role fragments from a completed receipt after redelivery."""

    rendered = str(response or "")
    if not rendered.startswith(_SYSTEM_ROLE_RESPONSE_PREFIX):
        return ()
    fenced = rendered[len(_SYSTEM_ROLE_RESPONSE_PREFIX) :]
    opening, separator, remainder = fenced.partition("\n")
    if not separator or not opening.endswith("text"):
        return ()
    fence = opening[: -len("text")]
    if len(fence) < 3 or set(fence) != {"`"}:
        return ()
    closing = "\n" + fence
    if not remainder.endswith(closing):
        return ()
    content = remainder[: -len(closing)]
    if not content:
        return ()
    return _system_role_fragment_specs(content)


def _format_shell_markdown(command: str, result: Any) -> str:
    """Render the bounded legacy shell result without trusting its contents."""

    raw = str(result)
    first_line, separator, output = raw.partition("\n")
    exit_code = "unknown"
    if first_line.lower().startswith("exit code:"):
        candidate = first_line.split(":", 1)[1].strip()
        # A subprocess exit status is small.  Bounding this before ``int``
        # also avoids Python 3.10 accepting an arbitrarily long decimal that
        # would consume the Markdown budget in the trusted heading.
        if len(candidate) > 16 or re.fullmatch(r"[+-]?\d+", candidate) is None:
            output = raw
        else:
            exit_code = str(int(candidate))
            if not separator:
                output = "(no output)"
    else:
        output = raw
    prefix = (
        "## Shell Result\n\n"
        f"- **Exit code:** `{exit_code}`\n\n"
        "### Command\n\n"
    )
    output_heading = "\n\n### Output\n\n"
    minimum_output = _markdown_fence("", language="text")
    command_budget = min(
        _MAX_SHELL_COMMAND_MARKDOWN,
        _MAX_COMMAND_MARKDOWN
        - len(prefix)
        - len(output_heading)
        - len(minimum_output),
    )
    command_fence = _bounded_markdown_fence(
        command,
        language="sh",
        max_length=command_budget,
    )
    output_prefix = prefix + command_fence + output_heading
    output_fence = _bounded_markdown_fence(
        output or "(no output)",
        language="text",
        max_length=_MAX_COMMAND_MARKDOWN - len(output_prefix),
    )
    return output_prefix + output_fence


def _format_shell_error(message: Any) -> str:
    prefix = "## Shell Error\n\n"
    return prefix + _bounded_markdown_fence(
        message,
        language="text",
        max_length=_MAX_COMMAND_MARKDOWN - len(prefix),
    )


def _model_mapping(record: Any) -> Mapping[str, Any]:
    if isinstance(record, Mapping):
        return record
    for method_name in ("model_dump", "to_dict", "as_dict"):
        method = getattr(record, method_name, None)
        if callable(method):
            value = method()
            if isinstance(value, Mapping):
                return value
    return {}


def _model_id(record: Any) -> str:
    value = _value(record, "id", "model_id", "model", default="")
    return str(getattr(value, "value", value) or "").strip()


def _model_display_name(record: Any) -> str:
    return str(
        _value(record, "displayName", "display_name", "name", default="") or ""
    ).strip()


def _model_is_default(record: Any) -> bool:
    return bool(_value(record, "isDefault", "is_default", "default", default=False))


def _model_default_effort(record: Any) -> str:
    value = _value(
        record,
        "defaultReasoningEffort",
        "default_reasoning_effort",
        default="",
    )
    return str(getattr(value, "value", value) or "").strip()


def _model_efforts(record: Any) -> tuple[str, ...]:
    values = _value(
        record,
        "supportedReasoningEfforts",
        "supported_reasoning_efforts",
        "reasoning_efforts",
        default=(),
    )
    if isinstance(values, Mapping):
        values = values.values()
    efforts: list[str] = []
    for value in values or ():
        effort = (
            _value(value, "reasoningEffort", "reasoning_effort", "effort", default="")
            if isinstance(value, Mapping) or not isinstance(value, str)
            else value
        )
        normalized = str(getattr(effort, "value", effort) or "").strip()
        if normalized and normalized.lower() not in {item.lower() for item in efforts}:
            efforts.append(normalized)
    return tuple(efforts)


def _find_model(records: Sequence[Any], model_id: str) -> Any | None:
    requested = str(model_id or "").strip().lower()
    return next(
        (record for record in records if _model_id(record).lower() == requested),
        None,
    )


def _default_model(records: Sequence[Any]) -> Any | None:
    return next((record for record in records if _model_is_default(record)), None)


def _matching_effort(record: Any, effort: str) -> str | None:
    requested = str(effort or "").strip().lower()
    return next(
        (value for value in _model_efforts(record) if value.lower() == requested),
        None,
    )


def _model_selection(result: Any) -> tuple[str, str]:
    model_id = str(
        _value(result, "model_id", "model", "id", default="") or ""
    ).strip()
    effort = str(
        _value(result, "reasoning_effort", "effort", default="") or ""
    ).strip()
    if isinstance(result, (tuple, list)):
        if result:
            model_id = str(result[0] or "").strip()
        if len(result) > 1:
            effort = str(result[1] or "").strip()
    return model_id, effort


def _format_model_selection_markdown(
    records: Sequence[Any],
    *,
    agent_id: str,
    configured_model: str,
    configured_effort: str,
    heading: str = "Current Model",
) -> str:
    selected = _find_model(records, configured_model) if configured_model else _default_model(records)
    selected_id = _model_id(selected) if selected is not None else configured_model
    default_effort = _model_default_effort(selected) if selected is not None else ""
    effective_effort = configured_effort or default_effort or "model default"
    effort_source = "override" if configured_effort else "default"
    public_agent_id = _bounded_public_error(
        agent_id,
        max_length=_MAX_PUBLIC_ID,
    )
    public_model_id = _bounded_public_error(
        selected_id or "runtime default",
        max_length=_MAX_PUBLIC_ID,
    )
    public_effort = _bounded_public_error(
        effective_effort,
        max_length=_MAX_EFFORT_NAME,
    )
    return "\n".join(
        (
            f"## {heading}",
            "",
            f"- **Agent:** `{public_agent_id}`",
            f"- **Model:** `{public_model_id}`",
            f"- **Reasoning effort:** `{public_effort}` ({effort_source})",
            "- **Applies to:** future tasks",
        )
    )


def _format_models_markdown(
    records: Sequence[Any],
    *,
    agent_id: str,
    configured_model: str,
    configured_effort: str,
) -> str:
    selected = _find_model(records, configured_model) if configured_model else _default_model(records)
    prefix = [
        "## Models",
        "",
        "**Current Agent:** `"
        + _bounded_public_error(agent_id, max_length=_MAX_PUBLIC_ID)
        + "`",
        "",
    ]
    entries: list[tuple[str, bool]] = []
    if not records:
        if configured_model:
            model_id = _bounded_public_error(
                configured_model,
                max_length=_MAX_PUBLIC_ID,
            )
            effective_effort = _bounded_public_error(
                configured_effort or "model default",
                max_length=_MAX_EFFORT_NAME,
            )
            source = "override" if configured_effort else "default"
            entries.append(
                (
                    f"- **`{model_id}`** **(current)** **(unavailable)**. "
                    f"Efforts: not reported; current effort: `{effective_effort}` ({source}).",
                    True,
                )
            )
        else:
            entries.append(
                (
                    "- **`runtime default`** **(current)** **(unavailable)**. "
                    "Efforts: not reported; current effort: "
                    f"`{_bounded_public_error(configured_effort or 'model default', max_length=_MAX_EFFORT_NAME)}` "
                    f"({'override' if configured_effort else 'default'}).",
                    True,
                )
            )
        return _bounded_catalog_markdown(prefix, entries)
    for record in records:
        raw_model_id = _model_id(record) or "unknown"
        model_id = _bounded_public_error(
            raw_model_id,
            max_length=_MAX_PUBLIC_ID,
        )
        is_current = record is selected
        marker = " **(current)**" if is_current else ""
        if _model_is_default(record):
            marker += " **(default)**"
        display = _bounded_public_error(
            _model_display_name(record),
            max_length=_MAX_PUBLIC_TEXT,
        )
        efforts: list[str] = []
        effort_length = 0
        efforts_truncated = False
        for raw_effort in _model_efforts(record):
            effort = _bounded_public_error(raw_effort, max_length=_MAX_EFFORT_NAME)
            addition = len(effort) + 2 + (2 if efforts else 0)
            if effort_length + addition > _MAX_EFFORT_LIST:
                efforts_truncated = True
                break
            efforts.append(f"`{effort}`")
            effort_length += addition
        effort_text = ", ".join(efforts) or "not reported"
        if efforts_truncated:
            effort_text += ", ..."
        default_effort = _bounded_public_error(
            _model_default_effort(record),
            max_length=_MAX_EFFORT_NAME,
        )
        details = f"; default effort: `{default_effort}`" if default_effort else ""
        current_effort = ""
        if is_current:
            effective_effort = _bounded_public_error(
                configured_effort or default_effort or "model default",
                max_length=_MAX_EFFORT_NAME,
            )
            source = "override" if configured_effort else "default"
            current_effort = f"; current effort: `{effective_effort}` ({source})"
        display_text = f" - {display}" if display else ""
        entries.append(
            (
                f"- **`{model_id}`**{marker}{display_text}. Efforts: "
                f"{effort_text}{details}{current_effort}.",
                is_current,
            )
        )
    if configured_model and selected is None:
        # A durable preference can outlive a runtime catalog entry.  Do not
        # silently relabel the runtime default as current: future tasks still
        # carry the stored selection until the user changes it.  Keeping the
        # unavailable selection visible also preserves the `/models`
        # exactly-one-current-marker contract without mutating a read command.
        model_id = _bounded_public_error(
            configured_model,
            max_length=_MAX_PUBLIC_ID,
        )
        effective_effort = _bounded_public_error(
            configured_effort or "model default",
            max_length=_MAX_EFFORT_NAME,
        )
        source = "override" if configured_effort else "default"
        entries.append(
            (
                f"- **`{model_id}`** **(current)** **(unavailable)**. "
                f"Efforts: not reported; current effort: `{effective_effort}` ({source}).",
                True,
            )
        )
    elif not configured_model and selected is None:
        # Some runtimes report a catalog without flagging a default.  There
        # is still one effective selection: the runtime's opaque default.
        # Keep it explicit instead of marking an arbitrary catalog row.
        effective_effort = _bounded_public_error(
            configured_effort or "model default",
            max_length=_MAX_EFFORT_NAME,
        )
        source = "override" if configured_effort else "default"
        entries.append(
            (
                "- **`runtime default`** **(current)** **(unavailable)**. "
                "Efforts: not reported; current effort: "
                f"`{effective_effort}` ({source}).",
                True,
            )
        )
    return _bounded_catalog_markdown(prefix, entries)


def _format_modes_markdown(records: Sequence[Any], *, current_mode: str) -> str:
    prefix = ["## Modes", ""]
    entries: list[tuple[str, bool]] = []
    if not records:
        if current_mode:
            mode_id = _bounded_public_value(
                current_mode,
                max_length=_MAX_PUBLIC_ID,
            )
            entries.append(
                (f"- **`{mode_id}`** **(current)** **(unavailable)**", True)
            )
        else:
            entries.append(("_No modes are available._", False))
        return _bounded_catalog_markdown(prefix, entries)
    current_listed = False
    for record in records:
        raw_mode_id = str(
            _value(record, "mode_id", "id", "name", default="") or ""
        ).strip()
        if not raw_mode_id:
            continue
        mode_id = _bounded_public_value(raw_mode_id, max_length=_MAX_PUBLIC_ID)
        is_current = (
            not current_listed
            and raw_mode_id.lower() == current_mode.lower()
        )
        if is_current:
            current_listed = True
        marker = " **(current)**" if is_current else ""
        sandbox = _bounded_public_value(
            _value(record, "sandbox_policy", "sandbox", default="") or "",
            max_length=_MAX_PUBLIC_TEXT,
        )
        permissions: list[str] = []
        if sandbox:
            permissions.append(f"sandbox `{sandbox}`")
        if bool(_value(record, "can_write_files", default=False)):
            permissions.append("file writes")
        if bool(_value(record, "can_execute_commands", default=False)):
            permissions.append("commands")
        detail = f" - {', '.join(permissions)}" if permissions else ""
        entries.append((f"- **`{mode_id}`**{marker}{detail}", is_current))
    if current_mode and not current_listed:
        mode_id = _bounded_public_value(current_mode, max_length=_MAX_PUBLIC_ID)
        entries.append(
            (f"- **`{mode_id}`** **(current)** **(unavailable)**", True)
        )
    if not entries:
        entries.append(("_No modes are available._", False))
    return _bounded_catalog_markdown(prefix, entries)


def _format_inbox_item(record: Any) -> str:
    content = str(_value(record, "content", "text", default="") or "").strip()
    task_id = str(_value(record, "task_id", default="") or "")
    priority = _value(record, "priority", default=1)
    priority = int(getattr(priority, "value", priority))
    prefix = f"[{task_id}] " if task_id else ""
    if priority >= 3:
        prefix = "[attention] " + prefix
    elif priority >= 2:
        prefix = "[notify] " + prefix
    return f"- {prefix}{content or '(empty notification)'}"


def _inbox_ids(records: Sequence[Any]) -> tuple[str, ...]:
    """Extract durable outbox/item IDs from presentation candidates."""

    values: list[str] = []
    for record in records:
        identifier = _value(
            record,
            "outbox_id",
            "delivery_id",
            "reply_candidate_id",
            "presentation_id",
            "id",
            default="",
        )
        if identifier:
            values.append(str(identifier))
    return tuple(dict.fromkeys(values))


def _switch_back_fragment_specs(
    acknowledgement: str,
    records: Sequence[Any],
) -> tuple[dict[str, str], ...]:
    """Pack retained items into bounded terminal WeChat text messages.

    Completed items remain the durable presentation/deduplication boundary,
    but they are not forced to consume one ``SendMsg`` each.  Adjacent text is
    joined with exactly one blank line; a long individual item is continued
    losslessly without adding a synthetic separator inside that item.
    """

    logical_items = [
        str(acknowledgement) + "\n\nunseen messages:",
        *(_format_inbox_item(record) for record in records),
    ]
    fragments: list[dict[str, str]] = []
    current = ""

    def limit_for_next_fragment() -> int:
        return (
            _WECHAT_REPLY_TEXT_LIMIT
            if len(fragments) + 1 < 10
            else _WECHAT_REPLY_TEXT_LIMIT - len(_REPLY_CONTINUATION_SUFFIX)
        )

    def seal() -> None:
        nonlocal current
        if current:
            fragments.append({"kind": "text", "content": current})
            current = ""

    for logical_item in logical_items:
        remaining = str(logical_item)
        if not remaining:
            continue
        separator = "\n\n" if current else ""
        limit = limit_for_next_fragment()
        if current and len(current) + len(separator) + len(remaining) <= limit:
            current += separator + remaining
            continue
        if current:
            seal()
        # Keep an item whole when it fits an empty message.  Only an item that
        # is itself too large is divided; continuation chunks receive no item
        # separator because they are still one logical source item.
        while remaining:
            limit = limit_for_next_fragment()
            if len(remaining) <= limit:
                current = remaining
                remaining = ""
            else:
                fragments.append(
                    {"kind": "text", "content": remaining[:limit]}
                )
                remaining = remaining[limit:]
    seal()
    return tuple(fragments)


def _task_belongs_to(record: Any, envelope: InboundEnvelope) -> bool:
    """Check durable task ownership without exposing another user's task IDs.

    An explicit task ID is user-scoped, not session-scoped.  Session and
    active-Agent filtering belongs to the no-argument ``/cancel`` lookup; a
    user must still be able to control one of their tasks after switching
    sessions.
    """

    target = _value(record, "reply_target", "target", default=None)
    target_owner: Any = None
    if target is not None:
        target_value = (
            target.to_dict()
            if hasattr(target, "to_dict")
            else target.as_dict()
            if hasattr(target, "as_dict")
            else target
        )
        target_owner = _value(
            target_value,
            "external_user_id",
            "user_id",
            default=None,
        )
        if target_owner not in (None, ""):
            return (
                str(_value(target_value, "channel", default="") or "")
                == envelope.channel
                and str(_value(target_value, "bot_id", default="") or "")
                == envelope.bot_id
                and str(target_owner) == envelope.external_user_id
            )
    # Store task rows expose these columns even when no target projection was
    # materialized yet.
    actor_owner = _value(
        record,
        "actor_external_user_id",
        default=None,
    )
    owner = _value(record, "external_user_id", "user_id", default=None)
    # A row with neither a populated reply target nor explicit owner columns
    # cannot be authorized.  Treating empty defaults as a match would let any
    # user operate on a malformed/legacy task ID.
    if owner is None and actor_owner is None and target_owner in (None, ""):
        return False
    if actor_owner not in (None, ""):
        if str(actor_owner) != envelope.external_user_id:
            return False
    elif owner is not None:
        route_owner = str(
            getattr(envelope, "routing_subject_id", "")
            or envelope.external_user_id
        )
        if str(owner) != route_owner:
            return False
    channel = str(_value(record, "channel", default="") or "")
    bot_id = str(_value(record, "bot_id", default="") or "")
    return channel == envelope.channel and bot_id == envelope.bot_id

def _command_logger(channel: str) -> logging.Logger:
    """Keep command diagnostics under the owning channel adapter logger."""

    canonical = str(channel or "").strip().lower()
    if canonical == "feishu":
        canonical = "lark"
    if canonical in {"wechat", "lark"}:
        return logging.getLogger(f"{__package__}.{canonical}")
    return logger

class MVPCommandRouter:
    """Handle durable-task control commands through a manager/store facade."""

    def __init__(
        self,
        manager: Any,
        *,
        shell_cwd: str | Path | None = None,
        shell_runner: Callable[..., str] = run_shell_command,
    ) -> None:
        self.manager = manager
        self.shell_cwd = shell_cwd
        self.shell_runner = shell_runner

    async def handle_command(
        self,
        command: ChannelCommand,
        envelope: InboundEnvelope,
        *,
        command_id: str = "",
        existing_agent_only: bool = False,
    ) -> str | None:
        name = command.name
        command_logger = _command_logger(envelope.channel)
        if name == "__file_upload__":
            media_values = (
                envelope.raw.get("media", ())
                if isinstance(envelope.raw, Mapping)
                else ()
            )
            if isinstance(media_values, Mapping):
                media_values = (media_values,)
            files = [
                item
                for item in media_values
                if isinstance(item, Mapping)
                and str(item.get("kind", "")).strip().lower() == "file"
            ]
            labels: list[str] = []
            for ordinal, item in enumerate(files, start=1):
                filename = Path(str(item.get("filename", "") or "")).name
                filename = " ".join(filename.split()).replace("`", "\\`")
                labels.append(f"- `{filename or f'file {ordinal}'}`")
            heading = "File stored" if len(labels) == 1 else "Files stored"
            listing = "\n".join(labels) if labels else "- Uploaded file"
            pronoun = "it" if len(labels) == 1 else "them"
            return (
                f"## {heading}\n\n{listing}\n\n"
                f"What would you like me to do with {pronoun}?"
            )
        if name == "__skill_error__":
            return command.args[0] if command.args else "unknown skill: use /skills"
        if name == "__audio_error__":
            return (
                command.args[0]
                if command.args
                else "I couldn't transcribe that audio; please resend it or type the instruction."
            )
        if name == "__queue_full__":
            return "Agent queue is full; try again later."
        if name not in MVP_COMMANDS or not command_supported(
            name,
            channel=envelope.channel,
        ):
            # A slash-prefixed message is a control-plane input even when the
            # command is unsupported.  Returning a response here lets the
            # gateway durably record it without accidentally dispatching it to
            # an Agent as ordinary user text.
            return unsupported_command_response(name)

        # A command redelivery must execute against the route captured when its
        # inbound row was first accepted.  The gateway restores that durable
        # snapshot under the reserved raw-payload key.  Only standalone router
        # calls without a snapshot consult the live route.
        command_snapshot = (
            envelope.raw.get("__command_snapshot")
            if isinstance(envelope.raw, Mapping)
            else None
        )
        # Mutable conversation state is bot-local to the normalized subject.
        # For WeChat and direct chats this is exactly the historical actor ID;
        # Lark group/topic commands use their chat/thread subject while actor
        # arguments below continue to identify the authenticated sender.
        route_external_user_id = str(
            getattr(envelope, "routing_subject_id", "")
            or envelope.external_user_id
        )
        route_scope = {
            "channel": envelope.channel,
            "bot_id": envelope.bot_id,
            "external_user_id": route_external_user_id,
            "user_id": route_external_user_id,
            "session_id": envelope.session_id,
            "conversation_id": envelope.conversation_id,
        }
        snapshot_agent = (
            command_snapshot.get("agent_id")
            if isinstance(command_snapshot, Mapping)
            else None
        )
        if snapshot_agent:
            active_agent = snapshot_agent
        else:
            try:
                active_agent = await _invoke_compatible(
                    self.manager,
                    ("get_active_agent", "active_agent"),
                    keyword=route_scope,
                )
            except Exception:
                command_logger.debug("active Agent lookup unavailable; using envelope route", exc_info=True)
                active_agent = envelope.agent_id
        active_agent = _value(active_agent, "agent_id", "id", default=active_agent)
        active_agent = str(active_agent or envelope.agent_id or DEFAULT_AGENT_ID)
        if isinstance(command_snapshot, Mapping) and command_snapshot.get(
            "conversation_id"
        ):
            route_scope["conversation_id"] = str(command_snapshot["conversation_id"])
        scope = {**route_scope, "agent_id": active_agent}
        if name == "help":
            return (
                command_help_for_channel(envelope.channel)
                if not command.args
                else _command_usage(name)
            )

        if name == "cron":
            subcommand = command.args[0].casefold() if command.args else "help"
            ownership = {
                "principal_id": envelope.principal_id,
                "principal_account_id": envelope.principal_account_id,
                "channel": envelope.channel,
                "bot_id": envelope.bot_id,
                # Ownership is always the authenticated actor, never a group
                # or thread routing subject controlled by message metadata.
                "external_user_id": envelope.external_user_id,
            }
            if envelope.principal_mapping_revision is not None:
                ownership["principal_mapping_revision"] = (
                    envelope.principal_mapping_revision
                )
            if subcommand == "help":
                if len(command.args) != 1:
                    return _CRON_COMMAND_HELP if not command.args else _command_usage(name)
                return _CRON_COMMAND_HELP
            if subcommand == "list":
                if len(command.args) != 1:
                    return _command_usage(name)
                try:
                    jobs = await _invoke_compatible(
                        self.manager,
                        ("list_cron_jobs", "cron_jobs"),
                        keyword={**ownership, "enabled": True, "limit": 100},
                    )
                except AttributeError:
                    return "cron scheduling is unavailable"
                except (KeyError, PermissionError, ValueError, RuntimeError) as exc:
                    detail = _bounded_public_error(exc, max_length=_MAX_PUBLIC_TEXT)
                    return f"cannot list cron jobs: {detail or 'operation failed'}"
                return _format_cron_jobs(list(jobs or ()))
            if subcommand == "delete":
                if len(command.args) != 2:
                    return _command_usage(name)
                job_id = command.args[1].strip()
                try:
                    await _invoke_compatible(
                        self.manager,
                        ("delete_cron_job", "disable_cron_job"),
                        positional=(job_id,),
                        keyword=ownership,
                    )
                except AttributeError:
                    return "cron scheduling is unavailable"
                except (KeyError, PermissionError, ValueError, RuntimeError) as exc:
                    detail = _bounded_public_error(exc, max_length=_MAX_PUBLIC_TEXT)
                    return (
                        f"cannot delete cron job: "
                        f"{detail or 'operation failed'}"
                    )
                return f"cron job deleted: {job_id}"
            if subcommand == "add":
                parts = _cron_add_parts(command)
                if parts is None:
                    return _CRON_COMMAND_HELP
                schedule, prompt = parts
                job_id = _cron_job_id_for_command(command_id, envelope)
                try:
                    job = await _invoke_compatible(
                        self.manager,
                        ("add_cron_job", "create_cron_job"),
                        positional=(schedule, prompt),
                        keyword={
                            **ownership,
                            "job_id": job_id,
                            "session_id": envelope.session_id,
                            "conversation_subject_id": (
                                envelope.conversation_subject_id
                            ),
                            "conversation_subject_scope": (
                                envelope.conversation_subject_scope
                            ),
                            "conversation_subject_kind": (
                                envelope.conversation_subject_kind
                            ),
                            "reply_target": envelope.reply_target,
                            "agent_id": active_agent,
                            # The persisted inbound timestamp is stable across
                            # command replay.  In particular, an `at` job that
                            # was future-dated when accepted remains
                            # idempotently creatable after that instant passes.
                            "now": envelope.received_at,
                        },
                    )
                except AttributeError:
                    return "cron scheduling is unavailable"
                except QueueFullError:
                    return "cannot add cron job: queue is full"
                except (KeyError, PermissionError, ValueError, RuntimeError) as exc:
                    detail = _bounded_public_error(exc, max_length=_MAX_PUBLIC_TEXT)
                    return f"cannot add cron job: {detail or 'invalid schedule'}"
                resolved_id = str(
                    _value(job, "job_id", "id", default=job_id) or job_id
                )
                timezone_name = str(
                    _value(job, "timezone_name", "timezone", default="Asia/Shanghai")
                    or "Asia/Shanghai"
                )
                return (
                    f"cron job added: {resolved_id}; timezone: {timezone_name}"
                )
            return _CRON_COMMAND_HELP

        if name == "report":
            report = _command_message(command)
            if not report:
                return _command_usage(name)
            task_id: str | None = None
            try:
                active_tasks = await _invoke_compatible(
                    self.manager,
                    ("list_tasks", "tasks", "get_tasks"),
                    keyword={
                        **scope,
                        "states": (
                            "claimed",
                            "running",
                            "cancel_requested",
                        ),
                        "limit": 1,
                        "newest_first": True,
                    },
                )
            except Exception:
                active_tasks = ()
                command_logger.debug(
                    "active task lookup unavailable for operator report",
                    exc_info=True,
                )
            for item in active_tasks or ():
                if (
                    _task_state(item)
                    in {"claimed", "dispatching", "running", "cancel_requested"}
                    and str(
                        _value(item, "agent_id", default=active_agent)
                        or active_agent
                    )
                    == active_agent
                ):
                    task_id = _task_id(item) or None
                    break
            payload = {
                "ts": utc_now(),
                "agent_id": active_agent,
                "user": {
                    "external_user_id": envelope.external_user_id,
                    "bot_id": envelope.bot_id,
                },
                "task_id": task_id,
                "session": envelope.session_id or DEFAULT_SESSION_ID,
                "conversation": str(
                    route_scope.get("conversation_id")
                    or envelope.conversation_id
                    or ""
                ),
                "report": report,
            }
            command_logger.warning(
                "COW_OP_REPORT %s",
                json.dumps(
                    payload,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
            )
            return "report received"

        if name in {"skills", "listskill", "listskills"}:
            if command.args:
                return _command_usage(name)
            try:
                result = await _invoke_compatible(
                    self.manager,
                    ("list_skills", "skills"),
                    keyword={
                        **scope,
                        "agent_id": active_agent,
                        "refresh": False,
                        "workspace_snapshot": (
                            command_snapshot.get("execution_workspace")
                            if isinstance(command_snapshot, Mapping)
                            else None
                        ),
                    },
                )
            except Exception:
                command_logger.debug("skill listing unavailable", exc_info=True)
                return "skills unavailable; try again later"
            return format_skills_markdown(result)

        if name in {"clear", "reset"}:
            if command.args:
                return _command_usage(name)
            # ``/clear`` is a compatibility command, but it still has to use
            # the durable manager boundary.  A manager implementation can
            # clear its SQLite thread binding and reset the selected runtime
            # atomically under the per-session control lock.  The fallback is
            # retained for narrow legacy facades that expose only the runtime
            # reset method.
            try:
                result = await _invoke_compatible(
                    self.manager,
                    ("clear_session", "reset_session", "clear_conversation", "reset_conversation"),
                    keyword={
                        **scope,
                        "agent_id": active_agent,
                        "actor": envelope.external_user_id,
                    },
                )
            except AttributeError:
                runtime_target = None
                registry = getattr(self.manager, "registry", None)
                if registry is not None:
                    getter = getattr(registry, "require", None) or getattr(
                        registry, "runtime_for", None
                    )
                    if getter is not None:
                        try:
                            runtime_target = getter(active_agent)
                        except Exception:
                            runtime_target = None
                if runtime_target is None:
                    runtime_target = self.manager
                try:
                    result = await _invoke_compatible(
                        runtime_target,
                        ("reset_session", "clear_session", "clear_conversation"),
                        positional=(envelope.conversation_id,),
                        keyword={"conversation_id": envelope.conversation_id},
                    )
                except (AttributeError, KeyError, PermissionError, ValueError, RuntimeError) as exc:
                    detail = _bounded_public_error(exc, max_length=_MAX_PUBLIC_TEXT)
                    return (
                        "cannot clear conversation: "
                        f"{detail or 'operation failed'}"
                    )
            except (KeyError, PermissionError, ValueError, RuntimeError) as exc:
                detail = _bounded_public_error(exc, max_length=_MAX_PUBLIC_TEXT)
                return (
                    "cannot clear conversation: "
                    f"{detail or 'operation failed'}"
                )
            # Do not expose a provider-specific thread identifier in the
            # command response; it is an implementation detail and may be
            # rotated on the next task claim.
            return "context cleared, starting a new conversation"

        if name == "compact":
            if command.args:
                return _command_usage(name)
            # Manual compaction belongs at the durable manager boundary so it
            # shares the per-session control lock with route changes, model
            # changes, and `/clear`.  The manager also owns persistence of any
            # exact provider thread binding.
            try:
                result = await _invoke_compatible(
                    self.manager,
                    ("compact_session", "compact_conversation"),
                    keyword={
                        **scope,
                        "agent_id": active_agent,
                        "actor": envelope.external_user_id,
                    },
                )
            except AttributeError:
                return "cannot compact conversation: context compaction is unavailable"
            except (KeyError, PermissionError, ValueError, RuntimeError) as exc:
                detail = _bounded_public_error(exc, max_length=_MAX_PUBLIC_TEXT)
                return (
                    "cannot compact conversation: "
                    f"{detail or 'operation failed'}"
                )
            # The pinned SDK reports request acceptance before asynchronous
            # compaction finishes.  A runtime without the public thread-read
            # confirmation surface therefore receives a truthful "started"
            # acknowledgement instead of a false completion claim.
            if (
                isinstance(result, Mapping)
                and result.get("completion_confirmed") is False
            ):
                return "context compaction started"
            # Keep the provider-specific thread identifier out of the channel
            # acknowledgement; in-place compaction preserves that identity.
            return "context compacted"

        if name == "system":
            # Role content is an exact raw-tail contract.  Consume only the
            # command token and its horizontal separator; normalization owns
            # outer Unicode whitespace while preserving internal layout.
            raw_command = str(command.raw or "")
            match = re.match(
                r"^\s*/system(?=$|\s)",
                raw_command,
                flags=re.IGNORECASE,
            )
            if match is None:
                # ``ChannelCommand.raw`` is required for this command because
                # whitespace and newlines in the role are semantically
                # significant.  Compatibility callers must not accidentally
                # turn a malformed/missing raw command into a read request.
                return "invalid system command"
            raw_tail = raw_command[match.end() :]
            raw_tail = re.sub(r"^[ \t]*", "", raw_tail, count=1)
            try:
                canonical = normalize_role_text(raw_tail)
            except RoleValidationError as exc:
                detail = _bounded_public_error(exc, max_length=_MAX_PUBLIC_TEXT)
                return f"invalid system role: {detail or 'invalid role'}"

            if not canonical:
                try:
                    value = await _invoke_compatible(
                        self.manager,
                        ("get_system_role", "get_session_role", "get_role"),
                        keyword=scope,
                    )
                    role = validate_role_snapshot(value)
                except AttributeError:
                    return "system role is unavailable"
                except RoleValidationError:
                    return "cannot get system role: invalid stored role"
                except (KeyError, PermissionError, ValueError, RuntimeError) as exc:
                    detail = _bounded_public_error(exc, max_length=_MAX_PUBLIC_TEXT)
                    return f"cannot get system role: {detail or 'operation failed'}"
                if role["kind"] == "default":
                    return "system role: default"
                return _system_role_command_response(role["normalized_content"])

            requested_kind = (
                "default" if is_default_role_token(canonical) else "custom"
            )
            role_text = "" if requested_kind == "default" else canonical
            try:
                role_keywords = {
                    **scope,
                    "kind": requested_kind,
                    "actor": envelope.external_user_id,
                }
                if command_id:
                    # Only the gateway receipt owner supplies this correlation.
                    # Standalone router calls intentionally omit it and retain
                    # the compatibility manager/store path.
                    role_keywords["command_id"] = str(command_id)
                value = await _invoke_compatible(
                    self.manager,
                    ("set_system_role", "set_session_role", "set_role"),
                    positional=(role_text,),
                    keyword=role_keywords,
                )
                if not isinstance(value, Mapping):
                    raise RuntimeError("invalid role persistence response")
                role = validate_role_snapshot(value)
                changed = bool(value.get("changed", True))
            except AttributeError:
                return "system role is unavailable"
            except RoleValidationError as exc:
                detail = _bounded_public_error(exc, max_length=_MAX_PUBLIC_TEXT)
                return f"invalid system role: {detail or 'invalid role'}"
            except (KeyError, PermissionError, ValueError, RuntimeError) as exc:
                detail = _bounded_public_error(exc, max_length=_MAX_PUBLIC_TEXT)
                return f"cannot set system role: {detail or 'operation failed'}"
            response = (
                "system role: unchanged"
                if not changed
                else (
                    "system role: default"
                    if role["kind"] == "default"
                    else "system role: updated"
                )
            )
            stored_response = value.get("command_response")
            if stored_response is not None and (
                not isinstance(stored_response, str)
                or stored_response != response
            ):
                return "cannot set system role: invalid persistence response"
            return stored_response or response

        if name == "mode":
            if len(command.args) > 1:
                return _command_usage(name)
            if not command.args:
                try:
                    mode = await _invoke_compatible(
                        self.manager,
                        ("get_mode", "mode"),
                        keyword=scope,
                    )
                except AttributeError:
                    return "mode selection is unavailable"
                except (KeyError, PermissionError, ValueError, RuntimeError) as exc:
                    # A malformed or revoked persisted mode must produce a
                    # durable command response rather than escaping through
                    # the gateway and leaving the inbound command unhandled.
                    detail = _bounded_public_error(exc, max_length=_MAX_PUBLIC_TEXT)
                    return f"cannot get mode: {detail or 'operation failed'}"
                return f"mode: {mode}"
            requested_mode = command.args[0].strip().lower()
            if requested_mode not in {"chat", "plan", "review", "execute"}:
                return _command_usage(name)
            try:
                mode = await _invoke_compatible(
                    self.manager,
                    ("set_mode", "switch_mode"),
                    positional=(requested_mode,),
                    keyword={
                        **scope,
                        "actor": envelope.external_user_id,
                        "explicit": True,
                    },
                )
            except (AttributeError, KeyError, PermissionError, ValueError, RuntimeError) as exc:
                detail = _bounded_public_error(exc, max_length=_MAX_PUBLIC_TEXT)
                return f"cannot set mode: {detail or 'operation failed'}"
            return f"mode: {mode}"

        if name == "modes":
            if command.args:
                return _command_usage(name)
            try:
                modes = await _invoke_compatible(
                    self.manager,
                    ("list_modes", "modes"),
                    keyword={"agent_id": active_agent},
                )
                current_mode = await _invoke_compatible(
                    self.manager,
                    ("get_mode", "mode"),
                    keyword=scope,
                )
            except AttributeError:
                return "mode registry is unavailable"
            except (KeyError, PermissionError, ValueError, RuntimeError) as exc:
                detail = _bounded_public_error(exc, max_length=_MAX_PUBLIC_TEXT)
                return f"cannot list modes: {detail or 'operation failed'}"
            return _format_modes_markdown(
                list(modes or ()), current_mode=str(current_mode or "")
            )

        if name in {"model", "models"}:
            if name == "models" and command.args:
                return _command_usage(name)
            if name == "model" and (
                len(command.args) > 2
                or (len(command.args) == 1 and command.args[0].lower() == "effort")
            ):
                return _command_usage(name)
            clears_effort = (
                name == "model"
                and len(command.args) == 2
                and command.args[0].lower() == "effort"
                and command.args[1].lower() == "default"
            )
            if clears_effort:
                # Resetting to the runtime/model default is valid without a
                # live catalog. Dispatch it directly so model discovery cannot
                # prevent or delay clearing the durable override.
                try:
                    selection = await _invoke_compatible(
                        self.manager,
                        ("set_reasoning_effort",),
                        positional=(command.args[1],),
                        keyword={
                            **scope,
                            "reasoning_effort": command.args[1],
                        },
                    )
                except AttributeError:
                    return "model selection is unavailable"
                except ValidationError as exc:
                    command_logger.warning(
                        "cannot reset model effort after invalid runtime response: %s",
                        _bounded_public_error(exc, max_length=_MAX_PUBLIC_TEXT),
                    )
                    return "cannot set model: model service is unavailable"
                except (KeyError, PermissionError):
                    return "model selection is unavailable"
                except ValueError as exc:
                    return _format_model_capability_error("set", exc)
                except RuntimeError as exc:
                    command_logger.warning(
                        "cannot reset model effort through runtime: %s",
                        _bounded_public_error(exc, max_length=_MAX_PUBLIC_TEXT),
                    )
                    return "cannot set model: model service is unavailable"
                except Exception as exc:
                    command_logger.warning(
                        "cannot reset model effort through runtime: %s",
                        _bounded_public_error(exc, max_length=_MAX_PUBLIC_TEXT),
                    )
                    return "cannot set model: model service is unavailable"
                selected_model, selected_effort = _model_selection(selection)
                return _format_model_selection_markdown(
                    (),
                    agent_id=active_agent,
                    configured_model=selected_model,
                    configured_effort=selected_effort,
                )
            try:
                models = list(
                    await _invoke_compatible(
                        self.manager,
                        ("list_models", "models"),
                        keyword={"agent_id": active_agent, "include_hidden": False},
                    )
                    or ()
                )
                if name == "model" and command.args:
                    requested = command.args[0]
                    effort = command.args[1] if len(command.args) == 2 else "default"
                    if requested.lower() == "effort":
                        selection = await _invoke_compatible(
                            self.manager,
                            ("set_reasoning_effort",),
                            positional=(effort,),
                            keyword={**scope, "reasoning_effort": effort},
                        )
                    else:
                        selection = await _invoke_compatible(
                            self.manager,
                            ("set_model", "select_model"),
                            positional=(requested,),
                            keyword={
                                **scope,
                                "model_id": requested,
                                "reasoning_effort": effort,
                            },
                        )
                else:
                    selection = await _invoke_compatible(
                        self.manager,
                        ("get_model_selection", "get_model_preference"),
                        keyword=scope,
                    )
            except AttributeError:
                return "model selection is unavailable"
            except ValidationError as exc:
                action = "set" if name == "model" and command.args else "list"
                command_logger.warning(
                    "cannot %s model after invalid runtime response: %s",
                    action,
                    _bounded_public_error(exc, max_length=_MAX_PUBLIC_TEXT),
                )
                return f"cannot {action} model: model service is unavailable"
            except (KeyError, PermissionError):
                return "model selection is unavailable"
            except ValueError as exc:
                action = "set" if name == "model" and command.args else "list"
                return _format_model_capability_error(action, exc)
            except RuntimeError as exc:
                action = "set" if name == "model" and command.args else "list"
                command_logger.warning(
                    "cannot %s model through runtime: %s",
                    action,
                    _bounded_public_error(exc, max_length=_MAX_PUBLIC_TEXT),
                )
                return f"cannot {action} model: model service is unavailable"
            except Exception as exc:
                # SDK transport/RPC failures are ordinary Exceptions. Convert
                # them into a deterministic command result so the durable
                # receipt completes and redelivery never reports an ambiguous
                # unknown outcome. Cancellation remains outside this boundary.
                action = "set" if name == "model" and command.args else "list"
                command_logger.warning(
                    "cannot %s model through runtime: %s",
                    action,
                    _bounded_public_error(exc, max_length=_MAX_PUBLIC_TEXT),
                )
                return f"cannot {action} model: model service is unavailable"
            selected_model, selected_effort = _model_selection(selection)
            if name == "models":
                return _format_models_markdown(
                    models,
                    agent_id=active_agent,
                    configured_model=selected_model,
                    configured_effort=selected_effort,
                )
            return _format_model_selection_markdown(
                models,
                agent_id=active_agent,
                configured_model=selected_model,
                configured_effort=selected_effort,
            )

        if name == "cd":
            # A path is one shell-like token so quoted spaces remain usable,
            # while the raw command remains the authority instead of the
            # generic whitespace-normalized ``ChannelCommand.args`` tuple.
            raw_command = str(command.raw or "")
            match = re.match(r"^\s*/cd(?=$|\s)", raw_command, flags=re.IGNORECASE)
            if match is None:
                return _command_usage(name)
            raw_tail = raw_command[match.end() :].strip()
            if not raw_tail:
                try:
                    value = await _invoke_compatible(
                        self.manager,
                        ("get_working_directory",),
                        keyword={
                            **scope,
                            "workspace_snapshot": (
                                command_snapshot.get("execution_workspace")
                                if isinstance(command_snapshot, Mapping)
                                else None
                            ),
                        },
                    )
                    _cwd, display_path = _working_directory_path(value)
                    return _format_working_directory(display_path)
                except AttributeError:
                    return "working directory is unavailable"
                except Exception as exc:
                    detail = _bounded_public_error(exc, max_length=_MAX_PUBLIC_TEXT)
                    return (
                        "cannot get working directory: "
                        f"{detail or 'operation failed'}"
                    )

            try:
                path_values = shlex.split(raw_tail, posix=True)
            except ValueError:
                return _command_usage(name)
            if len(path_values) != 1 or not path_values[0]:
                return _command_usage(name)
            requested_path = path_values[0]
            try:
                set_keywords = {
                    **scope,
                    "actor": envelope.external_user_id,
                }
                if command_id:
                    set_keywords["command_id"] = str(command_id)
                if isinstance(command_snapshot, Mapping):
                    set_keywords["workspace_snapshot"] = command_snapshot.get(
                        "execution_workspace"
                    )
                value = await _invoke_compatible(
                    self.manager,
                    ("set_working_directory",),
                    positional=(requested_path,),
                    keyword=set_keywords,
                )
                if not isinstance(value, Mapping):
                    raise ValueError("invalid working directory persistence response")
                _cwd, display_path = _working_directory_path(
                    value,
                    fallback=requested_path,
                )
                stored_response = value.get("command_response")
                if stored_response is not None and not isinstance(
                    stored_response, str
                ):
                    raise ValueError("invalid working directory persistence response")
                response = _format_working_directory(display_path)
                if stored_response is not None and stored_response != response:
                    raise ValueError("invalid working directory persistence response")
                return response
            except AttributeError:
                return "working directory is unavailable"
            except Exception as exc:
                detail = _bounded_public_error(exc, max_length=_MAX_PUBLIC_TEXT)
                return (
                    "cannot set working directory: "
                    f"{detail or 'operation failed'}"
                )

        if name == "sh":
            # ``ChannelCommand.args`` is intentionally whitespace-normalized
            # for most controls.  `/sh` is different: preserve the original
            # shell text so quoting and deliberate spacing retain the legacy
            # command's behavior.
            raw_command = str(command.raw or "").strip()
            command_text = (
                raw_command[3:].strip()
                if raw_command[:3].lower() == "/sh"
                else command.argument.strip()
            )
            if not command_text:
                return _command_usage(name)

            shell_cwd: str | Path | None = self.shell_cwd
            working_directory_reader = getattr(
                self.manager, "get_working_directory", None
            )
            if callable(working_directory_reader):
                workspace_snapshot = (
                    command_snapshot.get("execution_workspace")
                    if isinstance(command_snapshot, Mapping)
                    else None
                )
                try:
                    directory = await _invoke_compatible(
                        self.manager,
                        ("get_working_directory",),
                        keyword={
                            **scope,
                            "workspace_snapshot": workspace_snapshot,
                        },
                    )
                    shell_cwd, _display_path = _working_directory_path(directory)
                except Exception as exc:
                    detail = _bounded_public_error(exc, max_length=_MAX_PUBLIC_TEXT)
                    return _format_shell_error(
                        "cannot get working directory: "
                        f"{detail or 'operation failed'}"
                    )

            def invoke_shell() -> str:
                # Inspect the injected helper before invoking it so a genuine
                # TypeError raised by the command itself is not mistaken for
                # a narrow legacy signature and executed twice.
                try:
                    signature = inspect.signature(self.shell_runner)
                except (TypeError, ValueError):
                    return self.shell_runner(command_text, cwd=shell_cwd)
                accepts_var_kw = any(
                    parameter.kind == inspect.Parameter.VAR_KEYWORD
                    for parameter in signature.parameters.values()
                )
                if accepts_var_kw or "cwd" in signature.parameters:
                    return self.shell_runner(command_text, cwd=shell_cwd)
                return self.shell_runner(command_text)

            try:
                result = await asyncio.to_thread(invoke_shell)
            except subprocess.TimeoutExpired:
                return _format_shell_error(
                    f"shell command timed out after {_SHELL_TIMEOUT} seconds"
                )
            except OSError as exc:
                detail = _bounded_public_error(exc, max_length=_MAX_PUBLIC_TEXT)
                return _format_shell_error(
                    f"shell command failed to start: {detail or 'operation failed'}"
                )
            except Exception as exc:
                detail = _bounded_public_error(exc, max_length=_MAX_PUBLIC_TEXT)
                return _format_shell_error(
                    f"shell command failed: {detail or 'operation failed'}"
                )
            return _format_shell_markdown(command_text, result)

        if name == "ask":
            if len(command.args) < 2:
                return _command_usage(name)
            destination = command.args[0].strip()
            # ``ChannelCommand.args`` is whitespace-normalized for control
            # syntax. The prompt is user input, so split only the command and
            # destination tokens and preserve the remainder verbatim (apart
            # from surrounding whitespace).
            raw_command = str(command.raw or "")
            ask_match = re.match(
                r"^\s*/ask\s+(\S+)(?:\s+([\s\S]*))?$",
                raw_command,
                flags=re.IGNORECASE,
            )
            prompt = (
                (ask_match.group(2) or "").strip()
                if ask_match is not None
                else " ".join(command.args[1:]).strip()
            )
            if not prompt:
                return _command_usage(name)
            request_id = command_request_id(envelope)
            task_id = command_task_id(envelope)
            acknowledgement = f"Agent task queued: {task_id}"
            submit_keyword: dict[str, Any] = {
                "task_id": task_id,
                "agent_id": destination,
                "actor": envelope.external_user_id,
                "explicit": True,
                "request_id": request_id,
                "dedupe_key": f"command-ask:{request_id}",
                "metadata": {
                    "direct_user_request": True,
                    "requesting_agent_id": active_agent,
                    "user_reply_format": USER_REPLY_FORMAT_AGENT_PREFIX_V1,
                },
            }
            initial_reply = _slotful_command_initial_reply(
                envelope,
                acknowledgement,
                agent_id=active_agent,
            )
            if initial_reply is not None:
                # WeChat must reserve the acknowledgement and reply ordinal
                # before the queued row becomes visible to any dispatcher.
                # Slotless account adapters durably publish their response
                # after the command receipt is completed instead.
                submit_keyword["initial_reply"] = initial_reply
            try:
                await _invoke_compatible(
                    self.manager,
                    ("ensure_agent",),
                    positional=(destination,),
                    keyword={"agent_id": destination},
                )
                result = await _invoke_compatible(
                    self.manager,
                    ("submit", "enqueue", "create_task", "queue_task"),
                    positional=(prompt, envelope.reply_target),
                    keyword=submit_keyword,
                )
            except QueueFullError:
                return "cannot ask Agent: queue is full"
            except (AttributeError, KeyError, PermissionError, ValueError, RuntimeError) as exc:
                detail = _bounded_public_error(exc, max_length=_MAX_PUBLIC_TEXT)
                return f"cannot ask Agent: {detail or 'operation failed'}"
            resolved_task_id = _task_id(result) or task_id
            if resolved_task_id != task_id:
                # A pre-framed legacy dedupe winner may carry a different
                # random ID.  The atomic store rejects a new acknowledgement
                # in that case; fail closed for narrower compatibility stores
                # rather than telling the user about a task other than the
                # one named in the reserved reply.
                return "cannot ask Agent: task identity conflicts"
            return acknowledgement

        if name == "agents":
            if command.args:
                return _command_usage(name)
            try:
                result = await _invoke_compatible(
                    self.manager,
                    ("list_agents", "agents", "list_profiles"),
                    keyword={},
                )
            except AttributeError:
                return "Agent registry is unavailable"
            records = list(result or [])
            try:
                active = await _invoke_compatible(
                    self.manager,
                    ("get_active_agent", "active_agent"),
                    keyword={
                        "channel": envelope.channel,
                        "bot_id": envelope.bot_id,
                        "external_user_id": route_external_user_id,
                        "session_id": envelope.session_id,
                    },
                )
            except AttributeError:
                active = active_agent
            return _format_agents_markdown(records, active_agent=str(active))

        if name == "delagent":
            if len(command.args) not in {1, 2} or (
                len(command.args) == 2
                and command.args[1].strip().casefold() != "force"
            ):
                return _command_usage(name)
            selected = command.args[0].strip().lower()
            force = len(command.args) == 2
            try:
                result = await _invoke_compatible(
                    self.manager,
                    (
                        ("force_delete_agent",)
                        if force
                        else ("delete_agent", "delagent", "remove_agent")
                    ),
                    positional=(selected,),
                    keyword={
                        "channel": envelope.channel,
                        "bot_id": envelope.bot_id,
                        "external_user_id": route_external_user_id,
                        "session_id": envelope.session_id,
                    },
                )
            except (AttributeError, KeyError, PermissionError, ValueError, RuntimeError) as exc:
                detail = _bounded_public_error(exc, max_length=_MAX_PUBLIC_TEXT)
                return f"cannot delete Agent: {detail or 'operation failed'}"
            if force:
                task_ids = tuple(
                    str(task_id)
                    for task_id in (
                        _value(result, "cancelled_task_ids", default=()) or ()
                    )
                )
                mailbox_ids = tuple(
                    str(mailbox_id)
                    for mailbox_id in (
                        _value(result, "rejected_mailbox_ids", default=()) or ()
                    )
                )
                if task_ids:
                    task_detail = (
                        f"force-cancelled {len(task_ids)} unfinished task(s): "
                        + ", ".join(task_ids)
                    )
                else:
                    task_detail = "force-cancelled 0 unfinished tasks"
                mailbox_detail = (
                    f"; rejected {len(mailbox_ids)} queued Agent message(s)"
                    if mailbox_ids
                    else ""
                )
                return (
                    f"Agent force-deleted: {selected}; {task_detail}"
                    f"{mailbox_detail}"
                )
            return f"Agent deleted: {selected}"

        if name == "agent":
            if len(command.args) > 2:
                return _command_usage(name)
            if not command.args:
                try:
                    active = await _invoke_compatible(
                        self.manager,
                        ("get_active_agent", "active_agent"),
                        keyword={
                            "channel": envelope.channel,
                            "bot_id": envelope.bot_id,
                            "external_user_id": route_external_user_id,
                            "session_id": envelope.session_id,
                        },
                    )
                except AttributeError:
                    active = active_agent
                active = _value(active, "agent_id", "id", default=active)
                return f"active Agent: {active}"
            # Agent IDs are canonicalized by the manager so routes and
            # conversation identities remain stable across casing variants.
            selected = command.args[0].strip().lower()
            codex_config_profile = (
                command.args[1].strip() if len(command.args) == 2 else None
            )
            try:
                switch_methods = (
                    ("set_existing_active_agent",)
                    if existing_agent_only
                    else ("set_active_agent", "switch_agent", "set_agent")
                )
                await _invoke_compatible(
                    self.manager,
                    switch_methods,
                    positional=(selected,),
                    keyword={
                        "codex_config_profile": codex_config_profile,
                        "channel": envelope.channel,
                        "bot_id": envelope.bot_id,
                        "external_user_id": route_external_user_id,
                        "session_id": envelope.session_id,
                    },
                )
            except (AttributeError, KeyError, PermissionError, ValueError, RuntimeError) as exc:
                detail = _bounded_public_error(exc, max_length=_MAX_PUBLIC_TEXT)
                return f"cannot switch Agent: {detail or 'operation failed'}"
            # Only completed items retained while this Agent was in the
            # background are eligible here.  The manager deliberately keeps
            # allocated/sent history and `/recv` quota deferrals off this
            # surface, so switching cannot replay a transcript.
            try:
                unseen = list(
                    await _invoke_compatible(
                        self.manager,
                        ("switch_back_inbox",),
                        keyword={
                            "channel": envelope.channel,
                            "bot_id": envelope.bot_id,
                            "external_user_id": route_external_user_id,
                            "session_id": envelope.session_id,
                            "agent_id": selected,
                            "limit": 100,
                            "present": False,
                        },
                    )
                    or ()
                )
            except AttributeError:
                # Narrow compatibility managers predate durable candidate
                # presentation; retain their route-only behavior.
                unseen = []
            response = f"switched to Agent: {selected}"
            if not unseen:
                return response
            formatted = tuple(_format_inbox_item(item) for item in unseen)
            return CommandResponse(
                response
                + "\n\nunseen messages:\n"
                + "\n".join(formatted),
                _inbox_ids(unseen),
                response_fragments=_switch_back_fragment_specs(response, unseen),
            )

        if name == "notify":
            if len(command.args) > 1 or (
                command.args and command.args[0].lower() not in {"on", "off"}
            ):
                return _command_usage(name)
            if not command.args:
                try:
                    enabled = await _invoke_compatible(
                        self.manager,
                        ("get_notify", "get_notification_preference"),
                        keyword=scope,
                    )
                except AttributeError:
                    return "notification preferences are unavailable"
                return f"notifications: {'on' if bool(enabled) else 'off'}"
            enabled = command.args[0].lower() == "on"
            try:
                result = await _invoke_compatible(
                    self.manager,
                    ("set_notify", "set_notification_preference", "set_notify_preference"),
                    positional=(enabled,),
                    keyword=scope,
                )
            except (AttributeError, ValueError, PermissionError, RuntimeError) as exc:
                detail = _bounded_public_error(exc, max_length=_MAX_PUBLIC_TEXT)
                return f"cannot set notifications: {detail or 'operation failed'}"
            return f"notifications: {'on' if bool(result) else 'off'}"

        if name == "inbox":
            if len(command.args) > 1:
                return _command_usage(name)
            requested = command.args[0] if command.args else active_agent
            if requested.lower() == "all":
                try:
                    agents = await _invoke_compatible(
                        self.manager,
                        ("list_agents", "agents"),
                        keyword={},
                    )
                except AttributeError:
                    agents = []
                agent_ids = [
                    str(_value(item, "agent_id", "id", default=""))
                    for item in list(agents or [])
                ]
            else:
                agent_ids = [requested]
            records: list[Any] = []
            for agent_id in agent_ids:
                try:
                    result = await _invoke_compatible(
                        self.manager,
                        ("inbox", "present_inbox", "present_notifications"),
                        keyword={
                            **scope,
                            "agent_id": agent_id,
                            "limit": 100,
                            "present": False,
                            "include_command_responses": False,
                        },
                    )
                except AttributeError:
                    result = []
                records.extend(list(result or []))
            if not records:
                return "inbox: (empty)"
            return CommandResponse(
                "inbox:\n" + "\n".join(_format_inbox_item(item) for item in records),
                _inbox_ids(records),
            )

        if name == "recv":
            if command.args:
                return _command_usage(name)
            try:
                projection = await _invoke_compatible(
                    self.manager,
                    (
                        "drain_deferred_replies",
                        "receive_deferred_replies",
                        "drain_reply_overflow",
                    ),
                    keyword={
                        "target": envelope.reply_target.to_dict(),
                        # The full compound command identity is stable across
                        # channel redelivery and cannot collide across bots,
                        # users, sessions, or delimiter-bearing message IDs.
                        "source_key": command_delivery_id(envelope),
                        "limit": 10,
                    },
                )
            except AttributeError:
                return "reply continuation is unavailable"
            except (KeyError, PermissionError, ValueError, RuntimeError) as exc:
                detail = _bounded_public_error(exc, max_length=_MAX_PUBLIC_TEXT)
                return (
                    "cannot receive deferred replies: "
                    f"{detail or 'operation failed'}"
                )
            outbox_items = tuple(
                _value(projection, "outbox_items", default=()) or ()
            )
            if not outbox_items:
                return "no deferred replies"
            # The allocator already created one canonical outbox send per
            # reply slot.  Returning another command string here would spend
            # an extra slot and could overtake the retained FIFO batch.
            return CommandResponse("")

        if name == "status":
            if command.args:
                return _command_usage(name)
            try:
                result = await _invoke_compatible(
                    self.manager,
                    ("status", "get_status", "task_status"),
                    keyword=scope,
                )
            except AttributeError:
                return "status is unavailable"
            if isinstance(result, str):
                return result
            if result is None:
                return "idle"
            if isinstance(result, Sequence) and not isinstance(
                result, (str, bytes, bytearray)
            ):
                records = list(result)
                # Facades other than TaskManager may return the full task
                # history.  `/status` is intentionally an active-work query;
                # completed/failed rows belong under `/tasks`.
                records = [
                    item
                    for item in records
                    if not _task_state(item) or _task_state(item) in ACTIVE_TASK_STATES
                ]
                if not records:
                    return "idle"
                return "active tasks:\n" + "\n".join(
                    f"- {_format_task(item)}" for item in records
                )
            if _task_state(result) in TERMINAL_TASK_STATES:
                return "idle"
            return _format_task(result)

        if name == "tasks":
            if len(command.args) > 1 or (
                command.args and not command.args[0].isdigit()
            ):
                return _command_usage(name)
            limit = int(command.args[0]) if command.args else 20
            limit = min(max(limit, 1), 100)
            try:
                result = await _invoke_compatible(
                    self.manager,
                    ("list_tasks", "tasks", "get_tasks"),
                    keyword={**scope, "limit": limit},
                )
            except AttributeError:
                return "tasks are unavailable"
            records = list(result or [])
            if not records:
                return "tasks: (none)"
            return "tasks:\n" + "\n".join(
                f"- {_format_task(item)}" for item in records
            )

        if name == "cancel" and not command.args:
            # Conversation serialization normally guarantees one running task
            # for this Agent route. Fail closed if a corrupt/legacy store
            # reports more than one instead of interrupting an arbitrary turn.
            try:
                active = await _invoke_compatible(
                    self.manager,
                    ("list_tasks", "tasks", "get_tasks"),
                    keyword={
                        **scope,
                        "states": ("running",),
                        "limit": 2,
                        "newest_first": True,
                    },
                )
            except AttributeError:
                active = []
            active_records = [
                item
                for item in list(active or [])
                if _task_state(item) == "running"
                and str(_value(item, "agent_id", default=active_agent) or active_agent)
                == active_agent
            ]
            if len(active_records) == 1:
                selected_id = _task_id(active_records[0])
                if not selected_id:
                    return "no running task for current Agent"
                command = ChannelCommand(name="cancel", args=(selected_id,), raw=command.raw)
            elif not active_records:
                return "no running task for current Agent"
            else:
                return "multiple running tasks; use /cancel <task-id>"

        if len(command.args) != 1:
            if name == "cancel":
                return _command_usage(name)
            return _command_usage(name)
        task_id = command.args[0]
        # Authorization is performed before invoking a control operation when
        # the facade exposes a public task lookup.  Missing/foreign tasks use
        # the same response to avoid leaking existence across users.
        record = None
        getter = getattr(self.manager, "get_task", None)
        if getter is not None:
            try:
                record = await _maybe_await(getter(task_id))
            except Exception:
                record = None
            if record is None or not _task_belongs_to(record, envelope):
                return f"cannot {name} task {task_id}"
            state = _task_state(record)
            if name == "retry" and state and state not in RETRYABLE_TASK_STATES:
                return f"cannot retry task {task_id}"
            if name == "cancel" and state in TERMINAL_TASK_STATES:
                return f"cannot cancel task {task_id}"
        action_names = {
            "retry": ("retry", "retry_task", "requeue_task"),
            "cancel": ("cancel", "cancel_task", "request_cancel"),
        }
        action_keyword = dict(scope)
        if name == "retry":
            retry_acknowledgement = f"retry queued: {task_id}"
            initial_reply = _slotful_command_initial_reply(
                envelope,
                retry_acknowledgement,
                agent_id=active_agent,
            )
            if initial_reply is not None:
                action_keyword["initial_reply"] = initial_reply
        try:
            result = await _invoke_compatible(
                self.manager,
                action_names[name],
                positional=(task_id,),
                keyword=action_keyword,
            )
        except AttributeError:
            return f"{name} is unavailable"
        except QueueFullError:
            return f"cannot {name} task {task_id}: queue is full"
        except (KeyError, PermissionError, ValueError, RuntimeError) as exc:
            # A durable command acknowledgement should not make the monitor
            # retain its cursor indefinitely when the store rejects a stale
            # or invalid task transition.  Keep the response deliberately
            # non-disclosing for ownership/security errors.
            command_logger.debug("%s task %s rejected", name, task_id, exc_info=True)
            return f"cannot {name} task {task_id}"
        if isinstance(result, str):
            return result
        state_value = _value(result, "state", "status", default="")
        state = str(getattr(state_value, "value", state_value) or "").lower()
        if name == "retry" and state:
            changed = state == "queued"
        else:
            changed = bool(
                _value(result, "changed", "accepted", "requested", default=result)
            )
        verb = {
            "retry": "retry queued",
            "cancel": "cancel requested",
        }[name]
        if changed:
            acknowledgement = f"{verb}: {task_id}"
            if name == "cancel" and record is not None:
                task_agent_id = str(
                    _value(record, "agent_id", default="") or ""
                ).strip()
                task_state = _task_state(record)
                if (
                    task_agent_id
                    and task_state
                    in ACTIVE_TASK_STATES | {"dispatching"}
                ):
                    try:
                        mailbox_cancelled = bool(
                            await _invoke_compatible(
                                self.manager,
                                (
                                    "cancel_active_agent_mailbox",
                                    "cancel_agent_mailbox",
                                    "request_mailbox_cancel",
                                ),
                                positional=(task_agent_id,),
                            )
                        )
                    except AttributeError:
                        mailbox_cancelled = False
                    except Exception:
                        mailbox_cancelled = False
                        command_logger.warning(
                            "mailbox cancellation failed after task %s "
                            "was cancelled",
                            task_id,
                            exc_info=True,
                        )
                    if mailbox_cancelled:
                        acknowledgement += "; agent mailbox turn cancelled"
            return acknowledgement
        return f"cannot {name} task {task_id}"

__all__ = [
    "CommandResponse",
    "DEFAULT_AGENT_ID",
    "DEFAULT_SESSION_ID",
    "MVPCommandRouter",
    "COMMAND_HELP",
    "COMMAND_POLICIES",
    "COMMAND_REGISTRY",
    "DEFAULT_COMMAND_POLICY",
    "DEFERRED_REPLY_QUOTA_CAPABILITY",
    "LARK_BOT_ONBOARDING_CAPABILITY",
    "LARK_COMMAND_REGISTRY_EXTENSION",
    "LARK_COMMAND_POLICY",
    "MVP_COMMANDS",
    "MVP_COMMAND_NAMES",
    "WECHAT_COMMAND_POLICY",
    "ChannelCommandPolicy",
    "CommandRegistryEntry",
    "CommandRegistryGroup",
    "command_client_id",
    "command_delivery_id",
    "command_help_for_channel",
    "command_initial_reply",
    "command_request_id",
    "command_task_id",
    "command_names_for_channel",
    "command_policy_for",
    "command_supported",
    "filter_command_registry",
    "run_shell_command",
    "unsupported_command_response",
]
