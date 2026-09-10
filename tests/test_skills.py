"""Focused regressions for trusted skill discovery and channel ingress."""

from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import dataclass
from dataclasses import replace
from pathlib import Path

import pytest

from src.channels.models import InboundEnvelope, parse_command
from src.channels.wechat import MVPCommandRouter, WeChatGateway
from src.runtime.manager import TaskManager
from src.runtime.registry import AgentRegistry, codex_profile
from src.runtime.skills import (
    SkillBundleError,
    SkillDefinition,
    SkillSyntaxError,
    find_skill,
    format_skills_markdown,
    hash_skill_bundle,
    normalize_skill,
    normalize_skills,
    parse_skill_invocation,
)
from src.runtime.sqlite_store import SQLiteStore, StoreError
from wechat_ilink.types import (
    ITEM_TYPE_TEXT,
    MESSAGE_STATE_FINISH,
    MESSAGE_TYPE_USER,
    MessageItem,
    TextItem,
    WeixinMessage,
)


def _envelope(text: str) -> InboundEnvelope:
    return InboundEnvelope(
        channel="wechat",
        bot_id="bot",
        external_user_id="user",
        external_message_id="message-1",
        text=text,
        agent_id="codex",
        conversation_id="wechat:bot:user:default:codex",
    )


def _message(text: str, message_id: int) -> WeixinMessage:
    return WeixinMessage(
        seq=message_id,
        message_id=message_id,
        from_user_id="user",
        to_user_id="bot",
        message_type=MESSAGE_TYPE_USER,
        message_state=MESSAGE_STATE_FINISH,
        item_list=[
            MessageItem(type=ITEM_TYPE_TEXT, text_item=TextItem(text=text))
        ],
    )


def _catalog() -> list[dict[str, object]]:
    return [
        {
            "name": "zeta",
            "path": "/trusted/skills/zeta",
            "description": "Review a zeta document.",
            "version": "2",
        },
        {
            "name": "alpha",
            "path": "/private/skills/alpha",
            "interface": {"displayName": "Alpha", "shortDescription": "Plan alpha work."},
            "version": "1",
        },
        {
            "name": "disabled",
            "path": "/private/skills/disabled",
            "description": "Must not be listed.",
            "enabled": False,
        },
    ]


def test_skill_parser_requires_a_leading_name_and_description():
    invocation = parse_skill_invocation("  $Code-Review inspect the diff\ncarefully  ")
    assert invocation is not None
    assert invocation.name == "Code-Review"
    assert invocation.skill_id == "code-review"
    assert invocation.description == "inspect the diff\ncarefully"
    multiline = parse_skill_invocation("$Code-Review\ninspect the diff")
    assert multiline is not None
    assert multiline.name == "Code-Review"
    assert multiline.description == "inspect the diff"
    assert parse_skill_invocation("ordinary task") is None

    for malformed in ("$", "$demo", "$1demo task", "$demo! task", "$demo\t"):
        with pytest.raises(SkillSyntaxError):
            parse_skill_invocation(malformed)


def test_skill_lookup_and_markdown_are_case_insensitive_deterministic_and_private_safe():
    values = _catalog()
    definition = find_skill(values, "ALPHA")
    assert definition is not None
    assert definition.skill_id == "alpha"
    assert definition.path == "/private/skills/alpha"

    rendered = format_skills_markdown(values)
    assert rendered == format_skills_markdown(list(reversed(values)))
    assert rendered.startswith("## Skills\n")
    assert "`$alpha`" in rendered
    assert "`$zeta`" in rendered
    assert "disabled" not in rendered
    assert "/private/skills" not in rendered
    assert rendered.index("`$alpha`") < rendered.index("`$zeta`")

    # Typed descriptors and a nested SDK response are both accepted by the
    # normalization boundary, while path-like text remains redacted from the
    # public projection.
    typed = SkillDefinition(
        name="typed",
        path="/secret/typed",
        description="Read /secret/typed only",
    )
    typed_rendered = format_skills_markdown({"data": [typed]})
    assert "`$typed`" in typed_rendered
    assert "/secret/typed" not in typed_rendered

    canonical = format_skills_markdown(
        [{"name": "Mixed-Case", "path": "/trusted/mixed"}]
    )
    assert "`$mixed-case`" in canonical
    assert "`$Mixed-Case`" not in canonical


def test_skill_content_hash_tracks_skill_bytes(tmp_path):
    skill_dir = tmp_path / "skill"
    skill_dir.mkdir()
    skill_file = skill_dir / "SKILL.md"
    skill_file.write_text("version one", encoding="utf-8")
    first = normalize_skill({"name": "bytes", "path": str(skill_dir)})
    skill_file.write_text("version two", encoding="utf-8")
    second = normalize_skill({"name": "bytes", "path": str(skill_dir)})
    assert first.content_hash != second.content_hash
    assert first.version == f"sha256:{first.content_hash}"
    assert second.version == f"sha256:{second.content_hash}"


def test_skill_bundle_hash_tracks_nested_mutation_and_file_addition(tmp_path):
    skill_dir = tmp_path / "skill"
    references = skill_dir / "references"
    references.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text("instructions", encoding="utf-8")
    support_file = references / "rules.txt"
    support_file.write_text("version one", encoding="utf-8")

    baseline = hash_skill_bundle(skill_dir)
    assert hash_skill_bundle(skill_dir) == baseline
    assert normalize_skill({"name": "bundle", "path": str(skill_dir)}).content_hash == baseline
    assert normalize_skill(
        {
            "name": "bundle",
            "path": str(skill_dir),
            "content_hash": "0" * 64,
        },
        rehash_local_bundle=True,
    ).content_hash == baseline

    support_file.write_text("version two", encoding="utf-8")
    mutated = hash_skill_bundle(skill_dir)
    assert mutated != baseline

    support_file.write_text("version one", encoding="utf-8")
    (references / "added.txt").write_text("new instructions", encoding="utf-8")
    assert hash_skill_bundle(skill_dir) not in {baseline, mutated}

    same_bundle = tmp_path / "same-bundle"
    same_references = same_bundle / "references"
    same_references.mkdir(parents=True)
    # Create files in the opposite order to prove directory enumeration order
    # does not influence the bundle identity.
    (same_references / "rules.txt").write_text("version one", encoding="utf-8")
    (same_bundle / "SKILL.md").write_text("instructions", encoding="utf-8")
    assert hash_skill_bundle(same_bundle) == baseline


def test_skill_bundle_hash_rejects_symlinks_in_or_at_bundle(tmp_path):
    skill_dir = tmp_path / "skill"
    skill_dir.mkdir()
    (skill_dir / "SKILL.md").write_text("instructions", encoding="utf-8")
    target = tmp_path / "outside.txt"
    target.write_text("outside instructions", encoding="utf-8")
    linked_file = skill_dir / "linked.txt"
    linked_file.symlink_to(target)

    with pytest.raises(SkillBundleError, match="symlink"):
        hash_skill_bundle(skill_dir)
    with pytest.raises(SkillBundleError, match="symlink"):
        normalize_skill({"name": "unsafe", "path": str(skill_dir)})

    linked_file.unlink()
    linked_bundle = tmp_path / "linked-skill"
    linked_bundle.symlink_to(skill_dir, target_is_directory=True)
    with pytest.raises(SkillBundleError, match="symlink"):
        hash_skill_bundle(linked_bundle)


def test_normalize_skills_recursively_flattens_typed_envelopes_in_sequences():
    @dataclass
    class Entry:
        skills: object

    @dataclass
    class Response:
        data: object

    typed = SkillDefinition(
        name="Typed",
        path="/trusted/typed",
        version="1",
        content_hash="f" * 64,
    )
    mapped = {
        "name": "mapped",
        "path": "/trusted/mapped",
        "version": "2",
        "content_hash": "e" * 64,
    }
    catalog = [Response(data=(Entry(skills=[typed]),)), {"data": [{"skills": [mapped]}]}]

    assert normalize_skills(catalog) == (
        SkillDefinition.from_value(mapped),
        typed,
    )


def test_skill_store_fresh_schema_and_migration(tmp_path):
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        store = SQLiteStore(path)
        await store.initialize()
        await store.close()

        with sqlite3.connect(path) as connection:
            version = connection.execute(
                "SELECT MAX(version) FROM schema_migrations"
            ).fetchone()[0]
            columns = {
                str(row[1]) for row in connection.execute("PRAGMA table_info(skills)")
            }
        assert version == 41
        assert columns == {
            "agent_id",
            "skill_id",
            "version",
            "name",
            "path",
            "display_name",
            "description",
            "content_hash",
            "enabled",
            "created_at",
        }

    asyncio.run(scenario())


def test_skill_store_persists_scoped_immutable_versions(tmp_path):
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        first = SkillDefinition(
            name="Review",
            path="/trusted/review-v2",
            description="Review version two.",
            version="2",
            content_hash="2" * 64,
        )
        newest = replace(
            first,
            path="/trusted/review-v10",
            description="Review version ten.",
            version="10",
            content_hash="a" * 64,
        )
        disabled = SkillDefinition(
            name="Deploy",
            path="/trusted/deploy",
            version="1",
            content_hash="d" * 64,
            enabled=False,
        )

        store = SQLiteStore(path)
        await store.initialize()
        assert await store.put_skill(newest)
        assert await store.put_skill(first)
        assert await store.put_skill(disabled)
        assert await store.put_skill(first, agent_id="reviewer")
        await store.close()

        reopened = SQLiteStore(path)
        await reopened.initialize()
        try:
            exact = await reopened.get_skill("codex", "REVIEW", "2")
            latest = await reopened.get_skill("codex", "review")
            scoped = await reopened.get_skill("reviewer", "review", "2")
            assert isinstance(exact, SkillDefinition)
            assert exact == first
            assert latest == newest
            assert scoped == first
            assert await reopened.get_skill("other", "review") is None
            assert await reopened.list_skills(agent_id="codex") == [
                disabled,
                first,
                newest,
            ]
            assert await reopened.list_skills(agent_id="codex", enabled=True) == [
                first,
                newest,
            ]
        finally:
            await reopened.close()

    asyncio.run(scenario())


def test_skill_store_registration_is_idempotent_and_versions_are_immutable(tmp_path):
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        skill = SkillDefinition(
            name="Review",
            path="/trusted/review",
            description="Review changes.",
            display_name="Code Review",
            version="1",
            content_hash="1" * 64,
        )
        store = SQLiteStore(path)
        await store.initialize()
        try:
            assert await store.register_skill(skill)
            assert await store.register_skill(skill)
            for changed in (
                replace(skill, content_hash="2" * 64),
                replace(skill, path="/trusted/other"),
                replace(skill, description="Changed metadata."),
                replace(skill, enabled=False),
            ):
                with pytest.raises(StoreError, match="skill version metadata conflicts"):
                    await store.put_skill(changed)
            with pytest.raises(ValueError, match="version"):
                await store.put_skill(replace(skill, version=""))
        finally:
            await store.close()

        with sqlite3.connect(path) as connection:
            count = connection.execute(
                "SELECT COUNT(*) FROM skills WHERE agent_id='codex' "
                "AND skill_id='review' AND version='1'"
            ).fetchone()[0]
        assert count == 1

    asyncio.run(scenario())


def test_router_lists_skills_and_aliases_as_markdown():
    class Manager:
        async def get_active_agent(self, **_kwargs) -> str:
            return "codex"

        async def list_skills(self, *, agent_id: str, refresh: bool) -> list[dict[str, object]]:
            assert agent_id == "codex"
            assert refresh is False
            return _catalog()

    async def scenario() -> None:
        router = MVPCommandRouter(Manager())
        expected = await router.handle_command(parse_command("/skills"), _envelope("/skills"))
        assert expected is not None and expected.startswith("## Skills\n")
        assert "`$alpha`" in expected
        assert await router.handle_command(
            parse_command("/listskill"), _envelope("/listskill")
        ) == expected
        assert await router.handle_command(
            parse_command("/listskills extra"), _envelope("/listskills extra")
        ) == "usage: /listskills"

    asyncio.run(scenario())


def test_manager_persists_live_skill_versions_for_its_agent(tmp_path):
    async def scenario() -> None:
        path = tmp_path / "runtime.sqlite"
        store = SQLiteStore(path)
        registry = AgentRegistry()
        registry.register(
            "codex",
            _SkillRuntime(),
            profile=codex_profile(profile_version=1, default_mode_id="chat"),
        )
        manager = TaskManager(store, registry, worker_count=0)
        await manager.start()
        try:
            values = await manager.list_skills(agent_id="codex")
            assert len(values) == 3
            persisted = await store.get_skill("codex", "alpha", "1")
            assert persisted is not None
            assert persisted.path == "/private/skills/alpha"

            # Disabled discovery entries are not invocable and are not
            # published as enabled durable registry versions.
            assert await store.get_skill("codex", "disabled") is None
        finally:
            await manager.stop()

        reopened = SQLiteStore(path)
        await reopened.initialize()
        try:
            assert await reopened.get_skill("codex", "alpha", "1") is not None
        finally:
            await reopened.close()

    asyncio.run(scenario())


def test_manager_accepts_one_typed_skill_descriptor(tmp_path):
    class Runtime(_SkillRuntime):
        async def list_skills(self, *, refresh: bool = False):
            assert refresh is False
            return SkillDefinition(
                name="Typed",
                path="/trusted/typed",
                version="1",
                content_hash="f" * 64,
            )

    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        registry = AgentRegistry()
        registry.register("codex", Runtime(), profile=codex_profile())
        manager = TaskManager(store, registry, worker_count=0)
        await manager.start()
        try:
            values = await manager.list_skills()
            assert len(values) == 1
            assert isinstance(values[0], SkillDefinition)
            assert await store.get_skill("codex", "typed", "1") == values[0]
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_manager_flattens_nested_typed_skill_envelopes_before_persisting(tmp_path):
    @dataclass
    class Entry:
        skills: object

    @dataclass
    class Response:
        data: object

    skill = SkillDefinition(
        name="Nested",
        path="/trusted/nested",
        version="1",
        content_hash="a" * 64,
    )

    class Runtime(_SkillRuntime):
        async def list_skills(self, *, refresh: bool = False):
            assert refresh is False
            return [Response(data=[Entry(skills=(skill,))])]

    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        registry = AgentRegistry()
        registry.register("codex", Runtime(), profile=codex_profile())
        manager = TaskManager(store, registry, worker_count=0)
        await manager.start()
        try:
            values = await manager.list_skills()
            assert values == [skill]
            assert await store.get_skill("codex", "nested", "1") == skill
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_router_does_not_expose_skill_discovery_errors():
    class Manager:
        async def get_active_agent(self, **_kwargs) -> str:
            return "codex"

        async def list_skills(self, **_kwargs):
            raise OSError("cannot read /private/skills")

    async def scenario() -> None:
        router = MVPCommandRouter(Manager())
        result = await router.handle_command(
            parse_command("/skills"), _envelope("/skills")
        )
        assert result == "skills unavailable; try again later"
        assert "/private/skills" not in result

    asyncio.run(scenario())


class _SkillRuntime:
    agent_id = "codex"

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def run(self, _task, _emit):
        return None

    async def interrupt(self, _task_id: str) -> bool:
        return False

    async def list_skills(self, *, refresh: bool = False) -> list[dict[str, object]]:
        assert refresh is False
        return _catalog()


def test_gateway_snapshots_valid_skill_and_keeps_unknown_or_malformed_input_out_of_tasks(
    tmp_path,
):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        registry = AgentRegistry()
        registry.register(
            "codex",
            _SkillRuntime(),
            profile=codex_profile(profile_version=1, default_mode_id="chat"),
        )
        manager = TaskManager(
            store,
            registry,
            worker_count=0,
            default_agent_id="codex",
            default_mode_id="chat",
        )
        await manager.start()
        try:
            gateway = WeChatGateway(manager, bot_id="bot")
            valid = await gateway.accept(_message("$ALPHA plan this", 1))
            assert valid is not None and valid.accepted and valid.task_id
            task = await store.get_task(valid.task_id)
            assert task is not None
            assert task.inputs["text"] == "plan this"
            assert task.inputs["skill"]["skill_id"] == "alpha"
            assert task.inputs["skill"]["path"] == "/private/skills/alpha"
            assert task.metadata["skill_id"] == "alpha"
            assert valid.envelope.text == "$ALPHA plan this"

            unknown = await gateway.accept(_message("$missing do this", 2))
            assert unknown is not None and unknown.accepted
            assert unknown.task_id == ""
            assert unknown.command_response == (
                "unknown skill: $missing. use /skills to see available skills"
            )

            malformed = await gateway.accept(_message("$ALPHA", 3))
            assert malformed is not None and malformed.accepted
            assert malformed.task_id == ""
            assert malformed.command_response == "usage: $<skill> <task description>"

            tasks = await store.list_tasks(external_user_id="user", limit=20)
            assert [item.task_id for item in tasks] == [valid.task_id]
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_gateway_discovers_and_revalidates_skills_in_the_agents_cwd(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    selected = workspace / "selected"
    selected.mkdir(parents=True)

    class Runtime(_SkillRuntime):
        def __init__(self) -> None:
            self.observed_cwds: list[str | None] = []

        async def list_skills(
            self,
            *,
            refresh: bool = False,
            cwd: str | None = None,
        ) -> list[dict[str, object]]:
            assert refresh is False
            self.observed_cwds.append(cwd)
            return _catalog()

    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime-cwd.sqlite")
        runtime = Runtime()
        registry = AgentRegistry()
        registry.register("codex", runtime, profile=codex_profile())
        manager = TaskManager(
            store,
            registry,
            worker_count=0,
            workspace_root=workspace,
        )
        await manager.start()
        try:
            await manager.set_working_directory(
                "selected",
                channel="wechat",
                bot_id="bot",
                external_user_id="user",
                session_id="default",
                agent_id="codex",
            )
            accepted = await WeChatGateway(manager, bot_id="bot").accept(
                _message("$alpha inspect this", 41)
            )
            assert accepted is not None and accepted.task_id
            task = await store.get_task(accepted.task_id)
            assert task is not None
            assert task.metadata["execution_workspace"]["path"] == str(
                selected.resolve()
            )
            assert runtime.observed_cwds
            assert set(runtime.observed_cwds) == {str(selected.resolve())}
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_skill_task_replay_uses_persisted_snapshot_when_catalog_disappears(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        runtime = _SkillRuntime()
        catalog = _catalog()

        async def list_skills(*, refresh: bool = False):
            assert refresh is False
            return catalog

        runtime.list_skills = list_skills  # type: ignore[method-assign]
        registry = AgentRegistry()
        registry.register(
            "codex",
            runtime,
            profile=codex_profile(profile_version=1, default_mode_id="chat"),
        )
        manager = TaskManager(
            store,
            registry,
            worker_count=0,
            default_agent_id="codex",
            default_mode_id="chat",
        )
        await manager.start()
        try:
            gateway = WeChatGateway(manager, bot_id="bot")
            first = await gateway.accept(_message("$ALPHA plan this", 11))
            assert first is not None and first.task_id
            first_task = await store.get_task(first.task_id)
            assert first_task is not None

            # The live discovery result can disappear during a restart or
            # deployment, but the accepted task's immutable snapshot remains.
            catalog.clear()
            replay = await gateway.accept(_message("$ALPHA plan this", 11))
            assert replay is not None and replay.duplicate
            assert replay.task_id == first.task_id
            assert replay.command_response == ""
            replay_task = await store.get_task(replay.task_id)
            assert replay_task is not None
            assert replay_task.inputs["skill"] == first_task.inputs["skill"]
            assert len(await store.list_tasks(external_user_id="user", limit=20)) == 1
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_manager_rejects_skill_snapshot_path_or_hash_tampering(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        registry = AgentRegistry()
        registry.register(
            "codex",
            _SkillRuntime(),
            profile=codex_profile(profile_version=1, default_mode_id="chat"),
        )
        manager = TaskManager(
            store,
            registry,
            worker_count=0,
            default_agent_id="codex",
            default_mode_id="chat",
        )
        await manager.start()
        try:
            trusted = normalize_skill(_catalog()[1]).snapshot()
            path_tampered = {
                **trusted,
                "path": "/attacker/skills/alpha",
            }
            with pytest.raises(PermissionError, match="skill"):
                await manager.submit(
                    "inspect",
                    _envelope("ordinary").reply_target,
                    skill_snapshot=path_tampered,
                )

            hash_tampered = {
                **trusted,
                "content_hash": "0" * 64,
            }
            with pytest.raises(PermissionError, match="skill"):
                await manager.submit(
                    "inspect",
                    _envelope("ordinary").reply_target,
                    skill_snapshot=hash_tampered,
                )
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_manager_skill_replay_uses_first_snapshot_before_live_catalog_validation(tmp_path):
    async def scenario() -> None:
        store = SQLiteStore(tmp_path / "runtime.sqlite")
        runtime = _SkillRuntime()
        catalog = [_catalog()[1]]

        async def list_skills(*, refresh: bool = False):
            assert refresh is False
            return catalog

        runtime.list_skills = list_skills  # type: ignore[method-assign]
        registry = AgentRegistry()
        registry.register(
            "codex",
            runtime,
            profile=codex_profile(profile_version=1, default_mode_id="chat"),
        )
        manager = TaskManager(
            store,
            registry,
            worker_count=0,
            default_agent_id="codex",
            default_mode_id="chat",
        )
        await manager.start()
        try:
            inbound = _envelope("$ALPHA inspect this")
            old_snapshot = normalize_skill(catalog[0]).snapshot()
            first = await manager.accept_inbound(
                inbound,
                inputs={"text": "inspect this"},
                skill_snapshot=old_snapshot,
            )
            assert first.task is not None

            # The live registry now points alpha at another immutable version.
            catalog[:] = [
                {
                    **_catalog()[1],
                    "path": "/private/skills/alpha-v2",
                    "version": "2",
                }
            ]
            replay = await manager.accept_inbound(
                inbound,
                inputs={"text": "a forged replay prompt"},
                skill_snapshot=old_snapshot,
            )
            assert replay.duplicate
            assert replay.task is not None
            assert replay.task.task_id == first.task.task_id
            assert replay.task.inputs["text"] == "inspect this"
            assert replay.task.inputs["skill"] == first.task.inputs["skill"]
            assert len(await store.list_tasks(external_user_id="user", limit=20)) == 1

            # A matching external ID from another user cannot borrow the first
            # inbound/task and must still fail at the durable identity check.
            catalog[:] = [_catalog()[1]]
            foreign = replace(inbound, external_user_id="other")
            with pytest.raises(StoreError, match="identity|inbound"):
                await manager.accept_inbound(
                    foreign,
                    inputs={"text": "foreign"},
                    skill_snapshot=old_snapshot,
                )
            assert len(await store.list_tasks(limit=20)) == 1
        finally:
            await manager.stop()

    asyncio.run(scenario())
