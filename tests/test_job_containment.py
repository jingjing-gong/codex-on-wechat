"""Focused tests for the disconnected cgroup-v2 containment foundation."""

from __future__ import annotations

import json
import os
import shutil
import stat
import subprocess
from pathlib import Path
from typing import Sequence

import pytest

from src.runtime.job_containment import (
    CGROUP_CONTAINMENT_REQUIREMENTS_V1,
    CgroupContainmentUnavailableReason,
    CgroupJobIdentityError,
    CgroupJobIdentityMismatchError,
    CgroupJobIdentityV1,
    CgroupPathSecurityError,
    CgroupV2ContainmentUnavailable,
    JobContainmentError,
    LauncherProbeExecution,
    bind_existing_cgroup_job,
    new_cgroup_job_name,
    open_secure_cgroup_root,
    parse_cgroup_events,
    probe_cgroup_v2_containment,
    reopen_cgroup_job,
)


OWNER = ("agent-invocation", "bb", 2, 7, "invocation-123")
SAFE_FAKE_LAUNCHER = "/bin/true"


def _existing_fake_job(tmp_path: Path) -> tuple[Path, str, CgroupJobIdentityV1]:
    root_path = tmp_path / "delegated"
    root_path.mkdir(mode=0o700)
    job_id, component = new_cgroup_job_name()
    assert component == f"job-{job_id}"
    job_path = root_path / component
    job_path.mkdir(mode=0o700)
    (job_path / "cgroup.events").write_text(
        "populated 0\nfrozen 0\n",
        encoding="ascii",
    )
    with open_secure_cgroup_root(root_path) as root:
        identity = bind_existing_cgroup_job(
            root,
            relative_component=component,
            owner=OWNER,
        )
    return root_path, component, identity


def _fake_probe_root(tmp_path: Path) -> tuple[Path, str]:
    root_path = tmp_path / "delegated-probe"
    root_path.mkdir(mode=0o700)
    controls = {
        "cgroup.events": ("populated 0\nfrozen 0\n", 0o400),
        "cgroup.procs": ("", 0o600),
        "cgroup.kill": ("", 0o200),
    }
    for name, (content, mode) in controls.items():
        path = root_path / name
        path.write_text(content, encoding="ascii")
        path.chmod(mode)
    with open_secure_cgroup_root(root_path) as root:
        mount_id = root.mount_id
    # Kernel-owned mountinfo is injectable only so the secure probe can be
    # tested against ordinary temporary directories without creating cgroups.
    mountinfo = (
        f"{mount_id} 1 0:42 / {root_path} rw,nosuid,nodev,noexec "
        "- cgroup2 cgroup rw,nsdelegate\n"
    )
    return root_path, mountinfo


def _successful_runner(_command: Sequence[str]) -> LauncherProbeExecution:
    return LauncherProbeExecution(returncode=0)


def test_identity_round_trip_binds_every_owner_and_kernel_field(
    tmp_path: Path,
) -> None:
    root_path, _component, identity = _existing_fake_job(tmp_path)
    wire = identity.to_wire()
    parsed = CgroupJobIdentityV1.parse(wire)

    assert parsed == identity
    assert parsed.owner == OWNER
    assert parsed.relative_component == f"job-{parsed.job_id}"
    assert parsed.canonical_hash.startswith("sha256:")
    assert json.loads(wire)["owner"] == list(OWNER)

    with reopen_cgroup_job(root_path, wire) as reopened:
        assert reopened.identity == identity
        assert reopened.read_populated() is False


def test_identity_is_canonical_and_rejects_hash_and_owner_drift(
    tmp_path: Path,
) -> None:
    _root_path, _component, identity = _existing_fake_job(tmp_path)
    decoded = json.loads(identity.to_wire())
    decoded["owner"][-1] = "different-invocation"
    drifted = json.dumps(decoded, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    with pytest.raises(CgroupJobIdentityError, match="hash does not match"):
        CgroupJobIdentityV1.parse(drifted)

    with pytest.raises(CgroupJobIdentityError, match="not canonical"):
        CgroupJobIdentityV1.parse(identity.to_wire() + " ")

    duplicate = identity.to_wire().replace(
        '{"backend":"cgroup-v2",',
        '{"backend":"cgroup-v2","backend":"cgroup-v2",',
        1,
    )
    with pytest.raises(CgroupJobIdentityError, match="duplicate"):
        CgroupJobIdentityV1.parse(duplicate)


@pytest.mark.parametrize(
    "component",
    (
        "../job-00000000000000000000000000000000",
        "nested/job-00000000000000000000000000000000",
        "/job-00000000000000000000000000000000",
        "job-00000000000000000000000000000000/..",
        ".",
    ),
)
def test_identity_rejects_traversal_or_multiple_components(component: str) -> None:
    with pytest.raises(CgroupJobIdentityError, match="safe component"):
        CgroupJobIdentityV1.issue(
            boot_id="11111111-1111-4111-8111-111111111111",
            mount_id=1,
            root_device=1,
            root_inode=1,
            job_id="0" * 32,
            owner=OWNER,
            relative_component=component,
            job_device=1,
            job_inode=2,
        )


def test_secure_root_walk_rejects_traversal_and_symlink_ancestor(
    tmp_path: Path,
) -> None:
    real_parent = tmp_path / "real-parent"
    real_parent.mkdir(mode=0o700)
    (real_parent / "root").mkdir(mode=0o700)
    linked_parent = tmp_path / "linked-parent"
    linked_parent.symlink_to(real_parent, target_is_directory=True)

    with pytest.raises(CgroupPathSecurityError, match="symlink"):
        open_secure_cgroup_root(linked_parent / "root")
    with pytest.raises(CgroupPathSecurityError, match="traversal"):
        open_secure_cgroup_root(real_parent / ".." / "real-parent" / "root")
    with pytest.raises(CgroupPathSecurityError, match="absolute"):
        open_secure_cgroup_root(Path("relative/root"))


def test_reopen_rejects_job_symlink_replacement(tmp_path: Path) -> None:
    root_path, component, identity = _existing_fake_job(tmp_path)
    original = root_path / component
    shutil.rmtree(original)
    replacement = root_path / "replacement"
    replacement.mkdir(mode=0o700)
    original.symlink_to(replacement.name, target_is_directory=True)

    with pytest.raises(CgroupJobIdentityMismatchError, match="symlinked"):
        reopen_cgroup_job(root_path, identity)


def test_reopen_rejects_boot_and_mount_drift(tmp_path: Path) -> None:
    root_path, _component, identity = _existing_fake_job(tmp_path)
    other_boot = "11111111-1111-4111-8111-111111111111"
    if other_boot == identity.boot_id:  # pragma: no cover - real boot UUID is random
        other_boot = "22222222-2222-4222-8222-222222222222"
    with pytest.raises(CgroupJobIdentityMismatchError, match="another Linux boot"):
        reopen_cgroup_job(root_path, identity, current_boot_id=other_boot)

    mount_drift = CgroupJobIdentityV1.issue(
        boot_id=identity.boot_id,
        mount_id=identity.mount_id + 1,
        root_device=identity.root_device,
        root_inode=identity.root_inode,
        job_id=identity.job_id,
        owner=identity.owner,
        relative_component=identity.relative_component,
        job_device=identity.job_device,
        job_inode=identity.job_inode,
    )
    with pytest.raises(CgroupJobIdentityMismatchError, match="mount or inode"):
        reopen_cgroup_job(root_path, mount_drift)


def test_reopen_rejects_root_and_job_inode_drift(tmp_path: Path) -> None:
    root_path, component, identity = _existing_fake_job(tmp_path)
    old_root = tmp_path / "old-delegated"
    root_path.rename(old_root)
    root_path.mkdir(mode=0o700)
    (root_path / component).mkdir(mode=0o700)
    with pytest.raises(CgroupJobIdentityMismatchError, match="root mount or inode"):
        reopen_cgroup_job(root_path, identity)

    # Restoring the exact root isolates drift of the named job inode.
    shutil.rmtree(root_path)
    old_root.rename(root_path)
    displaced_job = root_path / "displaced-job"
    (root_path / component).rename(displaced_job)
    (root_path / component).mkdir(mode=0o700)
    with pytest.raises(CgroupJobIdentityMismatchError, match="job mount or inode"):
        reopen_cgroup_job(root_path, identity)


def test_reopened_validation_rechecks_root_and_job_permissions(
    tmp_path: Path,
) -> None:
    root_path, component, identity = _existing_fake_job(tmp_path)
    reopened = reopen_cgroup_job(root_path, identity)
    try:
        (root_path / component).chmod(0o770)
        with pytest.raises(CgroupJobIdentityMismatchError, match="job identity"):
            reopened.validate()
        (root_path / component).chmod(0o700)
        root_path.chmod(0o770)
        with pytest.raises(CgroupJobIdentityMismatchError, match="root identity"):
            reopened.validate()
    finally:
        reopened.close()


def test_reopened_enter_failure_closes_both_descriptors(tmp_path: Path) -> None:
    root_path, component, identity = _existing_fake_job(tmp_path)
    reopened = reopen_cgroup_job(root_path, identity)
    job_fd = reopened.job_descriptor
    root_fd = reopened.root.descriptor
    (root_path / component).rename(root_path / "moved-after-reopen")

    with pytest.raises(OSError):
        reopened.__enter__()

    assert reopened._job_descriptor is None
    assert reopened.root._descriptor is None
    with pytest.raises(OSError):
        os.fstat(job_fd)
    with pytest.raises(OSError):
        os.fstat(root_fd)


def test_reopened_close_still_closes_root_after_bad_job_fd(tmp_path: Path) -> None:
    root_path, _component, identity = _existing_fake_job(tmp_path)
    reopened = reopen_cgroup_job(root_path, identity)
    job_fd = reopened.job_descriptor
    root_fd = reopened.root.descriptor
    os.close(job_fd)

    with pytest.raises(OSError):
        reopened.close()

    assert reopened._job_descriptor is None
    assert reopened.root._descriptor is None
    with pytest.raises(OSError):
        os.fstat(root_fd)


@pytest.mark.parametrize(
    ("value", "expected"),
    (
        ("populated 0\n", False),
        ("populated 1\n", True),
        ("populated 0\nfrozen 1\n", False),
        ("frozen 0\npopulated 1\n", True),
    ),
)
def test_cgroup_events_requires_one_canonical_populated_bit(
    value: str,
    expected: bool,
) -> None:
    assert parse_cgroup_events(value) is expected


@pytest.mark.parametrize(
    "value",
    (
        "",
        "frozen 0\n",
        "populated 2\n",
        "populated -1\n",
        "populated 01\n",
        " populated 1\n",
        "populated  1\n",
        "populated\t1\n",
        "populated 1\r\n",
        "populated 1\n\n",
        "populated 1\npopulated 1\n",
        "frozen 0\nfrozen 1\npopulated 0\n",
    ),
)
def test_cgroup_events_rejects_malformed_and_duplicate_records(value: str) -> None:
    with pytest.raises(JobContainmentError):
        parse_cgroup_events(value)


def test_probe_zero_exit_remains_incomplete_and_preserves_network_metadata(
    tmp_path: Path,
) -> None:
    root_path, mountinfo = _fake_probe_root(tmp_path)
    commands: list[tuple[str, ...]] = []

    def record_success(command: Sequence[str]) -> LauncherProbeExecution:
        commands.append(tuple(command))
        return LauncherProbeExecution(returncode=0)

    result = probe_cgroup_v2_containment(
        root_path,
        launcher=SAFE_FAKE_LAUNCHER,
        launcher_runner=record_success,
        mountinfo_text=mountinfo,
    )

    assert isinstance(result, CgroupV2ContainmentUnavailable)
    assert result.reason is CgroupContainmentUnavailableReason.LAUNCHER_PROOF_INCOMPLETE
    assert len(commands) == 1
    command = commands[0]
    assert "--unshare-user" in command
    assert "--unshare-cgroup" in command
    assert "--unshare-net" not in command
    assert "--ro-bind" in command
    requirements = result.requirements
    assert requirements.network_namespace_policy == "preserve"
    assert requirements.network_access_required
    assert requirements.requires_pid_namespace
    assert requirements.requires_ipc_namespace
    assert requirements.requires_host_bus_isolation
    assert requirements.requires_all_cgroup_views_sealed
    assert not requirements.launcher_proof_complete
    assert not requirements.allocation_api_enabled
    assert not requirements.kill_api_enabled
    assert not requirements.production_dispatch_enabled
    assert requirements.foundation_state == "disconnected"
    assert requirements is CGROUP_CONTAINMENT_REQUIREMENTS_V1


def test_real_true_binary_cannot_forge_available_with_zero_exit(
    tmp_path: Path,
) -> None:
    root_path, mountinfo = _fake_probe_root(tmp_path)
    result = probe_cgroup_v2_containment(
        root_path,
        launcher=SAFE_FAKE_LAUNCHER,
        mountinfo_text=mountinfo,
    )

    assert isinstance(result, CgroupV2ContainmentUnavailable)
    assert result.reason is CgroupContainmentUnavailableReason.LAUNCHER_PROOF_INCOMPLETE
    assert result.available is False


def test_probe_revalidates_root_and_controls_after_runner(tmp_path: Path) -> None:
    root_path, mountinfo = _fake_probe_root(tmp_path)

    def weaken_root(_command: Sequence[str]) -> LauncherProbeExecution:
        root_path.chmod(0o770)
        return LauncherProbeExecution(returncode=0)

    result = probe_cgroup_v2_containment(
        root_path,
        launcher=SAFE_FAKE_LAUNCHER,
        launcher_runner=weaken_root,
        mountinfo_text=mountinfo,
    )
    assert isinstance(result, CgroupV2ContainmentUnavailable)
    assert result.reason is CgroupContainmentUnavailableReason.LAUNCHER_PROBE_FAILED

    root_path.chmod(0o700)

    def replace_control(_command: Sequence[str]) -> LauncherProbeExecution:
        (root_path / "cgroup.kill").unlink()
        return LauncherProbeExecution(returncode=0)

    result = probe_cgroup_v2_containment(
        root_path,
        launcher=SAFE_FAKE_LAUNCHER,
        launcher_runner=replace_control,
        mountinfo_text=mountinfo,
    )
    assert isinstance(result, CgroupV2ContainmentUnavailable)
    assert result.reason is CgroupContainmentUnavailableReason.LAUNCHER_PROBE_FAILED


def test_probe_rejects_user_owned_launcher_even_when_executable(
    tmp_path: Path,
) -> None:
    if os.geteuid() == 0:
        pytest.skip("root-owned test process cannot create a user-owned fixture")
    root_path, mountinfo = _fake_probe_root(tmp_path)
    launcher = tmp_path / "user-launcher"
    shutil.copyfile(SAFE_FAKE_LAUNCHER, launcher)
    launcher.chmod(0o700)
    called = False

    def must_not_run(_command: Sequence[str]) -> LauncherProbeExecution:
        nonlocal called
        called = True
        return LauncherProbeExecution(returncode=0)

    result = probe_cgroup_v2_containment(
        root_path,
        launcher=str(launcher),
        launcher_runner=must_not_run,
        mountinfo_text=mountinfo,
    )
    assert isinstance(result, CgroupV2ContainmentUnavailable)
    assert result.reason is CgroupContainmentUnavailableReason.LAUNCHER_MISSING
    assert not called


@pytest.mark.parametrize(
    ("stderr", "reason"),
    (
        (
            "bwrap: AppArmor denied unprivileged user namespace creation",
            CgroupContainmentUnavailableReason.LAUNCHER_APPARMOR_DENIED,
        ),
        (
            "bwrap: setting up uid map: Permission denied",
            CgroupContainmentUnavailableReason.LAUNCHER_USER_NAMESPACE_UNAVAILABLE,
        ),
    ),
)
def test_probe_returns_typed_unavailable_for_realistic_namespace_denials(
    tmp_path: Path,
    stderr: str,
    reason: CgroupContainmentUnavailableReason,
) -> None:
    root_path, mountinfo = _fake_probe_root(tmp_path)

    def denied(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(command, 1, stdout="", stderr=stderr)

    result = probe_cgroup_v2_containment(
        root_path,
        launcher=SAFE_FAKE_LAUNCHER,
        launcher_runner=denied,
        mountinfo_text=mountinfo,
    )

    assert isinstance(result, CgroupV2ContainmentUnavailable)
    assert result.available is False
    assert result.reason is reason
    assert result.requirements.network_namespace_policy == "preserve"


@pytest.mark.parametrize(
    ("returncode", "reason"),
    (
        (70, CgroupContainmentUnavailableReason.LAUNCHER_CGROUP_NAMESPACE_UNAVAILABLE),
        (71, CgroupContainmentUnavailableReason.LAUNCHER_NETWORK_NOT_PRESERVED),
        (72, CgroupContainmentUnavailableReason.LAUNCHER_CGROUP_NAMESPACE_UNAVAILABLE),
        (73, CgroupContainmentUnavailableReason.LAUNCHER_CGROUP_VIEW_WRITABLE),
    ),
)
def test_probe_fails_closed_on_namespace_assertion(
    tmp_path: Path,
    returncode: int,
    reason: CgroupContainmentUnavailableReason,
) -> None:
    root_path, mountinfo = _fake_probe_root(tmp_path)
    result = probe_cgroup_v2_containment(
        root_path,
        launcher=SAFE_FAKE_LAUNCHER,
        launcher_runner=lambda _command: LauncherProbeExecution(returncode),
        mountinfo_text=mountinfo,
    )
    assert isinstance(result, CgroupV2ContainmentUnavailable)
    assert result.reason is reason


def test_probe_checks_nsdelegate_and_all_control_files(tmp_path: Path) -> None:
    root_path, mountinfo = _fake_probe_root(tmp_path)
    no_nsdelegate = mountinfo.replace(",nsdelegate", "")
    result = probe_cgroup_v2_containment(
        root_path,
        launcher=SAFE_FAKE_LAUNCHER,
        launcher_runner=_successful_runner,
        mountinfo_text=no_nsdelegate,
    )
    assert isinstance(result, CgroupV2ContainmentUnavailable)
    assert result.reason is CgroupContainmentUnavailableReason.NSDELEGATE_MISSING

    (root_path / "cgroup.kill").unlink()
    result = probe_cgroup_v2_containment(
        root_path,
        launcher=SAFE_FAKE_LAUNCHER,
        launcher_runner=_successful_runner,
        mountinfo_text=mountinfo,
    )
    assert isinstance(result, CgroupV2ContainmentUnavailable)
    assert result.reason is CgroupContainmentUnavailableReason.CONTROL_FILE_UNAVAILABLE


def test_probe_rejects_symlink_control_and_malformed_events(tmp_path: Path) -> None:
    root_path, mountinfo = _fake_probe_root(tmp_path)
    events = root_path / "cgroup.events"
    events.unlink()
    target = root_path / "events-target"
    target.write_text("populated 0\n", encoding="ascii")
    target.chmod(0o400)
    events.symlink_to(target.name)
    result = probe_cgroup_v2_containment(
        root_path,
        launcher=SAFE_FAKE_LAUNCHER,
        launcher_runner=_successful_runner,
        mountinfo_text=mountinfo,
    )
    assert isinstance(result, CgroupV2ContainmentUnavailable)
    assert result.reason is CgroupContainmentUnavailableReason.CONTROL_FILE_UNAVAILABLE

    events.unlink()
    events.write_text("populated 0\npopulated 1\n", encoding="ascii")
    events.chmod(0o400)
    result = probe_cgroup_v2_containment(
        root_path,
        launcher=SAFE_FAKE_LAUNCHER,
        launcher_runner=_successful_runner,
        mountinfo_text=mountinfo,
    )
    assert isinstance(result, CgroupV2ContainmentUnavailable)
    assert result.reason is CgroupContainmentUnavailableReason.CONTROL_FILE_MALFORMED


def test_current_host_probe_is_typed_unavailable_without_mutation() -> None:
    result = probe_cgroup_v2_containment("/sys/fs/cgroup")
    assert isinstance(result, CgroupV2ContainmentUnavailable)
    assert result.available is False
    assert result.requirements.foundation_state == "disconnected"
    assert not result.requirements.production_dispatch_enabled


def test_root_and_control_permissions_are_fail_closed(tmp_path: Path) -> None:
    root = tmp_path / "unsafe"
    root.mkdir(mode=0o700)
    root.chmod(0o777)
    with pytest.raises(CgroupPathSecurityError, match="another principal"):
        open_secure_cgroup_root(root)

    # Confirm the fixture did not accidentally depend on umask semantics.
    assert stat.S_IMODE(root.stat().st_mode) == 0o777
