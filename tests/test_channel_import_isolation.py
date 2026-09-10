"""Peer channel packages must not initialize one another's wire adapters."""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys
import textwrap


def test_lark_and_shared_commands_import_without_wechat_wire() -> None:
    root = Path(__file__).resolve().parents[1]
    script = textwrap.dedent(
        """
        import builtins
        import sys

        original_import = builtins.__import__

        def guarded_import(name, *args, **kwargs):
            if name == "wechat_ilink" or name.startswith("wechat_ilink."):
                raise AssertionError(f"unexpected WeChat wire import: {name}")
            return original_import(name, *args, **kwargs)

        builtins.__import__ = guarded_import

        import src.channels.commands as commands
        assert "src.channels.wechat" not in sys.modules
        assert not any(name == "wechat_ilink" or name.startswith("wechat_ilink.") for name in sys.modules)

        import src.channels.lark as lark
        assert "src.channels.wechat" not in sys.modules
        assert not any(name == "wechat_ilink" or name.startswith("wechat_ilink.") for name in sys.modules)

        import src.channels as channels
        assert channels.MVPCommandRouter is commands.MVPCommandRouter
        assert channels.LarkGateway is lark.LarkGateway
        assert "src.channels.wechat" not in sys.modules
        assert not any(name == "wechat_ilink" or name.startswith("wechat_ilink.") for name in sys.modules)
        """
    )
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_lazy_package_exports_preserve_adapter_apis() -> None:
    import src.channels as channels
    from src.channels import lark, wechat

    assert channels.LarkAccountState is lark.AccountState
    assert channels.LarkGateway is lark.LarkGateway
    assert channels.WeChatGateway is wechat.WeChatGateway
    assert channels.WechatGateway is wechat.WechatGateway
    assert channels.MVPCommandRouter.__module__ == "src.channels.commands"
    for name in channels.__all__:
        assert getattr(channels, name) is not None
