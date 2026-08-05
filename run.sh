#!/usr/bin/env bash
#
# Run the Codex <-> WeChat bridge bot.
#
# Installs uv if it isn't already on PATH, syncs the uv-managed environment
# (creating .venv on first run), and starts examples/codex_wechat_bot.py,
# which logs into WeChat (reusing saved credentials if present) and bridges
# incoming messages to a codex subprocess.
#
# Usage:
#   ./run.sh
#
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

if ! command -v uv >/dev/null 2>&1; then
    echo "==> 'uv' not found, installing it (https://docs.astral.sh/uv/)"
    curl -LsSf https://astral.sh/uv/install.sh | sh

    # The installer places uv in ~/.local/bin (or ~/.cargo/bin on some
    # setups); make sure it's on PATH for the rest of this script.
    export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"

    if ! command -v uv >/dev/null 2>&1; then
        echo "error: uv installation failed or is not on PATH. Install it manually from https://docs.astral.sh/uv/ and re-run this script." >&2
        exit 1
    fi
fi

uv sync --extra test
exec uv run examples/codex_wechat_bot.py
