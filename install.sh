#!/usr/bin/env bash
# install.sh — Install kahu-vessel on a Pi
set -euo pipefail

git clone https://github.com/KAHU-radar/kahu-vessel.git ~/kahu-vessel
cd ~/kahu-vessel
python3 -m venv .venv
.venv/bin/pip install -e .

echo ""
echo "==> Installed! Next steps:"
echo "    1. Edit ~/kahu-vessel/config.toml — set relay_host and api_key"
echo "    2. Run: ~/kahu-vessel/.venv/bin/kahu-daemon"
