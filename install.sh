#!/usr/bin/env bash
# install.sh — Install kahu-vessel on a Pi
# Usage: curl -fsSL https://raw.githubusercontent.com/KAHU-radar/kahu-vessel/main/install.sh | KAHU_API_KEY=<your-key> bash
set -euo pipefail

git clone https://github.com/KAHU-radar/kahu-vessel.git ~/kahu-vessel
cd ~/kahu-vessel
python3 -m venv .venv
.venv/bin/pip install -e .

# Write API key into config if provided
if [ -n "${KAHU_API_KEY:-}" ]; then
    sed -i "s/^api_key = .*/api_key = \"${KAHU_API_KEY}\"/" ~/kahu-vessel/config.toml
fi

echo ""
echo "==> Installed!"
echo "    Config: ~/kahu-vessel/config.toml"
echo "    Set radar_host to the IP of the machine running the relay."
echo "    Run: ~/kahu-vessel/.venv/bin/kahu-daemon"
