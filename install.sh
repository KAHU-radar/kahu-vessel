#!/usr/bin/env bash
# install.sh — Install kahu-vessel on a Pi
# Usage: curl -fsSL https://raw.githubusercontent.com/KAHU-radar/kahu-vessel/main/install.sh | KAHU_API_KEY=<your-key> bash
set -euo pipefail

git clone https://github.com/KAHU-radar/kahu-vessel.git ~/kahu-vessel
cd ~/kahu-vessel
python3 -m venv .venv
.venv/bin/pip install -e .

mkdir -p ~/.kahu
cat > ~/.kahu/config.toml << TOML
[relay]
source = "udp"
udp_port = 10110

[sink]
port = 10110

[daemon]
relay_host = "localhost"  # set to your relay's IP address
use_system_time = true    # set false if using historical data

[upload]
host = "crowdsource.kahu.earth"
port = 9900
api_key = "${KAHU_API_KEY:-}"
points_per_track = 10
TOML

echo ""
echo "==> Installed!"
echo "    Config written to ~/.kahu/config.toml"
echo "    Set relay_host to the IP of the machine running the relay."
echo "    Run: ~/kahu-vessel/.venv/bin/kahu-daemon"
