#!/bin/sh
set -e

# Create moon system user if it doesn't exist
if ! id -u moon >/dev/null 2>&1; then
    useradd --system --no-create-home --shell /usr/sbin/nologin moon
fi

# Belt-and-suspenders: ensure required directories exist before chown
mkdir -p /etc/moon /var/lib/moon /var/log/moon

# Set ownership on data/log dirs
chown -R moon:moon /var/lib/moon /var/log/moon 2>/dev/null || true

# Reload systemd if available
if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reload
    # On upgrade, reload config without dropping connections
    if systemctl is-active moon >/dev/null 2>&1; then
        systemctl reload moon 2>/dev/null || true
    fi
fi
