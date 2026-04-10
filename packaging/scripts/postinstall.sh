#!/bin/sh
set -e

# Create moon system user if it doesn't exist
if ! id -u moon >/dev/null 2>&1; then
    useradd --system --no-create-home --shell /usr/sbin/nologin moon
fi

# Set ownership on data/log dirs
chown -R moon:moon /var/lib/moon /var/log/moon 2>/dev/null || true

# Reload systemd if available
if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reload
fi
