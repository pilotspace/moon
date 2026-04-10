#!/bin/sh
set -e

# Stop and disable moon service before removal
if command -v systemctl >/dev/null 2>&1; then
    systemctl stop moon 2>/dev/null || true
    systemctl disable moon 2>/dev/null || true
fi
