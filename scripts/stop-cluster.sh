#!/usr/bin/env bash
cd "$(dirname "$0")/.."
[ -f logs/pids.txt ] && xargs kill < logs/pids.txt 2>/dev/null; rm -f logs/pids.txt
