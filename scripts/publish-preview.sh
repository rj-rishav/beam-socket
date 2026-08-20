#!/usr/bin/env bash
# Stage + pack the linux-x64 preview of beamsocket. Produces a tarball you then
# `npm publish ... --tag alpha`. Run from the repo root on a linux-x64 machine
# AFTER `npm run build:native -w beamsocket` has produced native/beamsocket.node.
#
# This never touches your npm token. Publishing is a separate, explicit step
# you run yourself (see PUBLISH.md).
set -euo pipefail

PKG="packages/beamsocket"
BIN="$PKG/native/beamsocket.node"
STAGE="$(mktemp -d)"

[ -f "$BIN" ] || { echo "ERROR: $BIN not found. Run: npm run build:native -w beamsocket"; exit 1; }

# Ensure dist is fresh.
npm run build -w beamsocket >/dev/null

cp -r "$PKG/dist" "$PKG/native" "$STAGE/"

cat > "$STAGE/package.json" <<'JSON'
{
  "name": "beamsocket",
  "version": "0.2.0",
  "description": "The high-performance networking runtime for Node.js — Rust engine, JS API. Preview build: linux-x64 (glibc) only.",
  "license": "MIT",
  "type": "module",
  "main": "./dist/index.js",
  "types": "./dist/index.d.ts",
  "files": ["dist", "native"],
  "os": ["linux"],
  "cpu": ["x64"],
  "engines": { "node": ">=18" },
  "keywords": ["websocket", "rust", "napi", "realtime", "networking", "runtime"]
}
JSON

( cd "$STAGE" && npm pack )
mv "$STAGE"/beamsocket-*.tgz .
echo
echo "Packed: $(ls beamsocket-*.tgz)"
echo "Publish with:  npm publish beamsocket-0.2.0.tgz --tag alpha --access public"
echo "(you must be 'npm login'-ed, or have NPM_TOKEN set in your env)"
