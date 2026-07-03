#!/bin/sh
# NOTE: do NOT use `set -e` here — we want config errors to be reported
# but not prevent the daemon from starting (some options vary by Kubo version).

IPFS_PATH="${IPFS_PATH:-/data/ipfs}"

# Initialize if not yet initialized.
if [ ! -d "$IPFS_PATH/blocks" ]; then
    echo "[ipfs-init] initializing repo at $IPFS_PATH ..."
    ipfs init --empty-repo --profile=server >/dev/null 2>&1 || true

    echo "[ipfs-init] applying optimized config ..."

    # ── API & Gateway bind addresses ──
    ipfs config Addresses.API       "/ip4/0.0.0.0/tcp/5001"
    ipfs config Addresses.Gateway   "/ip4/0.0.0.0/tcp/8080"
    ipfs config Addresses.Swarm     --json '["/ip4/0.0.0.0/tcp/4001","/ip6/::/tcp/4001"]'

    # ── Remove all public bootstrap nodes (offline / private network) ──
    ipfs config Bootstrap --json '[]'

    # ── Gateway: CORS + caching headers ──
    ipfs config Gateway.NoDNSLink --json true
    ipfs config Gateway.HTTPHeaders.Access-Control-Allow-Origin  --json '["*"]'
    ipfs config Gateway.HTTPHeaders.Access-Control-Allow-Methods --json '["GET","HEAD","OPTIONS"]'
    ipfs config Gateway.HTTPHeaders.Access-Control-Allow-Headers --json '["Range","Content-Type"]'
    ipfs config Gateway.HTTPHeaders.Cache-Control                 "public, max-age=29030400, immutable"

    # ── API CORS (for local dev tools) ──
    ipfs config API.HTTPHeaders.Access-Control-Allow-Origin  --json '["*"]'
    ipfs config API.HTTPHeaders.Access-Control-Allow-Methods --json '["GET","POST","OPTIONS"]'
    ipfs config API.HTTPHeaders.Access-Control-Allow-Headers --json '["Authorization","Content-Type"]'

    # ── Datastore: disable Bloom Filter to reduce write amplification ──
    ipfs config Datastore.BloomFilterSize 0

    # ── Disable telemetry ──
    ipfs config --json Plugins.Plugins.telemetry.Config.Mode '"off"' 2>/dev/null || true

    echo "[ipfs-init] config applied successfully."
fi

# Disable anonymous telemetry via env
export IPFS_TELEMETRY=off

# Hand off to the official Kubo daemon.
# --enable-gc=false: gateway manages pinning, GC disabled to prevent
#   deletion of CIDs referenced by non-latest object versions.
exec ipfs daemon --migrate=true --enable-gc=false
