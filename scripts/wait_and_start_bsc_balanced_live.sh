#!/bin/zsh
set -euo pipefail
cd /Users/gavin/Projects/meme0801
export NODE_USE_ENV_PROXY=1
export HTTP_PROXY=http://127.0.0.1:7890
export HTTPS_PROXY=http://127.0.0.1:7890
export ALL_PROXY=socks5://127.0.0.1:7890
export NO_PROXY=localhost,127.0.0.1
export BAW_BINARY=/Users/gavin/.nvm/versions/node/v24.15.0/bin/baw

# One login session only: verify must use the exact qrCodeId returned by signin.
signin_json=$("$BAW_BINARY" auth signin --json)
qr_code_id=$(printf '%s' "$signin_json" | /usr/bin/python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["qrCodeId"])')
login_url=$(printf '%s' "$signin_json" | /usr/bin/python3 -c 'import json,sys; print(json.load(sys.stdin)["data"].get("urlForWeb", ""))')
pairing_code=$(printf '%s' "$signin_json" | /usr/bin/python3 -c 'import json,sys; print(json.load(sys.stdin)["data"].get("pairingCode", ""))')
print "BAW_LOGIN_URL=$login_url"
print "BAW_PAIRING_CODE=$pairing_code"
verify_json=$("$BAW_BINARY" auth verify --qrCodeId "$qr_code_id" --json)
verify_ok=$(printf '%s' "$verify_json" | /usr/bin/python3 -c 'import json,sys; print("true" if json.load(sys.stdin).get("success") is True else "false")')
if [[ "$verify_ok" != "true" ]]; then
  print "BAW_AUTH_VERIFY_FAILED=$verify_json"
  exit 2
fi
wallet_state=$("$BAW_BINARY" wallet status --json)
if [[ "$wallet_state" != *'"CONNECTED"'* ]]; then
  print "BAW_AUTH_STATUS_NOT_CONNECTED=$wallet_state"
  exit 2
fi
exec env PYTHONUNBUFFERED=1 PYTHONPATH=src PAPER_ONLY=true LIVE_TRADING=true BSC_LIVE_ENABLED=true \
  WALLET_ENABLED=false SIGNING_ENABLED=false BROADCAST_ENABLED=false \
  VENUE_HISTORY_BACKFILL_ENABLED=false NODE_USE_ENV_PROXY=1 \
  BAW_BINARY=/Users/gavin/.nvm/versions/node/v24.15.0/bin/baw \
  HTTP_PROXY=http://127.0.0.1:7890 HTTPS_PROXY=http://127.0.0.1:7890 \
  ALL_PROXY=socks5://127.0.0.1:7890 \
  python3 -u run_realtime.py --chain bsc --mode live --strategy-profile balanced --poll-sec 4 --env-file /Users/gavin/Projects/meme0801/.env
