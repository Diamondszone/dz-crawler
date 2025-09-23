#!/usr/bin/env bash
set -euo pipefail

# ===== Pastikan git/rsync tersedia (fallback runtime installer) =====
if ! command -v git >/dev/null 2>&1; then
  echo "[setup] installing git/rsync..."
  export DEBIAN_FRONTEND=noninteractive
  if command -v apt-get >/dev/null 2>&1; then
    apt-get update -y && apt-get install -y git rsync ca-certificates tzdata
  elif command -v apk >/dev/null 2>&1; then
    apk add --no-cache git rsync ca-certificates tzdata
  elif command -v microdnf >/dev/null 2>&1; then
    microdnf install -y git rsync ca-certificates tzdata || true
  else
    echo "[setup] no supported package manager found; exiting"; exit 1
  fi
fi

# ===== Identitas git =====
git config user.name  "${GIT_USER_NAME:-Railway Bot}"
git config user.email "${GIT_USER_EMAIL:-railway@example.com}"
git config credential.helper store

# Token untuk HTTPS push (dz-crawler & 8k)
if [ -n "${GH_TOKEN:-}" ]; then
  echo "https://${GH_TOKEN}:x-oauth-basic@github.com" > ~/.git-credentials
  chmod 600 ~/.git-credentials || true
fi

# ===== Lokasi kerja kode (Nixpacks → /app) =====
export REPO_DIR="${REPO_DIR:-/app}"
# JANGAN git pull di sini karena /app bukan repo git
mkdir -p "$REPO_DIR/results" "$REPO_DIR/state" "$REPO_DIR/warc_paths_cache"

# ===== Repo OUTPUT 8k (hanya folder WARC + WP/WIX .txt) =====
: "${OUTPUT_REMOTE_URL:=https://github.com/crawler-8k/8k.git}"
: "${OUTPUT_GH_TOKEN:?OUTPUT_GH_TOKEN required}"
: "${OUTPUT_BRANCH:=main}"

OUT_DIR="$REPO_DIR/_out"
rm -rf "$OUT_DIR/.git" 2>/dev/null || true
mkdir -p "$OUT_DIR"
cd "$OUT_DIR"
git init -b "$OUTPUT_BRANCH"
git remote add origin "https://${OUTPUT_GH_TOKEN}:x-oauth-basic@github.com/${OUTPUT_REMOTE_URL#https://github.com/}"

: "${COMMIT_INTERVAL:=600}"

while true; do
  echo "[run] batch…"
  # ganti ke nama file pythonmu bila perlu
  python -u "$REPO_DIR/tools/8000.py" \
    ${SCAN_FROM_YEAR:+--from-year $SCAN_FROM_YEAR} \
    ${SCAN_TO_YEAR:+--to-year $SCAN_TO_YEAR} \
    ${START_CRAWL_ID:+--start-crawl-id $START_CRAWL_ID} \
    --max-crawls ${MAX_CRAWLS:-1} \
    --max-warcs-per-crawl ${MAX_WARCS:-100}

  echo "[stage] output bersih ke repo 8k"
  find "$OUT_DIR" -mindepth 1 -maxdepth 1 ! -name ".git" -exec rm -rf {} +
  rsync -a --prune-empty-dirs \
    --include="*/" --include="WP-site*.txt" --include="WIX-site*.txt" \
    --exclude="*" "$REPO_DIR/results/" "$OUT_DIR/"

  git add -A
  git commit -m "auto: clean results (WP/WIX) $(date -u +%FT%TZ)" || true
  git push -u origin "$OUTPUT_BRANCH" || true

  # ===== Sync state/ ke repo asal dz-crawler =====
  # (karena /app bukan repo git, kita clone dulu ke _origin)
  if [ -n "${GH_TOKEN:-}" ]; then
    ORIGIN_REMOTE_URL="${ORIGIN_REMOTE_URL:-https://github.com/Diamondszone/dz-crawler.git}"
    ORIGIN_DIR="$REPO_DIR/_origin"
    rm -rf "$ORIGIN_DIR"
    git clone "https://${GH_TOKEN}:x-oauth-basic@github.com/${ORIGIN_REMOTE_URL#https://github.com/}" "$ORIGIN_DIR"
    mkdir -p "$ORIGIN_DIR/state"
    rsync -a --delete "$REPO_DIR/state/" "$ORIGIN_DIR/state/" || true
    (
      cd "$ORIGIN_DIR"
      git add state/ || true
      git commit -m "sync: state $(date -u +%FT%TZ)" || true
      git push origin HEAD || true
    )
  else
    echo "[warn] GH_TOKEN kosong → skip sync state ke dz-crawler"
  fi

  sleep "$COMMIT_INTERVAL"
done
