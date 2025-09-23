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

# Token untuk origin (dz-crawler) – diperlukan agar bisa push state/ demi resume
if [ -n "${GH_TOKEN:-}" ]; then
  echo "https://${GH_TOKEN}:x-oauth-basic@github.com" > ~/.git-credentials
  chmod 600 ~/.git-credentials || true
fi

# ===== Lokasi kerja kode (Railway/Nixpacks checkout ke /workspace) =====
export REPO_DIR="${REPO_DIR:-/workspace}"
git -C "$REPO_DIR" pull --ff-only || true

# ===== Repo OUTPUT (akun lain) – HANYA folder WARC + WP/WIX .txt =====
: "${OUTPUT_REMOTE_URL:=https://github.com/crawler-8k/8k.git}"
: "${OUTPUT_GH_TOKEN:?OUTPUT_GH_TOKEN required}"
: "${OUTPUT_BRANCH:=main}"

OUT_DIR="$REPO_DIR/_out"
rm -rf "$OUT_DIR/.git" 2>/dev/null || true
mkdir -p "$OUT_DIR"
cd "$OUT_DIR"
git init -b "$OUTPUT_BRANCH"
git remote add origin "https://${OUTPUT_GH_TOKEN}:x-oauth-basic@github.com/${OUTPUT_REMOTE_URL#https://github.com/}"

# ===== Pastikan folder kerja hasil di repo kode =====
mkdir -p "$REPO_DIR/results" "$REPO_DIR/state" "$REPO_DIR/warc_paths_cache"

: "${COMMIT_INTERVAL:=600}"   # detik

while true; do
  echo "[run] batch…"
  python -u "$REPO_DIR/tools/8000.py" \
    ${SCAN_FROM_YEAR:+--from-year $SCAN_FROM_YEAR} \
    ${SCAN_TO_YEAR:+--to-year $SCAN_TO_YEAR} \
    ${START_CRAWL_ID:+--start-crawl-id $START_CRAWL_ID} \
    --max-crawls ${MAX_CRAWLS:-1} \
    --max-warcs-per-crawl ${MAX_WARCS:-100}

  echo "[stage] bangun output BERSIH untuk repo 8k (hanya folder WARC + WP/WIX .txt)"
  find "$OUT_DIR" -mindepth 1 -maxdepth 1 ! -name ".git" -exec rm -rf {} +
  rsync -a --prune-empty-dirs \
    --include="*/" \
    --include="WP-site*.txt" \
    --include="WIX-site*.txt" \
    --exclude="*" \
    "$REPO_DIR/results/" "$OUT_DIR/"

  echo "[git] commit & push → ${OUTPUT_REMOTE_URL}"
  git add -A
  git commit -m "auto: clean results (WP-site*/WIX-site*) $(date -u +%FT%TZ)" || true
  git push -u origin "$OUTPUT_BRANCH" || true

  echo "[git] sync origin (dz-crawler) dengan state/"
  git -C "$REPO_DIR" add state/ || true
  git -C "$REPO_DIR" commit -m "sync: state $(date -u +%FT%TZ)" || true
  git -C "$REPO_DIR" push origin HEAD || true

  sleep "$COMMIT_INTERVAL"
done
