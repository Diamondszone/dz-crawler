#!/usr/bin/env bash
set -euo pipefail

# ===== Pastikan git/rsync tersedia (fallback runtime installer) =====
if ! command -v git >/dev/null 2>&1; then
  echo "[setup] installing git/rsync..."
  export DEBIAN_FRONTEND=noninteractive
  if command -v apt-get >/dev/null 2>&1; then
    apt-get update -y && apt-get install -y git rsync ca-certificates tzdata openssh-client
  elif command -v apk >/dev/null 2>&1; then
    apk add --no-cache git rsync ca-certificates tzdata openssh-client
  elif command -v microdnf >/dev/null 2>&1; then
    microdnf install -y git rsync ca-certificates tzdata || true
  else
    echo "[setup] no supported package manager found; exiting"; exit 1
  fi
fi

# ===== Identitas git (GLOBAL) =====
git config --global user.name  "${GIT_USER_NAME:-Dz Crawler-8K}"
git config --global user.email "${GIT_USER_EMAIL:-dz@example.com}"
git config --global credential.helper store

# ===== Token HTTPS (gunakan .git-credentials) =====
# GH_TOKEN: push state ke dz-crawler
# OUTPUT_GH_TOKEN: push hasil ke crawler-8k/8k
if [ -n "${GH_TOKEN:-}" ]; then
  echo "https://${GH_TOKEN}:x-oauth-basic@github.com" > ~/.git-credentials
  chmod 600 ~/.git-credentials || true
fi

# ===== Lokasi kerja (Nixpacks → /app, BUKAN repo git) =====
export REPO_DIR="${REPO_DIR:-/app}"
mkdir -p "$REPO_DIR/results" "$REPO_DIR/state" "$REPO_DIR/warc_paths_cache"

# ===== Repo OUTPUT (8k) – hasil BERSIH =====
: "${OUTPUT_REMOTE_URL:=https://github.com/crawler-8k/8k.git}"
: "${OUTPUT_GH_TOKEN:?OUTPUT_GH_TOKEN required}"
: "${OUTPUT_BRANCH:=main}"
: "${OUTPUT_FORCE_PUSH:=true}"

OUT_DIR="$REPO_DIR/_out"
rm -rf "$OUT_DIR/.git" 2>/dev/null || true
mkdir -p "$OUT_DIR"
git -C "$OUT_DIR" init -b "$OUTPUT_BRANCH"
git -C "$OUT_DIR" remote add origin "https://${OUTPUT_GH_TOKEN}:x-oauth-basic@github.com/${OUTPUT_REMOTE_URL#https://github.com/}" || true

# ===== Repo ORIGIN (dz-crawler) – untuk sync state =====
ORIGIN_REMOTE_URL="${ORIGIN_REMOTE_URL:-https://github.com/Diamondszone/dz-crawler.git}"
ORIGIN_DIR="$REPO_DIR/_origin"
if [ -n "${GH_TOKEN:-}" ]; then
  rm -rf "$ORIGIN_DIR"
  git clone "https://${GH_TOKEN}:x-oauth-basic@github.com/${ORIGIN_REMOTE_URL#https://github.com/}" "$ORIGIN_DIR"
fi

# ===== Param siklus =====
: "${MAX_WARCS:=100}"       # berapa WARC per siklus
: "${COMMIT_INTERVAL:=600}" # jeda setelah 1 siklus selesai (detik)

while true; do
  echo "[run] cycle start (MAX_WARCS=$MAX_WARCS)"

  i=1
  while [ "$i" -le "$MAX_WARCS" ]; do
    echo "[run] WARC #$i / $MAX_WARCS"

    # --- 1) Proses PERSIS 1 WARC (state melanjutkan otomatis) ---
    python -u "$REPO_DIR/tools/8000.py" \
      --max-crawls 1 \
      --max-warcs-per-crawl 1 \
      ${SCAN_FROM_YEAR:+--from-year $SCAN_FROM_YEAR} \
      ${SCAN_TO_YEAR:+--to-year $SCAN_TO_YEAR} \
      ${START_CRAWL_ID:+--start-crawl-id $START_CRAWL_ID}

    # --- 2) Push hasil BERSIH ke repo 8k ---
    echo "[stage] flush → repo 8k (per-WARC)"
    # bersihkan root _out/ kecuali .git
    find "$OUT_DIR" -mindepth 1 -maxdepth 1 ! -name ".git" -exec rm -rf {} +
    # salin hanya WP-site*.txt / WIX-site*.txt dengan struktur folder (tahun/crawl/warc)
    rsync -a --prune-empty-dirs \
      --include="*/" \
      --include="WP-site*.txt" \
      --include="WIX-site*.txt" \
      --exclude="*" \
      "$REPO_DIR/results/" "$OUT_DIR/"

    git -C "$OUT_DIR" add -A
    git -C "$OUT_DIR" commit -m "auto(per-WARC): clean results (WP/WIX) $(date -u +%FT%TZ)" || echo "[stage] nothing to commit"
    if [ "$OUTPUT_FORCE_PUSH" = "true" ]; then
      git -C "$OUT_DIR" push -u origin "$OUTPUT_BRANCH" --force || true
    else
      git -C "$OUT_DIR" push -u origin "$OUTPUT_BRANCH" || true
    fi

    # --- 3) Sync state ke dz-crawler (per-WARC) ---
    if [ -n "${GH_TOKEN:-}" ]; then
      echo "[state] sync → dz-crawler (per-WARC)"
      if [ ! -d "$ORIGIN_DIR/.git" ]; then
        # kalau hilang, clone ulang
        rm -rf "$ORIGIN_DIR"
        git clone "https://${GH_TOKEN}:x-oauth-basic@github.com/${ORIGIN_REMOTE_URL#https://github.com/}" "$ORIGIN_DIR"
      else
        git -C "$ORIGIN_DIR" pull --rebase || true
      fi
      mkdir -p "$ORIGIN_DIR/state"
      rsync -a --delete "$REPO_DIR/state/" "$ORIGIN_DIR/state/" || true
      (
        cd "$ORIGIN_DIR"
        git add state/ || true
        git commit -m "sync: state per-WARC $(date -u +%FT%TZ)" || true
        git push origin HEAD || true
      )
    else
      echo "[warn] GH_TOKEN kosong → skip sync state"
    fi

    i=$((i+1))
  done

  echo "[sleep] COMMIT_INTERVAL=$COMMIT_INTERVAL detik"
  sleep "$COMMIT_INTERVAL"
done
