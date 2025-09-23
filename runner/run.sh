#!/usr/bin/env bash
set -euo pipefail
trap 'code=$?; echo "[fatal] runner exited with code $code at $(date -u +%FT%TZ)";' EXIT

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

# ===== Token HTTPS =====
if [ -n "${GH_TOKEN:-}" ]; then
  echo "https://${GH_TOKEN}:x-oauth-basic@github.com" > ~/.git-credentials
  chmod 600 ~/.git-credentials || true
fi

# ===== Lokasi kerja (Nixpacks → /app) =====
export REPO_DIR="${REPO_DIR:-/app}"
mkdir -p "$REPO_DIR/results" "$REPO_DIR/state" "$REPO_DIR/warc_paths_cache"

# ===== Repo OUTPUT (8k) =====
: "${OUTPUT_REMOTE_URL:=https://github.com/crawler-8k/8k.git}"
: "${OUTPUT_GH_TOKEN:?OUTPUT_GH_TOKEN required}"
: "${OUTPUT_BRANCH:=main}"
: "${OUTPUT_FORCE_PUSH:=true}"

OUT_DIR="$REPO_DIR/_out"
mkdir -p "$OUT_DIR"
git -C "$OUT_DIR" init -b "$OUTPUT_BRANCH"
git -C "$OUT_DIR" remote remove origin 2>/dev/null || true
git -C "$OUT_DIR" remote add origin "https://${OUTPUT_GH_TOKEN}:x-oauth-basic@github.com/${OUTPUT_REMOTE_URL#https://github.com/}"

# ===== Repo ORIGIN (dz-crawler) untuk sync state =====
ORIGIN_REMOTE_URL="${ORIGIN_REMOTE_URL:-https://github.com/Diamondszone/dz-crawler.git}"
ORIGIN_DIR="$REPO_DIR/_origin"
if [ -n "${GH_TOKEN:-}" ]; then
  rm -rf "$ORIGIN_DIR"
  git clone "https://${GH_TOKEN}:x-oauth-basic@github.com/${ORIGIN_REMOTE_URL#https://github.com/}" "$ORIGIN_DIR"
fi

# ===== Param siklus =====
: "${MAX_WARCS:=100}"
: "${COMMIT_INTERVAL:=600}"

while true; do
  echo "[run] cycle start (MAX_WARCS=$MAX_WARCS)"
  i=1
  while [ "$i" -le "$MAX_WARCS" ]; do
    echo "[run] WARC #$i / $MAX_WARCS"

    # --- 1) Jalankan 1 WARC ---
    python -u "$REPO_DIR/tools/8000.py" \
      --max-crawls 1 \
      --max-warcs-per-crawl 1 \
      ${SCAN_FROM_YEAR:+--from-year $SCAN_FROM_YEAR} \
      ${SCAN_TO_YEAR:+--to-year $SCAN_TO_YEAR} \
      ${START_CRAWL_ID:+--start-crawl-id $START_CRAWL_ID}

    # --- 2) FLUSH: mirror penuh results → _out (tanpa hapus manual) ---
    echo "[stage] flush → repo 8k (per-WARC, mirror)"
    rsync -a --delete --prune-empty-dirs \
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

    # --- 3) Sync state per-WARC ke dz-crawler ---
    if [ -n "${GH_TOKEN:-}" ]; then
      echo "[state] sync → dz-crawler (per-WARC)"
      if [ ! -d "$ORIGIN_DIR/.git" ]; then
        rm -rf "$ORIGIN_DIR"
        git clone "https://${GH_TOKEN}:x-oauth-basic@github.com/${ORIGIN_REMOTE_URL#https://github.com/}" "$ORIGIN_DIR"
      else
        git -C "$ORIGIN_DIR" pull --rebase || true
      fi
      mkdir -p "$ORIGIN_DIR/state"
      rsync -a --delete "$REPO_DIR/state/" "$ORIGIN_DIR/state/" || true
      ( cd "$ORIGIN_DIR" && git add state/ && git commit -m "sync: state per-WARC $(date -u +%FT%TZ)" || true && git push origin HEAD || true )
    else
      echo "[warn] GH_TOKEN kosong → skip sync state"
    fi

    i=$((i+1))
  done

  echo "[sleep] COMMIT_INTERVAL=$COMMIT_INTERVAL detik"
  sleep "$COMMIT_INTERVAL"
done
