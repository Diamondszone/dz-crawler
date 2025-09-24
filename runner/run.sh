#!/usr/bin/env bash
set -euo pipefail
trap 'code=$?; echo "[fatal] runner exited with code $code at $(date -u +%FT%TZ)";' EXIT

# ===== Ensure git/rsync available (fallback runtime installer) =====
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

# ===== Global git identity =====
git config --global user.name  "${GIT_USER_NAME:-Dz Crawler-8K}"
git config --global user.email "${GIT_USER_EMAIL:-dz@example.com}"
git config --global credential.helper store

# ===== HTTPS tokens → ~/.git-credentials =====
# GH_TOKEN: push state ke dz-crawler
# OUTPUT_GH_TOKEN: push hasil ke crawler-8k/8k
if [ -n "${GH_TOKEN:-}" ]; then
  echo "https://${GH_TOKEN}:x-oauth-basic@github.com" > ~/.git-credentials
  chmod 600 ~/.git-credentials || true
fi

# ===== Workdir (Nixpacks → /app, not a git repo) =====
export REPO_DIR="${REPO_DIR:-/app}"
mkdir -p "$REPO_DIR/results" "$REPO_DIR/state" "$REPO_DIR/warc_paths_cache"

# ===== Output repo (8k) =====
: "${OUTPUT_REMOTE_URL:=https://github.com/crawler-8k/8k.git}"
: "${OUTPUT_GH_TOKEN:?OUTPUT_GH_TOKEN required}"
: "${OUTPUT_BRANCH:=main}"
: "${OUTPUT_FORCE_PUSH:=true}"

OUT_DIR="$REPO_DIR/_out"
mkdir -p "$OUT_DIR"
git -C "$OUT_DIR" init -b "$OUTPUT_BRANCH" >/dev/null 2>&1 || true
git -C "$OUT_DIR" remote remove origin 2>/dev/null || true
git -C "$OUT_DIR" remote add origin "https://${OUTPUT_GH_TOKEN}:x-oauth-basic@github.com/${OUTPUT_REMOTE_URL#https://github.com/}"

# Preload branch remote agar README/berkas lama ikut terjaga
if git -C "$OUT_DIR" ls-remote --heads origin "$OUTPUT_BRANCH" >/dev/null 2>&1; then
  git -C "$OUT_DIR" fetch origin "$OUTPUT_BRANCH" --depth=1 || true
  git -C "$OUT_DIR" checkout -B "$OUTPUT_BRANCH" "origin/$OUTPUT_BRANCH" || true
else
  git -C "$OUT_DIR" checkout -B "$OUTPUT_BRANCH" || true
fi
# Per-repo identity (opsional)
[ -n "${OUTPUT_USER_NAME:-}" ]  && git -C "$OUT_DIR" config user.name  "$OUTPUT_USER_NAME"
[ -n "${OUTPUT_USER_EMAIL:-}" ] && git -C "$OUT_DIR" config user.email "$OUTPUT_USER_EMAIL"

# guard: if .git broken, reinit
if ! git -C "$OUT_DIR" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  rm -rf "$OUT_DIR/.git"
  git -C "$OUT_DIR" init -b "$OUTPUT_BRANCH"
  git -C "$OUT_DIR" remote add origin "https://${OUTPUT_GH_TOKEN}:x-oauth-basic@github.com/${OUTPUT_REMOTE_URL#https://github.com/}"
fi

# ===== Origin repo (dz-crawler) for state sync =====
ORIGIN_REMOTE_URL="${ORIGIN_REMOTE_URL:-https://github.com/Diamondszone/dz-crawler.git}"
ORIGIN_DIR="$REPO_DIR/_origin"
if [ -n "${GH_TOKEN:-}" ]; then
  rm -rf "$ORIGIN_DIR"
  git clone "https://${GH_TOKEN}:x-oauth-basic@github.com/${ORIGIN_REMOTE_URL#https://github.com/}" "$ORIGIN_DIR"
  [ -n "${ORIGIN_USER_NAME:-}" ]  && git -C "$ORIGIN_DIR" config user.name  "$ORIGIN_USER_NAME"
  [ -n "${ORIGIN_USER_EMAIL:-}" ] && git -C "$ORIGIN_DIR" config user.email "$ORIGIN_USER_EMAIL"
fi

# ===== Params =====
: "${COMMIT_INTERVAL:=600}"         # JEDA saat berpindah CRAWL ID
PY_SCRIPT="${PY_SCRIPT:-$REPO_DIR/tools/scan_wp_all_crawls_resume.py}"

PREV_CRAWL_ID=""

while true; do
  echo "[run] start one WARC"

  # --- 1) process exactly one WARC ---
  python -u "$PY_SCRIPT" \
    ${SCAN_FROM_YEAR:+--from-year $SCAN_FROM_YEAR} \
    ${SCAN_TO_YEAR:+--to-year $SCAN_TO_YEAR} \
    ${START_CRAWL_ID:+--start-crawl-id $START_CRAWL_ID}

  # --- 2) flush results → 8k (append-only; protect README & history) ---
  echo "[stage] flush → repo 8k (append)"
  HAS_FILES=$(find "$REPO_DIR/results" -type f \( -name 'WP-site*.txt' -o -name 'WIX-site*.txt' \) -print -quit || true)
  if [ -n "$HAS_FILES" ]; then
    git -C "$OUT_DIR" pull --rebase --ff-only origin "$OUTPUT_BRANCH" || true
    rsync -a --prune-empty-dirs \
      --exclude='.git/' --exclude='.git/**' \
      --include='*/' \
      --include='WP-site*.txt' \
      --include='WIX-site*.txt' \
      --exclude='*' \
      "$REPO_DIR/results/" "$OUT_DIR/"

    git -C "$OUT_DIR" add -A

    # build commit message (Added/Updated)
    CHANGES=$(git -C "$OUT_DIR" diff --cached --name-status | grep -E 'WP-site.*\.txt|WIX-site.*\.txt' || true)
    if [ -n "$CHANGES" ]; then
      NEW_FILES=$(echo "$CHANGES" | awk '$1=="A"{print $2}')
      MOD_FILES=$(echo "$CHANGES" | awk '$1=="M"{print $2}')
      format_list () {
        local list="$1" title="$2"
        local n shown
        n=$(echo "$list" | sed '/^$/d' | wc -l | tr -d ' ')
        if [ "$n" -eq 0 ]; then
          echo ""
        else
          echo "$title ($n):"
          shown=$(echo "$list" | sed '/^$/d' | head -n 20 | sed 's/^/ - /')
          echo "$shown"
          if [ "$n" -gt 20 ]; then echo " - … (+$((n-20)) more)"; fi
        fi
      }
      BLK_NEW=$(format_list "$NEW_FILES" "Added")
      BLK_MOD=$(format_list "$MOD_FILES" "Updated")

      LAST_STATE_FILE=$(ls -1t "$REPO_DIR"/state/*/done_warcs.txt 2>/dev/null | head -n1 || true)
      if [ -n "$LAST_STATE_FILE" ]; then
        CRAWL_ID=$(basename "$(dirname "$LAST_STATE_FILE")")
        LAST_WARC=$(tail -n1 "$LAST_STATE_FILE" | tr -d '\r\n')
      fi
      COMMIT_TITLE="auto(per-WARC): WP/WIX results $(date -u +%FT%TZ)"
      [ -n "${CRAWL_ID:-}" ] && COMMIT_TITLE="$COMMIT_TITLE | $CRAWL_ID"
      [ -n "${LAST_WARC:-}" ] && COMMIT_TITLE="$COMMIT_TITLE / $LAST_WARC"

      COMMIT_BODY=""
      [ -n "$BLK_NEW" ] && COMMIT_BODY="$COMMIT_BODY$BLK_NEW\n"
      [ -n "$BLK_MOD" ] && COMMIT_BODY="$COMMIT_BODY$BLK_MOD\n"

      git -C "$OUT_DIR" commit -m "$COMMIT_TITLE" -m "$(printf "%b" "$COMMIT_BODY")" || echo "[stage] nothing to commit"
    else
      echo "[stage] nothing to commit"
    fi

    if [ "${OUTPUT_FORCE_PUSH:-true}" = "true" ]; then
      git -C "$OUT_DIR" push -u origin "$OUTPUT_BRANCH" --force-with-lease || true
    else
      git -C "$OUT_DIR" push -u origin "$OUTPUT_BRANCH" || true
    fi
  else
    echo "[stage] results empty → skip push (protect 8k)"
  fi

  # --- 3) sync state per-WARC → dz-crawler ---
  if [ -n "${GH_TOKEN:-}" ]; then
    echo "[state] sync → dz-crawler"
    if [ ! -d "$ORIGIN_DIR/.git" ]; then
      rm -rf "$ORIGIN_DIR"
      git clone "https://${GH_TOKEN}:x-oauth-basic@github.com/${ORIGIN_REMOTE_URL#https://github.com/}" "$ORIGIN_DIR"
      [ -n "${ORIGIN_USER_NAME:-}" ]  && git -C "$ORIGIN_DIR" config user.name  "$ORIGIN_USER_NAME"
      [ -n "${ORIGIN_USER_EMAIL:-}" ] && git -C "$ORIGIN_DIR" config user.email "$ORIGIN_USER_EMAIL"
    else
      git -C "$ORIGIN_DIR" pull --rebase || true
    fi
    mkdir -p "$ORIGIN_DIR/state"
    rsync -a --delete "$REPO_DIR/state/" "$ORIGIN_DIR/state/" || true
    ( cd "$ORIGIN_DIR" && git add state/ && git commit -m "sync: state per-WARC $(date -u +%FT%TZ)" || true && git push origin HEAD || true )
  else
    echo "[warn] GH_TOKEN kosong → skip sync state"
  fi

  # --- 4) cleanup processed WARC locally ---
  LAST_STATE_FILE=$(ls -1t "$REPO_DIR"/state/*/done_warcs.txt 2>/dev/null | head -n1 || true)
  if [ -n "$LAST_STATE_FILE" ]; then
    CURR_CRAWL_ID=$(basename "$(dirname "$LAST_STATE_FILE")")
    LAST_WARC=$(tail -n1 "$LAST_STATE_FILE" | tr -d '\r\n')
    if [ -n "${CURR_CRAWL_ID:-}" ] && [ -n "${LAST_WARC:-}" ]; then
      TARGET_DIR="$REPO_DIR/results/$CURR_CRAWL_ID/$LAST_WARC"
      if [ -d "$TARGET_DIR" ]; then
        echo "[cleanup] remove local $TARGET_DIR"
        rm -rf "$TARGET_DIR" || true
      fi
    fi
  fi

  # --- 5) sleep hanya saat pindah crawl ID ---
  if [ -n "${PREV_CRAWL_ID:-}" ] && [ -n "${CURR_CRAWL_ID:-}" ] && [ "$PREV_CRAWL_ID" != "$CURR_CRAWL_ID" ]; then
    echo "[sleep] switched crawl: $PREV_CRAWL_ID → $CURR_CRAWL_ID, sleep ${COMMIT_INTERVAL}s"
    sleep "$COMMIT_INTERVAL"
  fi
  PREV_CRAWL_ID="${CURR_CRAWL_ID:-$PREV_CRAWL_ID}"

done
