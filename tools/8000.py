#!/usr/bin/env python3
# scan_wp_all_crawls_resume.py
# Sweep SEMUA crawl Common Crawl (urut tahun→minggu) dengan resume lintas-crawl & lintas-WARC.
# Simpan URL halaman yang memuat indikasi:
#   - WordPress (form komentar) -> WP-site*.txt  [opsional via WP_SITE_ENABLED]
#   - Wix (meta generator)      -> WIX-site*.txt          [opsional via WIX_SITE_ENABLED]
# Dedup per-kategori, rolling 1000 baris/berkas, folder per WARC (basename .warc.gz).
# Audit opsional: hits.ndjson (hanya URL BARU).

import io, os, re, sys, gzip, time, json, argparse
import urllib.request, urllib.error, urllib.parse
import re

from urllib.parse import urlparse

def domain_key(u: str) -> str:
    """Kembalikan kunci dedup domain (hostname lower tanpa 'www.')."""
    try:
        host = (urlparse(u).netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]
        # kalau ada port, buang
        if ":" in host:
            host = host.split(":", 1)[0]
        return host
    except Exception:
        return ""

# ===================== Konfigurasi default =====================
# (Nilai ini bisa dioverride oleh ENV; lihat patch ENV di bawah)
HITS_NDJSON            = False     # True = simpan audit hits.ndjson (hanya URL BARU); False = tidak tulis audit
WP_SITE_ENABLED        = True    # True = proses & simpan kandidat WordPress; False = nonaktif
WIX_SITE_ENABLED       = True     # True = proses & simpan kandidat Wix;       False = nonaktif

DEFAULT_FROM_YEAR      = 2013      # None = tanpa batas bawah; atau isi, mis. 2013
DEFAULT_TO_YEAR        = 2025      # None = tanpa batas atas;  atau isi, mis. 2025
DEFAULT_MAX_CRAWLS     = 3         # crawl per run (batch lintas-crawl atau perfolder 1 folder bisa isi ribuan disini ambil 100 saja  1 folder isi 1000 hasil)
DEFAULT_MAX_WARCS      = 120       # WARC per crawl per run (batch intra-crawl)

TIMEOUT                = 60
SLEEP_ERR              = 2
UA                     = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) CC-WP-Wix-AllCrawls-Resume/1.2"

# === Lokasi kerja (REPO_DIR). Semua output & state disimpan di sini ===
REPO_DIR = os.environ.get("REPO_DIR", os.getcwd())

def _as_bool(v, dflt):
    if v is None: return dflt
    return str(v).strip().lower() in ("1", "true", "yes", "on")

# Override dari ENV (jika ada)
HITS_NDJSON      = _as_bool(os.environ.get("HITS_NDJSON"),      HITS_NDJSON)
WP_SITE_ENABLED  = _as_bool(os.environ.get("WP_SITE_ENABLED"),  WP_SITE_ENABLED)
WIX_SITE_ENABLED = _as_bool(os.environ.get("WIX_SITE_ENABLED"), WIX_SITE_ENABLED)

ENV_FROM_YEAR = os.environ.get("SCAN_FROM_YEAR")
ENV_TO_YEAR   = os.environ.get("SCAN_TO_YEAR")
COMMIT_INTERVAL = int(os.environ.get("COMMIT_INTERVAL", "600"))

# Path di dalam REPO_DIR
STATE_DIR              = os.path.join(REPO_DIR, "state")
GLOBAL_CRAWL_IDX_FILE  = os.path.join(STATE_DIR, "crawl_idx.txt")
WARC_PATHS_CACHE_DIR   = os.path.join(REPO_DIR, "warc_paths_cache")
# NDJSON pakai base, akan dirotasi: hits.ndjson, hits_1.ndjson, dst.
OUT_NDJSON_BASE        = os.path.join(REPO_DIR, "hits")

# ===================== Pola HTML (bytes) =====================
# — WordPress (komentar)
RE_WP_COMMENTS_POST = re.compile(rb'wp-comments-post\.php', re.I)
RE_ACTION_WP_POST   = re.compile(rb'action\s*=\s*["\'][^"\']*wp-comments-post\.php[^"\']*["\']', re.I)
RE_COMMENT_FORM_ID  = re.compile(rb'id\s*=\s*["\']commentform["\']', re.I)
RE_COMMENT_FORM_CLS = re.compile(rb'class\s*=\s*["\'][^"\']*\bcomment-form\b[^"\']*["\']', re.I)
# RE_COMMENT_REPLY_JS = re.compile(rb'wp-includes/js/comment-reply(?:\.min)?\.js', re.I)

PATTERNS_WP = [
    ("wp-comments-post", RE_WP_COMMENTS_POST),
    ("action-wp-comments-post", RE_ACTION_WP_POST),
    ("id=commentform", RE_COMMENT_FORM_ID),
    ("class=comment-form", RE_COMMENT_FORM_CLS),
    # ("comment-reply.js", RE_COMMENT_REPLY_JS),
]

# — Wix (meta generator). Robust untuk variasi atribut/urutan.
RE_WIX_GENERATOR = re.compile(
    rb'<meta[^>]+name\s*=\s*["\']generator["\'][^>]+content\s*=\s*["\'][^"\']*Wix[^"\']*["\']',
    re.I
)
PATTERNS_WIX = [
    ("wix-generator", RE_WIX_GENERATOR),
]

# ===================== HTTP helper (retry + backoff) =====================
def http_get(url, timeout=TIMEOUT, max_tries=5):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    backoff = 1.0
    for attempt in range(1, max_tries + 1):
        try:
            return urllib.request.urlopen(req, timeout=timeout)
        except urllib.error.HTTPError as e:
            if e.code in (429, 503) and attempt < max_tries:
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            raise
        except urllib.error.URLError:
            if attempt < max_tries:
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            raise

# ===================== Crawl list (collinfo) =====================
def fetch_crawl_list():
    url = "https://index.commoncrawl.org/collinfo.json"
    with http_get(url) as r:
        data = json.loads(r.read().decode("utf-8", "replace"))
    rows = [row for row in data if "id" in row and row["id"].startswith("CC-MAIN-")]
    rows.sort(key=lambda x: x["id"])  # menaik: tua → baru
    return [row["id"] for row in rows]

def filter_by_year(crawl_ids, yfrom=None, yto=None):
    def year_of(cid):
        try:
            return int(cid.split("-")[2])
        except Exception:
            return None
    out = []
    for cid in crawl_ids:
        y = year_of(cid)
        if y is None:
            continue
        if (yfrom is not None and y < yfrom):
            continue
        if (yto is not None and y > yto):
            continue
        out.append(cid)
    return out

# ===================== State helpers =====================
def ensure_state_dir():
    os.makedirs(STATE_DIR, exist_ok=True)
    os.makedirs(WARC_PATHS_CACHE_DIR, exist_ok=True)

def read_global_crawl_idx():
    try:
        with open(GLOBAL_CRAWL_IDX_FILE, "r", encoding="utf-8") as f:
            return int(f.read().strip())
    except Exception:
        return 0

def write_global_crawl_idx(idx: int):
    with open(GLOBAL_CRAWL_IDX_FILE, "w", encoding="utf-8") as f:
        f.write(str(int(idx)))

def state_files_for_crawl(cid):
    crawl_state_dir = os.path.join(STATE_DIR, cid)
    os.makedirs(crawl_state_dir, exist_ok=True)
    return {
        "cursor": os.path.join(crawl_state_dir, "cursor.txt"),
        "donewarcs": os.path.join(crawl_state_dir, "done_warcs.txt"),
        "warcpaths": os.path.join(WARC_PATHS_CACHE_DIR, f"{cid}.paths"),
    }

def read_cursor(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return int(f.read().strip())
    except Exception:
        return 0

def write_cursor(path, idx: int):
    with open(path, "w", encoding="utf-8") as f:
        f.write(str(int(idx)))

def load_done_set(path):
    s = set()
    if os.path.isfile(path):
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for ln in f:
                b = ln.strip()
                if b:
                    s.add(b)
    return s

def append_done(path, basename):
    with open(path, "a", encoding="utf-8") as f:
        f.write(basename + "\n")

# ===================== warc.paths per crawl =====================
def cache_warc_paths(cid, dest):
    src = f"https://data.commoncrawl.org/crawl-data/{cid}/warc.paths.gz"
    print(f"[i] Fetch warc.paths: {cid} -> {src}")
    with http_get(src) as r:
        raw = r.read()
    txt = gzip.decompress(raw).decode("utf-8", "replace")
    with open(dest, "w", encoding="utf-8") as f:
        f.write(txt)
    total = sum(1 for _ in open(dest, "r", encoding="utf-8"))
    print(f"[✓] {cid}: simpan {os.path.basename(dest)} ({total} baris)")
    return total

# ===================== Output per-folder helper =====================
# def warc_basename_from(s: str) -> str:
#     base = os.path.basename(s)
#     if base.endswith(".warc.gz"):
#         base = base[:-8]
#     return base

def warc_basename_from(s: str) -> str:
    """
    Kembalikan nama folder WARC versi pendek:
    CC-MAIN-YYYYMMDDhhmmss-000NN
    (buang ekor '-ip-…' jika ada)
    """
    base = os.path.basename(s)
    if base.endswith(".warc.gz"):
        base = base[:-8]
    # potong pada '-ip-' jika ada
    cut = base.find("-ip-")
    if cut != -1:
        return base[:cut]
    # fallback: ambil pola umum CC-MAIN-YYYYMMDDhhmmss-000NN
    m = re.match(r"^(CC-MAIN-\d{14}-\d{5})", base)
    return m.group(1) if m else base

def ensure_folder(path: str):
    os.makedirs(path, exist_ok=True)

def list_category_files(folder: str, baseprefix: str):
    files = []
    base0 = os.path.join(folder, f"{baseprefix}.txt")
    if os.path.isfile(base0):
        files.append((0, base0))
    for name in os.listdir(folder):
        if name.startswith(baseprefix) and name.endswith(".txt") and name != f"{baseprefix}.txt":
            mid = name[len(baseprefix):-4]
            if mid.isdigit():
                files.append((int(mid), os.path.join(folder, name)))
    files.sort(key=lambda t: t[0])
    return files

def load_existing_urls(folder: str, baseprefix: str) -> set:
    """
    Muat SET domain yang sudah tersimpan (bukan URL penuh).
    Domain key: hostname lower tanpa 'www.' (port dibuang).
    """
    seen_domains = set()
    for _, fpath in list_category_files(folder, baseprefix):
        try:
            with open(fpath, "r", encoding="utf-8", errors="ignore") as f:
                for ln in f:
                    u = ln.strip()
                    if not u:
                        continue
                    dk = domain_key(u)
                    if dk:
                        seen_domains.add(dk)
        except FileNotFoundError:
            pass
    return seen_domains


def count_lines(filepath: str) -> int:
    try:
        with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
            return sum(1 for _ in f)
    except FileNotFoundError:
        return 0

def pick_target_file(folder: str, baseprefix: str) -> str:
    files = list_category_files(folder, baseprefix)
    base0 = os.path.join(folder, f"{baseprefix}.txt")
    if not files:
        return base0
    last_idx, last_path = files[-1]
    if count_lines(last_path) < 1000:
        return last_path
    new_idx = last_idx + 1
    return os.path.join(folder, f"{baseprefix}{new_idx}.txt")

def append_uri_to_folder(folder: str, baseprefix: str, uri: str, cache_seen: set) -> bool:
    """
    Tulis 1 URI ke file (rolling 1000 baris), DEDUP per DOMAIN.
    cache_seen berisi domain_key yang sudah pernah tersimpan.
    Return True jika BARU ditulis; False jika duplikat domain / invalid.
    """
    if not uri:
        return False
    dk = domain_key(uri)
    if not dk or dk in cache_seen:
        return False

    target = pick_target_file(folder, baseprefix)
    if os.path.isfile(target) and count_lines(target) >= 1000:
        files = list_category_files(folder, baseprefix)
        new_idx = (files[-1][0] + 1) if files else 1
        target = os.path.join(folder, f"{baseprefix}{new_idx}.txt")

    with open(target, "a", encoding="utf-8") as f:
        f.write(uri.strip() + "\n")

    cache_seen.add(dk)
    return True


# ===================== NDJSON audit helper + rotasi =====================
def _ndjson_path(base_path, max_bytes=10*1024*1024):
    """
    Pilih file ndjson aktif dengan batas ukuran (default 10MB).
    hits.ndjson, hits_1.ndjson, hits_2.ndjson, dst.
    """
    p = f"{base_path}.ndjson"
    if not os.path.exists(p) or os.path.getsize(p) < max_bytes:
        return p
    i = 1
    while True:
        p = f"{base_path}_{i}.ndjson"
        if not os.path.exists(p) or os.path.getsize(p) < max_bytes:
            return p
        i += 1

def emit_hit(fh_ndjson, crawl, warc_url, target_uri, domain, reason, snippet_bytes, wrote_new):
    if fh_ndjson is None or not wrote_new:
        return
    fh_ndjson.write(json.dumps({
        "crawl": crawl,
        "warc_url": warc_url,
        "uri": target_uri,
        "domain": domain,
        "reason": reason,
        "snippet": snippet_bytes[:300].decode("utf-8","ignore").replace("\r","")
    }, ensure_ascii=False) + "\n")

# ===================== WARC scanning =====================
def scan_one_warc(crawl, warc_url, fh_ndjson):
    """
    Stream satu file WARC:
      - Jika WP_SITE_ENABLED: cocok PATTERNS_WP → simpan ke WP-site*.txt
      - Jika WIX_SITE_ENABLED: cocok PATTERNS_WIX → simpan ke WIX-site*.txt
      - hits.ndjson hanya mencatat URL BARU (wrote_new=True)
    """
    print(f"[.] WARC: {warc_url}")
    # folder = os.path.join(REPO_DIR, "results", warc_basename_from(warc_url))
    # Simpan per crawl: results/<crawl>/<warc_basename>/
    folder = os.path.join(REPO_DIR, "results", crawl, warc_basename_from(warc_url))
    ensure_folder(folder)

    # cache dedup per-kategori (terpisah)
    seen_wp  = load_existing_urls(folder, "WP-site") if WP_SITE_ENABLED else set()
    seen_wix = load_existing_urls(folder, "WIX-site")         if WIX_SITE_ENABLED else set()

    total_new_wp = 0
    total_new_wx = 0

    try:
        with http_get(warc_url) as resp:
            with gzip.GzipFile(fileobj=resp) as gz:
                buf = b""
                target_uri = None
                domain = None
                in_http_payload = False

                def process_payload(payload):
                    nonlocal target_uri, domain, total_new_wp, total_new_wx
                    if not payload or not target_uri:
                        return

                    # 1) WP (opsional)
                    if WP_SITE_ENABLED:
                        for reason, rx in PATTERNS_WP:
                            if rx.search(payload):
                                wrote = append_uri_to_folder(folder, "WP-site", target_uri, seen_wp)
                                if wrote:
                                    total_new_wp += 1
                                emit_hit(fh_ndjson, crawl, warc_url, target_uri, domain or "", reason, payload, wrote)

                    # 2) Wix (opsional)
                    if WIX_SITE_ENABLED:
                        for reason, rx in PATTERNS_WIX:
                            if rx.search(payload):
                                wrote = append_uri_to_folder(folder, "WIX-site", target_uri, seen_wix)
                                if wrote:
                                    total_new_wx += 1
                                emit_hit(fh_ndjson, crawl, warc_url, target_uri, domain or "", reason, payload, wrote)

                for line in gz:
                    if line.startswith(b"WARC/"):
                        if in_http_payload and buf:
                            process_payload(buf)
                        buf = b""
                        target_uri = None
                        domain = None
                        in_http_payload = False
                        continue

                    if line.startswith(b"WARC-Target-URI: "):
                        target_uri = line.strip().split(b" ", 1)[1].decode("utf-8", "ignore")
                        try:
                            domain = urllib.parse.urlparse(target_uri).netloc.lower()
                        except Exception:
                            domain = ""
                        continue

                    if not in_http_payload and b"Content-Type:" in line and b"text/html" in line.lower():
                        pass

                    if not in_http_payload and line in (b"\r\n", b"\n"):
                        in_http_payload = True
                        buf = b""
                        continue

                    if in_http_payload:
                        buf += line

                if in_http_payload and buf:
                    process_payload(buf)

        print(f"    -> ditulis (WP:+{total_new_wp}, Wix:+{total_new_wx}) ke folder {folder}")
        return True
    except urllib.error.HTTPError as e:
        print(f"[!] HTTP {e.code} {warc_url}")
    except urllib.error.URLError as e:
        print(f"[!] URL error {e.reason} {warc_url}")
    except Exception as e:
        print(f"[!] Error {type(e).__name__}: {e}")
    return False

# ===================== Iterator WARC (sequential + resume per crawl) =====================
def iter_warc_urls_of_crawl(cid, warcpaths_file, start_index=0, max_items=50, skip_done=True, donefile_path=None):
    done = load_done_set(donefile_path) if (skip_done and donefile_path) else set()
    emitted = 0
    with open(warcpaths_file, "r", encoding="utf-8") as f:
        for i, p in enumerate(f):
            if i < start_index:
                continue
            p = p.strip()
            if not p:
                continue
            basename = warc_basename_from(p)
            if skip_done and basename in done:
                continue
            yield i, "https://data.commoncrawl.org/" + p, basename
            emitted += 1
            if emitted >= max_items:
                break

# ===================== MAIN =====================
def main():
    ap = argparse.ArgumentParser(description="Sweep semua crawl Common Crawl (urut tahun) + resume (WP & Wix)")
    ap.add_argument("--from-year", type=int, default=DEFAULT_FROM_YEAR, help="Batas bawah tahun (inklusif), mis. 2013")
    ap.add_argument("--to-year", type=int, default=DEFAULT_TO_YEAR, help="Batas atas tahun (inklusif), mis. 2025")
    ap.add_argument("--start-crawl-id", default=None, help='Mulai dari crawl ID tertentu, mis. "CC-MAIN-2013-20"')
    ap.add_argument("--max-crawls", type=int, default=DEFAULT_MAX_CRAWLS, help="Maks crawl per run")
    ap.add_argument("--max-warcs-per-crawl", type=int, default=DEFAULT_MAX_WARCS, help="Maks WARC/crawl per run")
    ap.add_argument("--reset", action="store_true", help="Reset semua progress (global & per-crawl)")
    args = ap.parse_args()

    ensure_state_dir()

    # Ambil & filter daftar crawl
    crawls_all = fetch_crawl_list()

    # Tahun dari ENV (jika ada) override arg default
    from_year = int(ENV_FROM_YEAR) if ENV_FROM_YEAR else args.from_year
    to_year   = int(ENV_TO_YEAR)   if ENV_TO_YEAR   else args.to_year

    crawls = filter_by_year(crawls_all, from_year, to_year)
    if not crawls:
        print("[!] Tidak ada crawl setelah filter tahun.")
        return

    # Posisi awal crawl
    if args.start_crawl_id:
        try:
            idx = crawls.index(args.start_crawl_id)
            write_global_crawl_idx(idx)
        except ValueError:
            print(f"[!] start-crawl-id {args.start_crawl_id} tidak ada dalam daftar terfilter.")
            return

    # Reset progress
    if args.reset:
        if os.path.isfile(GLOBAL_CRAWL_IDX_FILE):
            os.remove(GLOBAL_CRAWL_IDX_FILE)
        for cid in crawls:
            files = state_files_for_crawl(cid)
            for p in (files["cursor"], files["donewarcs"]):
                if os.path.isfile(p):
                    os.remove(p)
        print("[i] Progress di-reset.")

    gidx = read_global_crawl_idx()
    if gidx >= len(crawls):
        print("[i] Semua crawl terfilter sudah selesai.")
        return

    # NDJSON audit (append lintas-run) dengan rotasi
    fh = open(_ndjson_path(OUT_NDJSON_BASE), "a", encoding="utf-8") if HITS_NDJSON else None

    crawls_done_this_run = 0
    while gidx < len(crawls) and crawls_done_this_run < args.max_crawls:
        cid = crawls[gidx]
        print(f"\n===== CRAWL {cid} (index {gidx}/{len(crawls)-1}) =====")
        files = state_files_for_crawl(cid)

        # cache warc.paths
        if not os.path.isfile(files["warcpaths"]):
            cache_warc_paths(cid, files["warcpaths"])
        else:
            total = sum(1 for _ in open(files["warcpaths"], "r", encoding="utf-8"))
            print(f"[i] {cid}: pakai cache {os.path.basename(files['warcpaths'])} ({total} baris)")

        cursor = read_cursor(files["cursor"])
        processed = ok = 0

        for idx, warc_url, basename in iter_warc_urls_of_crawl(
            cid, files["warcpaths"], start_index=cursor, max_items=args.max_warcs_per_crawl,
            skip_done=True, donefile_path=files["donewarcs"]
        ):
            processed += 1
            if scan_one_warc(cid, warc_url, fh):
                ok += 1
                append_done(files["donewarcs"], basename)
            write_cursor(files["cursor"], idx + 1)

        print(f"[✓] {cid}: batch selesai. WARC diproses: {processed} (ok: {ok})")
        crawls_done_this_run += 1

        total_entries = sum(1 for _ in open(files["warcpaths"], "r", encoding="utf-8"))
        cur = read_cursor(files["cursor"])
        if cur >= total_entries:
            gidx += 1
            write_global_crawl_idx(gidx)
            print(f"[i] Crawl {cid} tuntas. Beralih ke crawl berikutnya (global idx={gidx}).")
        else:
            print(f"[i] Crawl {cid} BELUM tuntas. Cursor={cur}/{total_entries}. Global idx tetap di {gidx}.")
            # rotasi file ndjson sebelum break (opsional)
            if fh:
                fh.close()
                fh = open(_ndjson_path(OUT_NDJSON_BASE), "a", encoding="utf-8") if HITS_NDJSON else None
            break

    if fh:
        fh.close()

    print("\n[✓] Selesai run lintas-crawl.")
    print(f"[i] Global crawl index sekarang: {read_global_crawl_idx()} (0-based pada daftar terfilter)")
    if HITS_NDJSON:
        print(f"[i] Audit: ON -> {os.path.basename(_ndjson_path(OUT_NDJSON_BASE))}")
    else:
        print(f"[i] Audit: OFF")

if __name__ == "__main__":
    main()
