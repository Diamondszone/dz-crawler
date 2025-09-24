#!/usr/bin/env python3
# scan_wp_all_crawls_resume.py
# Sweep SEMUA crawl CC-MAIN (urut tahun).
# Mode per crawl:
#   - WARC_PER_CRAWL = "<angka>"  -> ambil N WARC ACAK per crawl ID, lalu lanjut crawl ID berikutnya
#   - WARC_PER_CRAWL = "all"      -> ambil SEMUA WARC per crawl ID (sequential, resume cursor)
# Output: results/<CrawlID>/<WARC_SHORT>/{WP-site*.txt, WIX-site*.txt}
# Dedup per-folder WARC (by-domain). NDJSON audit opsional.

import os, re, sys, gzip, time, json, random
import urllib.request, urllib.error, urllib.parse
from urllib.parse import urlparse

# =============== CONFIG (UTAMA) ===============
DEFAULT_FROM_YEAR = 2013
DEFAULT_TO_YEAR   = 2025
START_CRAWL_ID    = None      # contoh: "CC-MAIN-2014-22" (jika diisi, range tahun diabaikan)
WARC_PER_CRAWL    = "1"       # "1"/"2"/... atau "all"

# Optional ENV override (boleh diabaikan)
ENV_FROM_YEAR      = os.environ.get("SCAN_FROM_YEAR")
ENV_TO_YEAR        = os.environ.get("SCAN_TO_YEAR")
ENV_START_CRAWL_ID = os.environ.get("START_CRAWL_ID")
ENV_WARC_PER_CRAWL = os.environ.get("WARC_PER_CRAWL")

FROM_YEAR = int(ENV_FROM_YEAR) if ENV_FROM_YEAR else DEFAULT_FROM_YEAR
TO_YEAR   = int(ENV_TO_YEAR)   if ENV_TO_YEAR   else DEFAULT_TO_YEAR
START_CRAWL_ID = ENV_START_CRAWL_ID if ENV_START_CRAWL_ID else START_CRAWL_ID
WARC_PER_CRAWL  = (ENV_WARC_PER_CRAWL or WARC_PER_CRAWL).strip().lower()

# =============== FLAGS OPSIONAL ===============
def _as_bool(v, dflt):
    if v is None: return dflt
    return str(v).strip().lower() in ("1","true","yes","on")

HITS_NDJSON      = _as_bool(os.environ.get("HITS_NDJSON"), False)
WP_SITE_ENABLED  = _as_bool(os.environ.get("WP_SITE_ENABLED"), True)
WIX_SITE_ENABLED = _as_bool(os.environ.get("WIX_SITE_ENABLED"), True)

UA        = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) CC-WP-Wix-Resume/1.3"
TIMEOUT   = 60
SLEEP_ERR = 2

REPO_DIR = os.environ.get("REPO_DIR", os.getcwd())
STATE_DIR              = os.path.join(REPO_DIR, "state")
GLOBAL_CRAWL_IDX_FILE  = os.path.join(STATE_DIR, "crawl_idx.txt")   # kompatibilitas (tidak wajib)
WARC_PATHS_CACHE_DIR   = os.path.join(REPO_DIR, "warc_paths_cache")
OUT_NDJSON_BASE        = os.path.join(REPO_DIR, "hits")

# =============== POLA HTML (bytes) ===============
RE_WP_COMMENTS_POST = re.compile(rb'wp-comments-post\.php', re.I)
RE_ACTION_WP_POST   = re.compile(rb'action\s*=\s*["\'][^"\']*wp-comments-post\.php[^"\']*["\']', re.I)
RE_COMMENT_FORM_ID  = re.compile(rb'id\s*=\s*["\']commentform["\']', re.I)
RE_COMMENT_FORM_CLS = re.compile(rb'class\s*=\s*["\'][^"\']*\bcomment-form\b[^"\']*["\']', re.I)

PATTERNS_WP = [
    ("wp-comments-post", RE_WP_COMMENTS_POST),
    ("action-wp-comments-post", RE_ACTION_WP_POST),
    ("id=commentform", RE_COMMENT_FORM_ID),
    ("class=comment-form", RE_COMMENT_FORM_CLS),
]

RE_WIX_GENERATOR = re.compile(
    rb'<meta[^>]+name\s*=\s*["\']generator["\'][^>]+content\s*=\s*["\'][^"\']*Wix[^"\']*["\']',
    re.I
)
PATTERNS_WIX = [("wix-generator", RE_WIX_GENERATOR)]

# =============== HELPERS ===============
def http_get(url, timeout=TIMEOUT, max_tries=5):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    backoff = 1.0
    for i in range(max_tries):
        try:
            return urllib.request.urlopen(req, timeout=timeout)
        except urllib.error.HTTPError as e:
            if e.code in (429,503) and i < max_tries-1:
                time.sleep(backoff); backoff = min(backoff*2, 30); continue
            raise
        except urllib.error.URLError:
            if i < max_tries-1:
                time.sleep(backoff); backoff = min(backoff*2, 30); continue
            raise

def fetch_crawl_list():
    url = "https://index.commoncrawl.org/collinfo.json"
    with http_get(url) as r:
        data = json.loads(r.read().decode("utf-8","replace"))
    ids = [row["id"] for row in data if "id" in row and row["id"].startswith("CC-MAIN-")]
    ids.sort()  # tua → baru
    return ids

def filter_by_year(crawl_ids, yfrom, yto):
    def year_of(cid):
        try: return int(cid.split("-")[2])
        except Exception: return None
    out = []
    for cid in crawl_ids:
        y = year_of(cid)
        if y is None: continue
        if yfrom is not None and y < yfrom: continue
        if yto   is not None and y > yto:   continue
        out.append(cid)
    return out

def ensure_state_dir():
    os.makedirs(STATE_DIR, exist_ok=True)
    os.makedirs(WARC_PATHS_CACHE_DIR, exist_ok=True)

def state_files_for_crawl(cid):
    """Pastikan path state untuk crawl ID ini, dan kembalikan peta filenya."""
    crawl_state_dir = os.path.join(STATE_DIR, cid)
    os.makedirs(crawl_state_dir, exist_ok=True)
    return {
        "cursor":    os.path.join(crawl_state_dir, "cursor.txt"),
        "donewarcs": os.path.join(crawl_state_dir, "done_warcs.txt"),
        "warcpaths": os.path.join(WARC_PATHS_CACHE_DIR, f"{cid}.paths"),
    }

def read_cursor(path):
    try:
        with open(path, "r", encoding="utf-8") as f: return int(f.read().strip())
    except Exception: return 0

def write_cursor(path, idx: int):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f: f.write(str(int(idx)))

def load_done_set(path):
    s = set()
    if os.path.isfile(path):
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for ln in f:
                b = ln.strip()
                if b: s.add(b)
    return s

def append_done(path, basename):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f: f.write(basename + "\n")

def cache_warc_paths(cid, dest):
    src = f"https://data.commoncrawl.org/crawl-data/{cid}/warc.paths.gz"
    print(f"[i] Fetch warc.paths: {cid} -> {src}")
    with http_get(src) as r: raw = r.read()
    txt = gzip.decompress(raw).decode("utf-8","replace")
    with open(dest, "w", encoding="utf-8") as f: f.write(txt)
    total = sum(1 for _ in open(dest, "r", encoding="utf-8"))
    print(f"[✓] {cid}: simpan {os.path.basename(dest)} ({total} baris)")
    return total

def warc_basename_from(s: str) -> str:
    """Kembalikan nama pendek WARC: CC-MAIN-YYYYMMDDhhmmss-000NN (tanpa '...-ip-...')."""
    base = os.path.basename(s)
    if base.endswith(".warc.gz"): base = base[:-8]
    cut = base.find("-ip-")
    if cut != -1: return base[:cut]
    m = re.match(r"^(CC-MAIN-\d{14}-\d{5})", base)
    return m.group(1) if m else base

def list_category_files(folder: str, baseprefix: str):
    files = []
    base0 = os.path.join(folder, f"{baseprefix}.txt")
    if os.path.isfile(base0): files.append((0, base0))
    for name in os.listdir(folder):
        if name.startswith(baseprefix) and name.endswith(".txt") and name != f"{baseprefix}.txt":
            mid = name[len(baseprefix):-4]
            if mid.isdigit(): files.append((int(mid), os.path.join(folder, name)))
    files.sort(key=lambda t: t[0])
    return files

def load_existing_urls(folder: str, baseprefix: str) -> set:
    """SET domain yang sudah tersimpan (per-folder WARC)."""
    seen = set()
    for _, fpath in list_category_files(folder, baseprefix):
        try:
            with open(fpath, "r", encoding="utf-8", errors="ignore") as f:
                for ln in f:
                    u = ln.strip()
                    if not u: continue
                    dk = (urlparse(u).netloc or "").lower()
                    if dk.startswith("www."): dk = dk[4:]
                    if ":" in dk: dk = dk.split(":",1)[0]
                    if dk: seen.add(dk)
        except FileNotFoundError:
            pass
    return seen

def count_lines(filepath: str) -> int:
    try:
        with open(filepath, "r", encoding="utf-8", errors="ignore") as f: return sum(1 for _ in f)
    except FileNotFoundError: return 0

def pick_target_file(folder: str, baseprefix: str) -> str:
    files = list_category_files(folder, baseprefix)
    base0 = os.path.join(folder, f"{baseprefix}.txt")
    if not files: return base0
    last_idx, last_path = files[-1]
    if count_lines(last_path) < 1000: return last_path
    return os.path.join(folder, f"{baseprefix}{last_idx+1}.txt")

def append_uri_to_folder(folder: str, baseprefix: str, uri: str, cache_seen: set) -> bool:
    """Tulis full URL; dedup per-domain (per-folder WARC)."""
    if not uri: return False
    # gunakan domain_key() kalau mau normalisasi lebih ketat,
    # di sini cukup netloc lower tanpa www/port (agar konsisten dengan file lama)
    dk = (urlparse(uri).netloc or "").lower()
    if dk.startswith("www."): dk = dk[4:]
    if ":" in dk: dk = dk.split(":",1)[0]
    if not dk or dk in cache_seen: return False

    target = pick_target_file(folder, baseprefix)
    if os.path.isfile(target) and count_lines(target) >= 1000:
        files = list_category_files(folder, baseprefix)
        new_idx = (files[-1][0] + 1) if files else 1
        target = os.path.join(folder, f"{baseprefix}{new_idx}.txt")

    with open(target, "a", encoding="utf-8") as f:
        f.write(uri.strip() + "\n")
    cache_seen.add(dk)
    return True

def _ndjson_path(base_path, max_bytes=10*1024*1024):
    p = f"{base_path}.ndjson"
    if not os.path.exists(p) or os.path.getsize(p) < max_bytes: return p
    i = 1
    while True:
        p = f"{base_path}_{i}.ndjson"
        if not os.path.exists(p) or os.path.getsize(p) < max_bytes: return p
        i += 1

def emit_hit(fh_ndjson, crawl, warc_url, target_uri, domain, reason, snippet_bytes, wrote_new):
    if fh_ndjson is None or not wrote_new: return
    fh_ndjson.write(json.dumps({
        "crawl": crawl, "warc_url": warc_url, "uri": target_uri,
        "domain": domain, "reason": reason,
        "snippet": snippet_bytes[:300].decode("utf-8","ignore").replace("\r","")
    }, ensure_ascii=False) + "\n")

# =============== SCAN 1 WARC ===============
def scan_one_warc(crawl, warc_url, fh_ndjson):
    print(f"[.] WARC: {warc_url}")
    out_dir = os.path.join(REPO_DIR, "results", crawl, warc_basename_from(warc_url))
    os.makedirs(out_dir, exist_ok=True)

    seen_wp  = load_existing_urls(out_dir, "WP-site")  if WP_SITE_ENABLED else set()
    seen_wix = load_existing_urls(out_dir, "WIX-site") if WIX_SITE_ENABLED else set()

    total_new_wp = 0; total_new_wx = 0
    try:
        with http_get(warc_url) as resp:
            with gzip.GzipFile(fileobj=resp) as gz:
                buf = b""; target_uri = None; in_http_payload = False; domain = None

                def process_payload(payload):
                    nonlocal total_new_wp, total_new_wx, target_uri, domain
                    if not payload or not target_uri: return
                    if WP_SITE_ENABLED:
                        for reason, rx in PATTERNS_WP:
                            if rx.search(payload):
                                wrote = append_uri_to_folder(out_dir, "WP-site", target_uri, seen_wp)
                                if wrote: total_new_wp += 1
                                emit_hit(fh_ndjson, crawl, warc_url, target_uri, domain or "", reason, payload, wrote)
                    if WIX_SITE_ENABLED:
                        for reason, rx in PATTERNS_WIX:
                            if rx.search(payload):
                                wrote = append_uri_to_folder(out_dir, "WIX-site", target_uri, seen_wix)
                                if wrote: total_new_wx += 1
                                emit_hit(fh_ndjson, crawl, warc_url, target_uri, domain or "", reason, payload, wrote)

                for line in gz:
                    if line.startswith(b"WARC/"):
                        if in_http_payload and buf: process_payload(buf)
                        buf = b""; target_uri = None; domain = None; in_http_payload = False; continue
                    if line.startswith(b"WARC-Target-URI: "):
                        target_uri = line.strip().split(b" ",1)[1].decode("utf-8","ignore")
                        try: domain = urllib.parse.urlparse(target_uri).netloc.lower()
                        except Exception: domain = ""; continue
                        continue
                    if not in_http_payload and b"Content-Type:" in line and b"text/html" in line.lower(): pass
                    if not in_http_payload and line in (b"\r\n", b"\n"):
                        in_http_payload = True; buf = b""; continue
                    if in_http_payload: buf += line
                if in_http_payload and buf: process_payload(buf)

        print(f"    -> ditulis (WP:+{total_new_wp}, Wix:+{total_new_wx}) ke folder {out_dir}")
        return True
    except urllib.error.HTTPError as e:
        print(f"[!] HTTP {e.code} {warc_url}")
    except urllib.error.URLError as e:
        print(f"[!] URL error {e.reason} {warc_url}")
    except Exception as e:
        print(f"[!] Error {type(e).__name__}: {e}")
    return False

# =============== MAIN ===============
def main():
    ensure_state_dir()
    # Ambil SEMUA crawl ID dalam rentang tahun (atau start dari START_CRAWL_ID)
    all_ids = fetch_crawl_list()
    if START_CRAWL_ID:
        try:
            start_idx = all_ids.index(START_CRAWL_ID)
            crawls = all_ids[start_idx:]
        except ValueError:
            print(f"[!] START_CRAWL_ID {START_CRAWL_ID} tidak ada."); sys.exit(1)
    else:
        crawls = filter_by_year(all_ids, FROM_YEAR, TO_YEAR)

    if not crawls:
        print("[!] Tidak ada crawl pada rentang/START_CRAWL_ID."); return

    # NDJSON audit
    fh = open(_ndjson_path(OUT_NDJSON_BASE), "a", encoding="utf-8") if HITS_NDJSON else None

    mode_all = (WARC_PER_CRAWL == "all")
    quota = None if mode_all else max(1, int(WARC_PER_CRAWL))
    print(f"[i] MODE = {'ALL (sequential)' if mode_all else f'RANDOM {quota}/crawl'} | Range {FROM_YEAR}..{TO_YEAR if not START_CRAWL_ID else '(START:'+START_CRAWL_ID+')'}")

    for cid in crawls:
        files = state_files_for_crawl(cid)

        # pastikan warc.paths ada
        if not os.path.isfile(files["warcpaths"]):
            total = cache_warc_paths(cid, files["warcpaths"])
        else:
            total = sum(1 for _ in open(files["warcpaths"], "r", encoding="utf-8"))
            print(f"[i] {cid}: pakai cache {os.path.basename(files['warcpaths'])} ({total} baris)")

        if mode_all:
            # sequential: habiskan semua WARC (resume via cursor)
            cursor = read_cursor(files["cursor"])
            processed = 0
            with open(files["warcpaths"], "r", encoding="utf-8") as f:
                for idx, p in enumerate(f):
                    if idx < cursor: continue
                    p = p.strip()
                    if not p: continue
                    warc_url = "https://data.commoncrawl.org/" + p
                    if scan_one_warc(cid, warc_url, fh):
                        append_done(files["donewarcs"], warc_basename_from(p))
                    write_cursor(files["cursor"], idx + 1)
                    processed += 1
            print(f"[✓] {cid}: selesai (processed={processed}, total={total})")
            continue

        # RANDOM kuota: cek berapa WARC yang sudah diambil utk crawl ini
        done_set = load_done_set(files["donewarcs"])
        if len(done_set) >= quota:
            print(f"[i] {cid}: kuota terpenuhi ({len(done_set)}/{quota}). Skip.")
            continue

        with open(files["warcpaths"], "r", encoding="utf-8") as f:
            paths = [ln.strip() for ln in f if ln.strip()]

        # pilih 1 WARC acak yang belum done
        rand_idx = None
        for _ in range(30):
            i = random.randrange(0, len(paths))
            bn = warc_basename_from(paths[i])
            if bn not in done_set:
                rand_idx = i; break

        if rand_idx is None:
            print(f"[i] {cid}: semua WARC sudah done ({len(done_set)}/{quota}).")
            continue

        warc_url = "https://data.commoncrawl.org/" + paths[rand_idx]
        basename = warc_basename_from(paths[rand_idx])
        if scan_one_warc(cid, warc_url, fh):
            append_done(files["donewarcs"], basename)
            print(f"[✓] {cid}: ambil 1 acak → {basename} (progress {len(done_set)+1}/{quota})")
        # selesai 1 WARC utk crawl ini → biarkan run berikutnya lanjut ke crawl berikutnya
        break

    if fh: fh.close()
    print("\n[✓] Selesai sweep 1 putaran.")

if __name__ == "__main__":
    main()
