#!/usr/bin/env python3
"""Статический BFS одного корня: считает уникальные in-scope HTML-страницы.

Без пула краула (6 потоков, RSS ~30 MB): на слабом хосте это дешевле, чем 10 воркеров.
robots.txt не соблюдается — прибор только читает индексы; в отчёте это названо.
"""
from __future__ import annotations

import concurrent.futures as cf
import re
import subprocess
import sys
from urllib.parse import urljoin, urlsplit

UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120 Safari/537.36")
BIN = re.compile(r"\.(css|js|png|jpe?g|gif|svg|ico|woff2?|ttf|eot|map|webp|mp4|avi|zip|gz|tar|pdf|docx?|xlsx?|pptx?|epub|7z|rar)$", re.I)
SKIP = re.compile(r"(/order|/user|/services/|mailto:|tel:|javascript:|#)", re.I)


def fetch(url: str) -> tuple[str, str, str]:
    p = subprocess.run(
        ["curl", "-sS", "-m", "25", "-A", UA, "-w", "\n@@@%{http_code} %{url_effective}", "-L", url],
        capture_output=True, text=True, errors="replace",
    )
    body, _, meta = p.stdout.rpartition("\n@@@")
    code, _, eff = meta.partition(" ")
    return code.strip(), eff.strip(), body


def main() -> int:
    seed = sys.argv[1]
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 3000
    code, eff, body = fetch(seed)
    base = urlsplit(eff or seed)
    prefix = base.path if base.path.endswith("/") else base.path + "/"
    seen: set[str] = {base.path}
    todo: set[str] = set()
    pages: dict[str, int] = {base.path: 200 if code == "200" else 0}

    def expand(html: str, eff_url: str) -> set[str]:
        out = set()
        for h in re.findall(r'href=["\']([^"\']+)["\']', html, flags=re.I):
            u = urlsplit(urljoin(eff_url, h))
            if u.scheme not in ("http", "https") or u.netloc != base.netloc:
                continue
            if not u.path.startswith(prefix) or BIN.search(u.path) or SKIP.search(u.path):
                continue
            out.add(u.path)
        return out

    todo |= expand(body, eff)
    with cf.ThreadPoolExecutor(max_workers=6) as pool:
        while todo and len(seen) < limit:
            batch = list(todo)[:48]
            todo -= set(batch)
            futs = {pool.submit(fetch, f"https://{base.netloc}{p}"): p for p in batch}
            for fut in cf.as_completed(futs):
                p = futs[fut]
                if p in pages:
                    continue
                c, e, b = fut.result()
                pages[p] = int(c) if c.isdigit() else 0
                seen.add(p)
                if c == "200":
                    todo |= expand(b, e) - seen
            print(f"  pages={len(pages)} queued={len(todo)}", file=sys.stderr)
    ok = sum(1 for v in pages.values() if v == 200)
    print(f"ROOT {seed} pages_total={len(pages)} http200={ok} "
          f"truncated={'yes' if todo else 'no'} prefix={prefix}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
