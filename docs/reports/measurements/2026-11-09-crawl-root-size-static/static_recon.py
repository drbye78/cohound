#!/usr/bin/env python3
"""Статика размера корней: код ответа, sitemap <loc>, TOC-ссылки, robots-пути.

Ничего не пишет в пакет/state: только сеть и stdout (JSON). Краул не запускается.
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
from urllib.parse import urljoin, urlsplit

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120 Safari/537.36"
)
BIN = re.compile(
    r"\.(css|js|png|jpe?g|gif|svg|ico|woff2?|ttf|eot|map|webp|mp4|avi|zip|gz|tar)$",
    re.I,
)


def curl(url: str, method: str = "GET", timeout: int = 40) -> tuple[str, str, str]:
    args = ["curl", "-sS", "-m", str(timeout), "-A", UA,
            "-w", "\n@@@%{http_code} %{url_effective}", "-L"]
    if method == "HEAD":
        args.append("-I")
    p = subprocess.run(args + [url], capture_output=True, text=True, errors="replace")
    body, _, meta = p.stdout.rpartition("\n@@@")
    code, _, eff = meta.partition(" ")
    return code.strip(), eff.strip(), body


def hrefs(html: str) -> list[str]:
    return re.findall(r'href=["\']([^"\']+)["\']', html, flags=re.I)


def locs(xml: str) -> list[str]:
    return re.findall(r"<loc>\s*([^<\s]+)\s*</loc>", xml)


def in_scope(url: str, base_eff: str, prefix: str) -> str | None:
    u = urlsplit(urljoin(base_eff, url))
    b = urlsplit(base_eff)
    if u.scheme not in ("http", "https"):
        return None
    if u.netloc != b.netloc:
        return None
    if not u.path.startswith(prefix):
        return None
    if BIN.search(u.path):
        return None
    return u.path


def static_root(url: str, prefix: str | None = None) -> dict:
    code, eff, body = curl(url)
    b = urlsplit(eff or url)
    pref = prefix if prefix is not None else b.path
    if not pref.endswith("/"):
        pref = pref.rsplit("/", 1)[0] + "/"
    paths = {p for h in hrefs(body) if (p := in_scope(h, eff, pref))}
    return {
        "url": url,
        "code": code,
        "effective": eff,
        "bytes": len(body),
        "prefix": pref,
        "toc_links": len(paths),
        "toc_sample": sorted(paths)[:5],
        "body_head": body[:200].replace("\n", " ") if code != "200" else "",
    }


def sitemap(host: str, path: str = "/sitemap.xml", depth: int = 0) -> dict:
    url = f"https://{host}{path}"
    code, eff, body = curl(url)
    L = locs(body)
    feeds = [x for x in L if x.endswith(".xml") or ".xml?" in x]
    out = {"url": url, "code": code, "effective": eff, "loc": len(L), "feeds": len(feeds)}
    if feeds and depth < 1:
        child_total = 0
        child_detail = []
        for f in feeds[:40]:
            c, e, b2 = curl(f)
            n = len(locs(b2))
            child_total += n
            child_detail.append({"feed": f, "code": c, "loc": n})
        out["children"] = child_detail
        out["child_loc_total"] = child_total
    return out


def robots(host: str, prefix: str) -> dict:
    code, eff, body = curl(f"https://{host}/robots.txt")
    dis = [ln.split(":", 1)[1].strip() for ln in body.splitlines()
           if ln.lower().startswith("disallow:")]
    dis = [d for d in dis if d]
    blocking = [d for d in dis if prefix.startswith(d) or d == "/" or d.rstrip("*") in prefix]
    return {"url": f"https://{host}/robots.txt", "code": code,
            "disallow_total": len(dis), "disallow_blocking_prefix": blocking[:10],
            "agents": [ln.split(":", 1)[1].strip() for ln in body.splitlines()
                       if ln.lower().startswith("user-agent:")][:5]}


def main() -> int:
    cases = json.load(open(sys.argv[1]))
    out = {}
    for name, spec in cases.items():
        host = urlsplit(spec["url"]).netloc
        entry = {"static": static_root(spec["url"], spec.get("prefix")),
                 "robots": robots(host, spec.get("prefix") or "/")}
        if spec.get("sitemap", True):
            entry["sitemap"] = sitemap(host, spec.get("sitemap_path", "/sitemap.xml"))
        out[name] = entry
        print(f"{name}: {entry['static']['code']} toc={entry['static']['toc_links']} "
              f"sitemap={entry.get('sitemap', {}).get('loc', '-')} "
              f"robots_disallow={entry['robots']['disallow_total']}", flush=True)
    print(json.dumps(out, ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
