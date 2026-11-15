#!/usr/bin/env python3
"""Живость адресов эталона: одна проба на URL, без скачивания тела (O14 плана 26, D5).

Зачем отдельный прибор. Колонки ``live_kind``/``live_code``/``live_url`` в листе на
разметку — подсказка человеку, а не утверждение о релевантности. Подсказка обязана быть
**датированной**: замер 09.11.2026 (`2026-11-09-golden-url-liveness.json`) к 15.11 уже
врал — адрес `jatoba.ru/Программа-ТП-2023.pdf` числился `unreachable` (код `-1`) при
живом `206`. Переиспользовать такое число — ровно тот класс ошибки, который проект
называет «документ отстаёт от артефакта» (AGENTS §1.2): решение разметчика зависит от
факта, а факт был шестидневной давности.

Что делает проба: ``GET`` с ``Range: bytes=0-1023`` (тело не скачивается), до пяти
хопов вручную (иначе конечный адрес не виден, а он и есть предмет), таймаут на хоп.
Классы — те же, что в замере 09.11, чтобы файлы сравнивались:

``live``        2xx на исходном адресе (включая ``206`` — это ответ на ``Range``);
``moved``       цепочка редиректов пришла на **живой** адрес, отличный от исходного;
``gone``        ``404``/``410`` либо редирект на собственную страницу ``404``;
``auth``        ``401``/``403`` обычному HTTP (не «сайт умер», а «нас не пускают»);
``unreachable`` TLS/таймаут/DNS — **повторная проба возможна**, это не «страницы нет».

Класс ``moved`` не означает «та же страница»: ``…/17_5/…`` отдаёт 301 на ``…/17_11/…``,
то есть другую редакцию, и решает человек (D5). Прибор это только называет.

Прибор нейтрален (§1.3): он не знает, о чём документы.

Использование:

    .venv/bin/python scripts/measure_golden_liveness.py \\
        --candidate packages/ru-pg-dbms/sources/retrieval_golden_docs.candidate.xlsx \\
        --out docs/reports/measurements/2026-11-15-golden-url-liveness.json

Дальше лист пересобирается с этими колонками:

    .venv/bin/python scripts/prepare_retrieval_golden_docs.py build \\
        --cells packages/ru-pg-dbms/sources/golden_00_cells.csv \\
        --run-db ~/.cohound/stands/ru-pg-dbms/runs/{run}/cohound.db \\
        --out /tmp/candidate.csv --liveness <файл выше>
"""

from __future__ import annotations

import argparse
import csv
import json
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

#: Сколько хопов разворачивать. Пять — как в замере 09.11; больше не нужно: цепочка
#: длиннее почти всегда означает не «переезд», а петлю.
MAX_HOPS = 5
#: Таймаут одного хопа. Медленный ответ — не «страницы нет», а «не дождались».
TIMEOUT_S = 20
#: Параллельность: сайты публичные, но это чужие хосты — 4 потока, не «сколько ядер».
WORKERS = 4
#: Тело не скачивается: запрашивается первый килобайт, этого хватает для кода ответа.
RANGE_HEADER = "bytes=0-1023"
USER_AGENT = "cohound-liveness/1 (+measurement; one request per url)"


@dataclass(frozen=True)
class Probe:
    """Один факт о адресе: код, конечный адрес и класс."""

    code: int
    final_url: str
    kind: str
    hops: int
    error: str | None = None


def _ascii_url(url: str) -> str:
    """URL, пригодный для запроса: путь и query процент-кодируются, хост — идемпотентно.

    Кириллица в пути (`/Программа-ТП-2023.pdf`) в исходном документе написана как есть;
    HTTP-клиент обязан послать закодированную форму, но **сравнение** адресов делает
    вызывающий по канонической форме — иначе «тот же адрес» перестанет совпадать.
    """
    parts = urllib.parse.urlsplit(url.strip())
    path = urllib.parse.quote(parts.path or "/", safe="/%:@!$&'()*+,;=-._~")
    query = urllib.parse.quote(parts.query, safe="=&%:@!$'()*+,;/?-._~")
    return urllib.parse.urlunsplit(
        (parts.scheme or "https", parts.netloc, path, query, "")
    )


def _classify(code: int, final_url: str, origin: str) -> str:
    """Класс по коду и конечному адресу — те же пять классов, что в замере 09.11."""
    if code in (401, 403):
        return "auth"
    if code in (404, 410):
        return "gone"
    if 200 <= code < 300:
        return "live" if final_url == origin else "moved"
    if 300 <= code < 400:
        # Редирект без разрешения: цель недостижима, но адрес жив.
        return "moved"
    return "gone" if 400 <= code < 500 else "unreachable"


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Отключает авто-переход: редирект обязан быть **виден**, а не пройден молча.

    Без этого класса ``urlopen`` сам идёт по 301 и возвращает ``200`` на конечном
    адресе, а исходный адрес в отчёте выглядит живым: класс ``moved`` становится
    недостижимым, и подмена адреса (``…/17_5/…`` → ``…/17_11/…``, то есть **другая
    редакция**) перестаёт быть видимой. Проверено на живом листе: с авто-переходом
    прибор выдал «live 102 из 102» при том, что часть адресов отдаёт 301.
    """

    def redirect_request(  # noqa: PLR0913 - сигнатура родителя
        self,
        req: urllib.request.Request,
        fp: object,
        code: int,
        msg: str,
        headers: object,
        newurl: str,
    ) -> None:
        return None


def _opener() -> urllib.request.OpenerDirector:
    """Opener без авто-переходов и с явным TLS-контекстом."""
    return urllib.request.build_opener(
        _NoRedirect(), urllib.request.HTTPSHandler(context=ssl.create_default_context())
    )


def probe(url: str) -> Probe:
    """Одна проба: ручные хопы, ``Range``, без тела. Никогда не бросает."""
    origin = url.strip()
    current = _ascii_url(origin)
    opener = _opener()
    hops = 0
    try:
        while True:
            # Схема проверяется на **каждом** хопе, а не только на входе: `Location`
            # приходит с чужого хоста, и редирект на `file:`/`ftp:` — не повод открыть
            # его. До запроса допускаются только `http`/`https`; иначе — отказ с
            # причиной, а не попытка открыть что попало.
            if urllib.parse.urlsplit(current).scheme not in {"http", "https"}:
                return Probe(-1, origin, "unreachable", hops, "схема не http(s)")
            # noqa: S310 — схема проверена строкой выше.
            request = urllib.request.Request(  # noqa: S310
                current,
                headers={"Range": RANGE_HEADER, "User-Agent": USER_AGENT},
                method="GET",
            )
            try:
                with opener.open(request, timeout=TIMEOUT_S) as response:
                    code = int(response.status)
                    final = current
                    return Probe(code, final, _classify(code, final, _ascii_url(origin)), hops)
            except urllib.error.HTTPError as error:
                code = int(error.code)
                location = error.headers.get("Location") if error.headers else None
                if 300 <= code < 400 and location and hops < MAX_HOPS:
                    hops += 1
                    current = urllib.parse.urljoin(current, location)
                    continue
                # Конечный адрес — то, куда пришли; для «gone» он и есть улика
                # (302 на собственную 404.html — снятая редакция, а не «сайт лёг»).
                return Probe(
                    code,
                    current,
                    _classify(code, current, _ascii_url(origin)),
                    hops,
                    error.reason if isinstance(error.reason, str) else None,
                )
    except (urllib.error.URLError, TimeoutError, OSError, ValueError) as error:
        return Probe(-1, origin, "unreachable", hops, str(error))


def _sheet_rows(candidate: Path) -> list[dict[str, str]]:
    """Строки листа на разметку; формат — по расширению (``.xlsx`` или CSV).

    Лист живёт в XLSX (в нём размечает человек), а `build` пишет машинный отчёт в CSV.
    Прибор обязан читать оба: иначе «проверить живость» можно только по одной из двух
    форм одного и того же листа, и вторая тихо отстаёт.
    """
    if candidate.suffix.lower() != ".xlsx":
        with candidate.open(encoding="utf-8", newline="") as handle:
            return list(csv.DictReader(handle))
    try:
        import openpyxl
    except ImportError as error:  # pragma: no cover - зависит от окружения
        raise SystemExit(
            "XLSX требует openpyxl (extra `extract`); без него используйте CSV"
        ) from error
    book = openpyxl.load_workbook(candidate, read_only=True, data_only=True)
    try:
        values = book.worksheets[0].iter_rows(values_only=True)
        try:
            header = [("" if cell is None else str(cell)).strip() for cell in next(values)]
        except StopIteration:
            return []
        rows: list[dict[str, str]] = []
        for line in values:
            rows.append(
                {
                    name: ("" if index >= len(line) or line[index] is None else str(line[index]))
                    for index, name in enumerate(header)
                    if name
                }
            )
        return rows
    finally:
        book.close()


def _urls(candidate: Path) -> list[str]:
    """Уникальные адреса листа в порядке первого вхождения."""
    seen: dict[str, None] = {}
    for row in _sheet_rows(candidate):
        url = (row.get("source_url") or "").strip()
        if url:
            seen.setdefault(url, None)
    return list(seen)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", required=True, help="лист эталона (CSV)")
    parser.add_argument("--out", required=True, help="куда записать замер (JSON)")
    parser.add_argument(
        "--workers", type=int, default=WORKERS, help=f"потоков (по умолчанию {WORKERS})"
    )
    args = parser.parse_args(argv)

    urls = _urls(Path(args.candidate))
    if not urls:
        print("ОТКАЗ: в листе нет ни одного адреса", file=sys.stderr)
        return 2

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        probes = list(pool.map(probe, urls))

    payload = {url: asdict(result) for url, result in zip(urls, probes, strict=True)}
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        json.dumps(payload, ensure_ascii=False, indent=1) + "\n", encoding="utf-8"
    )

    counts: dict[str, int] = {}
    for result in probes:
        counts[result.kind] = counts.get(result.kind, 0) + 1
    print(
        f"{out}: {len(urls)} адресов, "
        + ", ".join(f"{kind} {count}" for kind, count in sorted(counts.items()))
    )
    print(f"дата: {datetime.now(UTC).replace(microsecond=0).isoformat()}")
    unreachable = [url for url, result in payload.items() if result["kind"] == "unreachable"]
    if unreachable:
        print("unreachable (повторная проба возможна):", file=sys.stderr)
        for url in unreachable:
            print(f"  {url} — {payload[url]['error']}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
