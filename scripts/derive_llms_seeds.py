#!/usr/bin/env python3
"""Прибор: выводит seed-набор продукта из **машинного индекса документации** вендора.

Зачем это нужно (замер 22.11.2026, продукт `gaussdb`). У сайта документации HTML-страница
несёт только каркас: 77 578 байт HTML, из них ~1 900 символов текста, и **ни одного**
абзаца документации — содержимое подгружается скриптом (`<div id="content">` пуст, длинных
китайских прогонов в разметке ноль). То есть обход по `<a href>` собрал бы навигацию вместо
документов, а `corpus_coverage.complete` стал бы `True` на корпусе без материала. Вендор
публикует ту же страницу альтернативным представлением — `rel="alternate"
type="text/markdown"` (`.md`, отдаётся `text/markdown`) и полный индекс
`llms.txt` (для GaussDB — 5.5 МБ, 15 235 ссылок с описаниями).

Инструмент превращает индекс в статический seed-набор: `plan` обязан считать `run_id`
**до** сборки портов и без сети, поэтому вывод прибора пишется в пакет
(`sources/corpus_urls.yaml`) и входит в `seeds_digest`, а не запрашивается на лету.

Что делает:

* читает `llms.txt` (или локальный файл — `--llms-file`);
* группирует ссылки по разделам `## …` и печатает перепись (сколько документов в разделе);
* отбирает объявленные разделы (`--section`, точное имя; `--section-match` — подстрока);
* проверяет каждый URL (``http``/``https``, без учётных данных; битая ссылка вендора —
  исключается с публикацией счёта) и схлопывает дубликаты;
* с `--write` заменяет блок продукта в `corpus_urls.yaml` **целиком** (идемпотентно),
  сохраняя остальные продукты и их комментарии байт-в-байт.

Перепись печатается всегда: «сколько документов объявлено» и «сколько отобрано» — два
разных числа, и второе без первого читается как «столько и есть» (AGENTS §1.1).

Прибор нейтрален (§1.3): он не знает, о чём документы, и не решает, что из них evidence.
"""

from __future__ import annotations

import argparse
import re
import sys
import urllib.request
from pathlib import Path

_SECTION_RE = re.compile(r"^##\s+(?P<name>.+?)\s*$")
_LINK_RE = re.compile(r"\[[^\]]*\]\((?P<url>https?://[^)\s]+)\)")

#: Сколько ссылок печатать в предупреждениях (полный список читается в файле).
_MAX_REPORTED = 20


def fetch_text(source: str) -> str:
    """Читает индекс: URL (http/https) или локальный файл."""
    if source.startswith(("http://", "https://")):
        # S310: схема проверена выше (только http/https) — вход объявлен вызывающим.
        req = urllib.request.Request(  # noqa: S310
            source, headers={"User-Agent": "Mozilla/5.0 (compatible; coHound/1.0)"}
        )
        with urllib.request.urlopen(req, timeout=60) as resp:  # noqa: S310
            return resp.read().decode("utf-8", errors="replace")
    return Path(source).read_text(encoding="utf-8", errors="replace")


def parse_sections(text: str) -> dict[str, list[str]]:
    """``## раздел`` → упорядоченный список URL (порядок вендора сохраняется)."""
    sections: dict[str, list[str]] = {}
    current: str | None = None
    for line in text.splitlines():
        head = _SECTION_RE.match(line)
        if head:
            current = head.group("name").strip()
            sections.setdefault(current, [])
            continue
        link = _LINK_RE.search(line)
        if link and current is not None:
            sections[current].append(link.group("url"))
    return sections


def select(
    sections: dict[str, list[str]],
    *,
    names: list[str],
    matches: list[str],
) -> tuple[dict[str, list[str]], list[str]]:
    """Отбирает разделы; возвращает ``(отобранное, неизвестные имена)``.

    Неизвестное имя раздела — **ошибка**, а не тихий пропуск: переименование раздела у
    вендора иначе уменьшило бы корпус, не изменив ни одного объявленного числа.
    """
    known = set(sections)
    unknown = [name for name in names if name not in known]
    chosen: dict[str, list[str]] = {}
    for name in names:
        if name in sections:
            chosen[name] = sections[name]
    for name in sections:
        if any(fragment in name for fragment in matches):
            chosen.setdefault(name, sections[name])
    return chosen, unknown


def dedupe(urls: list[str]) -> tuple[list[str], int]:
    """Уникальные URL в порядке первого появления; второе число — сколько снято."""
    seen: set[str] = set()
    out: list[str] = []
    for url in urls:
        if url not in seen:
            seen.add(url)
            out.append(url)
    return out, len(urls) - len(out)


#: Причины непригодности seed-адреса. Класс решает **что делать**, а не только «плохо»:
#: одно дело — адрес с паролем (его нельзя брать вовсе и это признак, что индекс не тот),
#: другое — битая ссылка в самом индексе вендора (её нельзя брать, но её наличие не
#: делает непригодным остальной индекс).
_CREDENTIALS = "credentials"
_NESTED_SCHEME = "nested_scheme"


def classify(url: str) -> str | None:
    """Почему адрес не годится как seed (``None`` — годится).

    ``nested_scheme`` — в **пути** сидит целый URL: индекс вендора содержит склейку вида
    ``https://host/section/https://other.host/page?…`` (измерено 22.11.2026 на GaussDB:
    одна такая ссылка в 15 235, метка пустая, ``[]``). Такой адрес не является страницей
    на объявленном хосте — сервер отвечает 404, а прогон получает `not_found` и корень
    `complete=0`, из-за чего всё отсутствие продукта перестаёт быть доказуемым (I2d).
    Класс отдельный именно поэтому: цена пропуска — не «одна страница», а вердикты.
    """
    if not url.startswith(("http://", "https://")):
        return _CREDENTIALS
    rest = url.split("//", 1)[1]
    host, _, path = rest.partition("/")
    if "@" in host:
        return _CREDENTIALS
    if "://" in path:
        return _NESTED_SCHEME
    return None


def _validate(urls: list[str]) -> tuple[list[str], list[str]]:
    """Разделяет непригодные адреса на два класса: ``(с паролем, битые ссылки)``."""
    fatal: list[str] = []
    malformed: list[str] = []
    for url in urls:
        reason = classify(url)
        if reason == _CREDENTIALS:
            fatal.append(url)
        elif reason == _NESTED_SCHEME:
            malformed.append(url)
    return fatal, malformed


def render_block(product: str, urls: list[str], header: list[str]) -> str:
    """Блок продукта для ``corpus_urls.yaml``: комментарий-шапка + строки списка."""
    lines = [f"{product}:", *[f"# {line}" if line else "#" for line in header]]
    lines.extend(f"- {url}" for url in urls)
    return "\n".join(lines) + "\n"


def existing_header(path: Path, product: str) -> list[str]:
    """Комментарий-шапка блока продукта **как он записан в файле**.

    Шапка — авторский текст (почему такой срез, чем измерено), а список ниже — вывод
    прибора. Перегенерация не имеет права стирать объяснение: иначе повторный запуск
    оставил бы числа без причины, а причина без числа.
    """
    lines = path.read_text(encoding="utf-8").splitlines()
    try:
        start = lines.index(f"{product}:")
    except ValueError:
        return []
    header: list[str] = []
    for line in lines[start + 1 :]:
        if line.startswith("#"):
            header.append(line[2:] if line.startswith("# ") else line[1:])
        else:
            break
    return header


def replace_block(path: Path, product: str, block: str) -> str:
    """Заменяет блок продукта целиком, остальные строки не трогает.

    Границы блока — от строки ``product:`` до следующего ключа верхнего уровня
    (``^\\S``) или конца файла. Возвращает результат, файл не пишет.
    """
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines(keepends=True)
    start = None
    for index, line in enumerate(lines):
        if line.rstrip("\n") == f"{product}:":
            start = index
            break
    if start is None:
        raise SystemExit(f"в {path} нет блока «{product}:» — добавьте его вручную")
    end = len(lines)
    for index in range(start + 1, len(lines)):
        if lines[index][:1] not in (" ", "\t", "-", "#") and lines[index].strip():
            end = index
            break
    return "".join(lines[:start]) + block + "".join(lines[end:])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--llms-url", default="", help="URL индекса llms.txt")
    parser.add_argument("--llms-file", default="", help="локальная копия индекса")
    parser.add_argument("--product", required=True, help="product_id в corpus_urls.yaml")
    parser.add_argument(
        "--section",
        action="append",
        default=[],
        help="точное имя раздела `## …` (можно повторять)",
    )
    parser.add_argument(
        "--section-match",
        action="append",
        default=[],
        help="подстрока имени раздела (можно повторять)",
    )
    parser.add_argument(
        "--header",
        action="append",
        default=[],
        help="строка комментария-шапки блока (можно повторять)",
    )
    parser.add_argument("--write", default="", help="путь к corpus_urls.yaml")
    parser.add_argument(
        "--list",
        action="store_true",
        help="только перепись разделов, без отбора и записи",
    )
    args = parser.parse_args(argv)

    source = args.llms_url or args.llms_file
    if not source:
        parser.error("нужен --llms-url или --llms-file")
    sections = parse_sections(fetch_text(source))
    total = sum(len(urls) for urls in sections.values())
    print(f"разделов: {len(sections)}, документов: {total}")
    if args.list:
        for name, urls in sections.items():
            print(f"{len(urls):6d}  {name}")
        return 0

    chosen, unknown = select(sections, names=args.section, matches=args.section_match)
    if unknown:
        print("НЕИЗВЕСТНЫЕ разделы (переименование у вендора?):", file=sys.stderr)
        for name in unknown:
            print(f"  {name!r}", file=sys.stderr)
        return 2
    if not chosen:
        print("не отобрано ни одного раздела — объявите --section/--section-match", file=sys.stderr)
        return 2

    urls: list[str] = []
    print("отобрано:")
    for name, section_urls in chosen.items():
        urls.extend(section_urls)
        print(f"{len(section_urls):6d}  {name}")
    unique, duplicates = dedupe(urls)
    fatal, malformed = _validate(unique)
    if fatal:
        print(f"непригодных URL: {len(fatal)}", file=sys.stderr)
        for url in fatal[:_MAX_REPORTED]:
            print(f"  {url}", file=sys.stderr)
        return 2
    # Битая ссылка вендора — не повод отказать всему индексу, но и не повод промолчать:
    # счёт публикуется, иначе корпус тихо меньше объявленного (AGENTS §1.1, A3).
    usable = [url for url in unique if classify(url) is None]
    excluded = f", исключено битых ссылок: {len(malformed)}" if malformed else ""
    print(f"итого: {len(usable)} уникальных (снято дубликатов: {duplicates}{excluded})")
    if malformed:
        print("битые ссылки индекса (в пути — целый URL; на хосте дают 404):", file=sys.stderr)
        for url in malformed[:_MAX_REPORTED]:
            print(f"  {url}", file=sys.stderr)

    if args.write:
        path = Path(args.write)
        header = list(args.header) or existing_header(path, args.product)
        block = render_block(args.product, usable, header)
        path.write_text(replace_block(path, args.product, block), encoding="utf-8")
        print(f"записано: {path} ({len(usable)} строк)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
