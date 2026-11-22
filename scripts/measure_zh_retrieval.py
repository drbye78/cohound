#!/usr/bin/env python3
"""Прибор: достаёт ли объявленный ретривал **китайский** корпус — и меняет ли это dense-плечо.

Вопрос замера. Профиль ``ru-pg-dbms`` впервые получил продукт с родным китайским корпусом
(``gaussdb``: 5 555 документов, 50 813 чанков, весь текст по-китайски). При этом каталог
написан на русском, ``concept_terms`` содержит только ``ru``/``en``, а ось
``retrieval.query_lang`` знает только ``ru`` | ``en`` | ``all``. То есть у Ш6 нет способа
спросить корпус **на его языке**: из трёх слотов бюджета ``title`` и термины приходят
русскими. Прежде чем добавлять китайские термины в каталог (это правка ``catalog_digest``,
то есть новый ``run_id`` и 181 строка ручной работы), надо знать **цену** этого: находит
ли объявленный ретривал китайский материал вообще, и меняет ли ответ dense-плечо.

Что именно измеряется — **находимость якоря**, а не вердикт. Китайские документы сохраняют
латинские идентификаторы дословно (``JSONB``, ``TOAST``, ``pg_cron``, ``WAL``) — это
единственный мост между русским запросом и китайским текстом, который виден без перевода.
Поэтому для каждого параметра берётся **дискриминативный** якорь из его собственных
``title``+``concept_terms`` (объявленное правило ниже) и проверяется, попал ли в top-K хоть
один чанк, содержащий этот якорь. «Попал якорь» ≠ «документ отвечает на вопрос параметра»:
это отвечает только на «достал ли поиск материал этого параметра».

Правило якоря объявлено, а не подобрано: ASCII-токен годится, если он **всё в верхнем
регистре** (``JSONB``, ``SLA``), либо содержит цифру/подчёркивание (``pg_cron``), либо
внутреннюю заглавную (``pgBouncer``). Обычные английские слова (``support``, ``version``,
``based``) не годятся: они есть в любом чанке и превратили бы метрику в «поиск что-нибудь
нашёл». Цена правила названа числом: дискриминативный якорь есть у **68 из 181** параметра
каталога ``ru-pg-dbms`` (перепись печатается), и замер идёт только по ним — остальные 113
этот прибор измерить не может, и он этого не скрывает.

Сравнение — **парное** по параметрам: те же параметры, тот же корпус, меняется одно плечо
(``R0`` лексика против ``R2`` RRF). Публикуются дискордантные пары, точный двусторонний
критерий МакНемара и бутстрэп-интервал разницы долей (пере-выборка по параметрам, сид
объявлен). Две агрегированные доли без этого смешивают эффект плеча с составом выборки.

Границы, названные заранее:

* **Вердиктов не публикует.** Никаких ячеек, никаких записей: БД прогона читается только
  на чтение. Вопрос замера — поиск, а не оценка.
* **Один продукт.** ``--product`` обязателен: китайскость корпуса — свойство партиции, и
  смешивать её с русскими партициями значило бы мерить состав, а не язык.
* **Якорь — не термин параметра.** Он проверяет, что материал найден, а не что он отвечает
  на вопрос; параметры без латинского идентификатора прибором не покрыты.
* **Сеть не нужна, веса нужны.** ``R2`` строит объявленный эмбеддер из ``deploy.yaml``
  (``embed.model_id``+``revision``) — это секунды и гигабайты RSS, поэтому профиль ``R0``
  считается отдельной сборкой портов (без весов) и его цена не приписывается гибриду.

Использование::

    .venv/bin/python scripts/measure_zh_retrieval.py \\
        --run-id ru-pg-dbms-0ada4b8f0adb --product gaussdb \\
        --catalog packages/ru-pg-dbms/catalog.csv \\
        --out docs/reports/measurements/2026-11-22-zh-retrieval.md

Стенд требует **оба** ``COHOUND_HOME`` и ``COHOUND_PATHS_FILE`` (AGENTS §3.1).
"""

from __future__ import annotations

import argparse
import json
import random
import re
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from core.cli.wiring import build_retrieval_ports
from core.config.deploy import load_deploy_config
from core.config.paths import resolve_paths
from core.config.run_config import load_catalog
from core.domain.models import CatalogRow
from core.pipeline.prompt import render_queries
from core.pipeline.retrieve import RetrievalProfile, retrieve
from core.store.corpus_reader import CorpusReader
from core.util.stats import mcnemar_exact_p

#: Сколько запросов шлёт Ш6 на ячейку (``MAX_QUERIES``). Объявлено, а не взято из модуля:
#: прибор обязан назвать условие, при котором получены числа.
MAX_QUERIES = 3

#: Настройки языка оси. ``zh`` здесь **нет** намеренно: его в оси не существует, и
#: прибор не имеет права мерить настройку, которой нельзя объявить в прогоне.
LANGS: tuple[str, ...] = ("ru", "en", "all")

#: Плечи ретривала, между которыми идёт парное сравнение.
PROFILES: tuple[str, ...] = ("R0", "R2")

BOOTSTRAP_ITERS = 5000
BOOTSTRAP_SEED = 20261122

_TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z0-9_.\-]{2,}")


def is_anchor(token: str) -> bool:
    """Дискриминативный ли ASCII-токен — правило объявлено в докстринге модуля."""
    if len(token) < 3:
        return False
    if re.fullmatch(r"[A-Z0-9_]{3,}", token):
        return True
    if re.search(r"[0-9_]", token):
        return True
    return bool(re.search(r"[a-z][A-Z]", token))


def anchors_of(param: CatalogRow) -> list[str]:
    """Якоря параметра из его собственных ``title``+``concept_terms`` (все языки)."""
    blob = " ".join(
        [param.title, *[term for terms in (param.concept_terms or {}).values() for term in terms]]
    )
    seen: set[str] = set()
    out: list[str] = []
    for token in _TOKEN_RE.findall(blob):
        if not is_anchor(token):
            continue
        key = token.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(token)
    return out


def anchor_census(rows: Sequence[CatalogRow]) -> tuple[dict[str, list[str]], dict[str, int]]:
    """Перепись якорей: ``({param: уникальные якоря}, {якорь: сколько параметров})``.

    **Уникальность — не украшение, а условие осмысленности метрики.** Замер 22.11.2026
    показал это числом: при «любом» якоре попадание одинаково у всех трёх настроек языка
    (47/68 при ``ru``, ``en`` и ``all``), потому что общий токен (``PostgreSQL`` у 6
    параметров, ``SQL`` у 5) находится в любом чанке документации — метрика мерила
    «страница вообще про СУБД», а не «достали материал **этого** параметра». Поэтому
    считается только якорь, которого нет ни у одного другого параметра каталога: так
    попадание адресует параметр, а не предмет.
    """
    per_param = {row.param_id: anchors_of(row) for row in rows}
    df: dict[str, int] = {}
    for anchors in per_param.values():
        for anchor in set(anchors):
            df[anchor.casefold()] = df.get(anchor.casefold(), 0) + 1
    unique = {
        param: [anchor for anchor in anchors if df[anchor.casefold()] == 1]
        for param, anchors in per_param.items()
    }
    return unique, df


@dataclass(frozen=True)
class CellResult:
    """Итог одного (параметр, язык, плечо): что спросили и что достали."""

    param_id: str
    lang: str
    profile: str
    queries: tuple[str, ...]
    chunks: int
    hit: bool


def _search(
    *,
    param: CatalogRow,
    lang: str,
    profile: str,
    anchors: Sequence[str],
    product: str,
    top_k: int,
    lexical_index: object,
    vector_index: object,
    embedder: object,
) -> CellResult:
    """Запросы объявленного языка → объединение top-K чанков → есть ли в них якорь."""
    queries = tuple(render_queries(param, max_queries=MAX_QUERIES, query_lang=lang))
    texts: dict[str, str] = {}
    for query in queries:
        vector = None
        if profile in (RetrievalProfile.R1.value, RetrievalProfile.R2.value):
            vector = embedder.embed([query])[0]
        found = retrieve(
            query_text=query,
            query_vector=vector,
            product_ids=[product],
            profile=RetrievalProfile(profile),
            cutoff_k=top_k,
            lexical_index=lexical_index,
            vector_index=vector_index,
        ).chunks
        for chunk in found:
            texts.setdefault(chunk.chunk_id, chunk.text)
    folded = [text.casefold() for text in texts.values()]
    hit = any(anchor.casefold() in text for anchor in anchors for text in folded)
    return CellResult(
        param_id=param.param_id,
        lang=lang,
        profile=profile,
        queries=queries,
        chunks=len(texts),
        hit=hit,
    )


def _bootstrap_delta(a: Sequence[bool], b: Sequence[bool]) -> tuple[float, float, float]:
    """Парный бутстрэп разницы долей: ``(delta, lo, hi)`` при 95 %.

    Пере-выборка идёт **по параметрам**: ячейка — пара «попал/не попал» в обеих
    настройках, поэтому состав выборки не смешивается с эффектом плеча.
    """
    if not a or len(a) != len(b):
        raise ValueError("бутстрэп: нужны две равные непустые выборки")
    n = len(a)
    deltas = [int(x) - int(y) for x, y in zip(a, b, strict=True)]
    delta = sum(deltas) / n
    rng = random.Random(BOOTSTRAP_SEED)  # noqa: S311 — воспроизводимость, не криптография
    samples: list[float] = []
    for _ in range(BOOTSTRAP_ITERS):
        samples.append(sum(deltas[rng.randrange(n)] for _ in range(n)) / n)
    samples.sort()
    return delta, samples[int(0.025 * (len(samples) - 1))], samples[int(0.975 * (len(samples) - 1))]


def _paired(
    cells: Sequence[CellResult],
    *,
    axis: str,
    fixed: str,
    left: str,
    right: str,
) -> dict[str, object] | None:
    """Парное сравнение двух значений **одной** оси: дискордансы, МакНемар, интервал.

    ``axis="profile"`` — сравниваются плечи при фиксированном языке (``fixed``);
    ``axis="lang"`` — сравниваются языки оси при фиксированном плече. Одна функция на обе
    оси намеренно: это одна арифметика, и второй её экземпляр разошёлся бы с первым.

    Дельта считается как ``right − left`` и так и печатается: знак обязан совпадать с
    подписью, иначе «R0 → R2» с отрицательной дельтой читается как «R0 хуже», хотя
    арифметика противоположна.
    """
    by_param: dict[str, dict[str, bool]] = {}
    for cell in cells:
        if (cell.lang if axis == "profile" else cell.profile) != fixed:
            continue
        key = cell.profile if axis == "profile" else cell.lang
        by_param.setdefault(cell.param_id, {})[key] = cell.hit
    paired = [(row[left], row[right]) for row in by_param.values() if left in row and right in row]
    if not paired:
        return None
    a = [x for x, _ in paired]
    b = [y for _, y in paired]
    both = sum(1 for x, y in paired if x and y)
    only_left = sum(1 for x, y in paired if x and not y)
    only_right = sum(1 for x, y in paired if y and not x)
    neither = sum(1 for x, y in paired if not x and not y)
    delta, lo, hi = _bootstrap_delta(b, a)
    return {
        "axis": axis,
        "fixed": fixed,
        "left": left,
        "right": right,
        "n": len(paired),
        "left_rate": sum(a) / len(a),
        "right_rate": sum(b) / len(b),
        "both": both,
        "only_left": only_left,
        "only_right": only_right,
        "neither": neither,
        "delta": delta,
        "ci": [lo, hi],
        "p": mcnemar_exact_p(only_left, only_right),
    }

def _fmt(value: float) -> str:
    return f"{value:+.4f}"


def _report(
    *,
    cells: Sequence[CellResult],
    paired: Sequence[dict[str, object]],
    run_id: str,
    product: str,
    top_k: int,
    anchor_params: int,
    total_params: int,
    skipped: Sequence[str],
    shared: Sequence[tuple[str, int]],
) -> str:
    lines = [
        "# Достаёт ли объявленный ретривал китайский корпус",
        "",
        f"* прогон: `{run_id}`, продукт-партиция: `{product}`",
        f"* профиль корпуса: `{product}` (весь текст по-китайски)",
        f"* метрика: попадание **уникального** латинского якоря параметра в top-{top_k}",
        f"* параметров с уникальным якорем: **{anchor_params} из {total_params}** "
        "(остальные прибором не покрыты)",
        f"* плечи: {' против '.join(PROFILES)}, парно по параметрам",
        f"* бутстрэп: {BOOTSTRAP_ITERS} итераций, сид {BOOTSTRAP_SEED}",
        "",
        "## Находимость по языку оси и плечу",
        "",
        "| язык | плечо | попало | доля |",
        "|---|---|---|---|",
    ]
    for lang in LANGS:
        for profile in PROFILES:
            subset = [c for c in cells if c.lang == lang and c.profile == profile]
            if not subset:
                continue
            hits = sum(1 for c in subset if c.hit)
            lines.append(f"| {lang} | {profile} | {hits}/{len(subset)} | {hits / len(subset):.4f} |")
    lines += ["", "## Парные сравнения (одна ось меняется, состав параметров тот же)", ""]
    if not paired:
        lines.append("нет пар — сравнение невозможно")
    for item in paired:
        where = (
            f"`query_lang={item['fixed']}`"
            if item["axis"] == "profile"
            else f"плечо `{item['fixed']}`"
        )
        lines += [
            f"### {item['left']} → {item['right']} при {where}",
            "",
            f"* пар: {item['n']}",
            f"* {item['left']}: {item['left_rate']:.4f}, {item['right']}: {item['right_rate']:.4f}",
            f"* дельта: {_fmt(float(item['delta']))}, 95 % CI "
            f"[{_fmt(float(item['ci'][0]))}; {_fmt(float(item['ci'][1]))}]",
            f"* дискордансы: оба {item['both']}, только {item['left']} {item['only_left']}, "
            f"только {item['right']} {item['only_right']}, ни один {item['neither']}",
            f"* точный МакНемар: p = {float(item['p']):.4g}",
            "",
        ]
    lines += [
        "## Границы этого замера",
        "",
        "* метрика — **находимость якоря**, а не вердикт: она отвечает «достал ли поиск",
        "  материал параметра», а не «отвечает ли документ на его вопрос»;",
        f"* покрыто {anchor_params} параметров из {total_params}: у остальных нет латинского",
        "  идентификатора **или** его якорь общий с другим параметром каталога, и переводить",
        "  их вопросы на китайский прибор не умеет;",
        "* якорь проверяет **адресность**, а не ответ: найденный чанк содержит токен",
        "  параметра, но утверждать, что он отвечает на вопрос, прибор не вправе;",
        "* язык `zh` в оси не существует — прибор мерит только те настройки, которые можно",
        "  объявить в прогоне (`ru`/`en`/`all`);",
    ]
    if shared:
        lines += [
            "",
            "## Почему якорь обязан быть уникальным",
            "",
            "Замер 22.11.2026 (первая версия прибора, «любой» якорь): попадание **47/68",
            "при всех трёх настройках языка** — `ru`, `en` и `all` дали одно число, потому",
            "что общий токен (`PostgreSQL` у 6 параметров, `SQL` у 5, `WAL` у 4) находится в",
            "любом чанке документации. Метрика мерила «страница вообще про СУБД», а не",
            "«достали материал этого параметра», и ось языка на ней не различалась. Поэтому",
            "считается только токен, которого нет ни у одного другого параметра.",
            "",
            "Самые общие якоря каталога (сколько параметров их делялят): "
            + ", ".join(f"`{token}` {count}" for token, count in shared[:10]),
        ]
    if skipped:
        lines += ["", "Параметры без уникального якоря (первые 20): "
                  + ", ".join(f"`{p}`" for p in skipped[:20])]
    return "\n".join(lines) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Замер: достаёт ли объявленный ретривал китайский корпус",
    )
    parser.add_argument("--run-id", required=True, help="идентификатор прогона")
    parser.add_argument("--runs-dir", default=None, help="каталог прогонов (из раскладки)")
    parser.add_argument("--product", required=True, help="продукт-партиция (язык корпуса)")
    parser.add_argument("--catalog", required=True, help="catalog.csv пакета")
    parser.add_argument("--top-k", type=int, default=10, help="cutoff ретривала")
    parser.add_argument(
        "--allow-legacy-schema",
        action="store_true",
        help="читать БД прогона другой schema_version (только чтение, без вердиктов)",
    )
    parser.add_argument("--out", default=None, help="файл markdown-отчёта")
    parser.add_argument("--json", action="store_true", help="печатать итог JSON-ом")
    args = parser.parse_args(argv)

    paths = resolve_paths()
    runs_dir = Path(args.runs_dir) if args.runs_dir else Path(paths.runs)
    db_path = runs_dir / args.run_id / "cohound.db"
    if not db_path.exists():
        print(f"нет БД прогона: {db_path}", file=sys.stderr)
        return 2

    corpus = CorpusReader(db_path, allow_foreign_schema=args.allow_legacy_schema)
    ref = corpus.run_ref()
    deploy = load_deploy_config(None)
    rows = load_catalog(Path(args.catalog))
    unique, df = anchor_census(rows)
    with_anchor = [row for row in rows if unique.get(row.param_id)]
    without = [row.param_id for row in rows if not unique.get(row.param_id)]
    if not with_anchor:
        print("ни у одного параметра нет уникального якоря — замер невозможен", file=sys.stderr)
        return 2
    shared = sorted(
        ((token, count) for token, count in df.items() if count > 1),
        key=lambda item: (-item[1], item[0]),
    )
    print(f"параметров с уникальным якорем: {len(with_anchor)} из {len(rows)}")

    # Плечи собираются по отдельности: цена R0 не должна включать загрузку весов.
    lexical = build_retrieval_ports(
        index_root=paths.index_store,
        package=ref.package_id,
        store=corpus,
        build_id=ref.index_build_id,
        deploy=deploy,
        dense=False,
    )
    if lexical.lexical_index is None:
        print("лексический индекс не собрался — замер невозможен", file=sys.stderr)
        return 2
    dense = build_retrieval_ports(
        index_root=paths.index_store,
        package=ref.package_id,
        store=corpus,
        build_id=ref.index_build_id,
        deploy=deploy,
        dense=True,
    )
    if dense.vector_index is None or dense.embedder is None:
        print("dense-плечо не собралось (нет объявленной модели) — R2 невозможен", file=sys.stderr)
        return 2
    for notice in (*lexical.notices, *dense.notices):
        print(f"note: {notice}")

    cells: list[CellResult] = []
    for index, row in enumerate(with_anchor, start=1):
        anchors = unique[row.param_id]
        for lang in LANGS:
            for profile in PROFILES:
                cells.append(
                    _search(
                        param=row,
                        lang=lang,
                        profile=profile,
                        anchors=anchors,
                        product=args.product,
                        top_k=args.top_k,
                        lexical_index=lexical.lexical_index,
                        vector_index=dense.vector_index,
                        embedder=dense.embedder,
                    )
                )
        if index % 10 == 0:
            print(f"  {index}/{len(with_anchor)}", file=sys.stderr)

    # Две оси сравниваются раздельно и обе публикуются: «какое плечо лучше» и «какой язык
    # лучше» — разные вопросы, и ответ на один ничего не говорит о другом.
    paired = [
        p
        for lang in LANGS
        if (p := _paired(cells, axis="profile", fixed=lang, left="R0", right="R2"))
    ]
    paired += [
        p
        for profile in PROFILES
        if (p := _paired(cells, axis="lang", fixed=profile, left="ru", right="en"))
    ]
    report = _report(
        cells=cells,
        paired=paired,
        run_id=ref.run_id,
        product=args.product,
        top_k=args.top_k,
        anchor_params=len(with_anchor),
        total_params=len(rows),
        skipped=without,
        shared=shared,
    )
    if args.out:
        Path(args.out).write_text(report, encoding="utf-8")
    if args.json:
        print(
            json.dumps(
                {
                    "run_id": ref.run_id,
                    "product": args.product,
                    "top_k": args.top_k,
                    "anchor_params": len(with_anchor),
                    "total_params": len(rows),
                    "paired": paired,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    else:
        print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
