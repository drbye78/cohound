"""Замер скорости и памяти реранкера (O23 плана 26): пары из реального корпуса.

Инструмент отвечает на вопрос «сколько стоит кросс-энкодер в пути поиска», а не «стало ли
лучше»: качество (recall@K/nDCG против эталона) — следующий шаг O23 и другой прибор. Здесь
только цена: пары в секунду, распределение времени на пару, пик RSS процесса и пик VRAM.

**Вход — сохранённый прогон, а не синтетика.** Запросы берутся из ``plan_inputs.json``
снимка прогона (``params[].title``), документы — из ``corpus_chunks`` объявленного продукта
равномерным шагом по ``chunk_id``: первые чанки корпуса смещены по длине (начало документа),
и выборка «первые 500» описывала бы не корпус, а его начало. Файл открывается **сырым
``sqlite3 mode=ro``**: гейт схемы отказывает прогону старой версии, а прибор читает данные,
и прочитанную версию схемы публикует в отчёте.

**Метрики** (определения зафиксированы, иначе число невоспроизводимо):

``pairs_per_s``
    Пар (запрос, документ) в секунду **после прогрева** (прогрев = ровно один батч, в замер
    не входит): ``пар / сумма времени батчей``. Батчи идут подряд, без пауз.
``ms_per_pair``
    Время батча, делённое на число пар в нём, — p50 / p95 / max **по батчам** (и среднее для
    справки). Это цена пары *в батче*: одиночная пара на CPU стоит иначе, и смешивать эти
    две величины в одном числе нельзя. По батчам, а не по среднему, потому что разброс между
    батчами — то, что видно в проде.
``peak_rss_bytes``
    Пик RSS **процесса** в окне замера (монитор ``/proc/self/status``, шаг 20 мс), а не
    ``ru_maxrss``: последний монотонен по процессу и включал бы загрузку всех моделей.
``rss_after_load_bytes``
    RSS сразу после загрузки модели, до прогрева: цена весов без буферов инференса.
``peak_vram_bytes``
    ``torch.cuda.max_memory_allocated()`` за вариант устройства, включая веса: пик сбрасывается
    **перед** загрузкой модели.
``max_pairs``
    Число пар в замере (объявленный размер набора; батч — отдельная ось).
``load_average``
    ``os.getloadavg()`` в окне ячейки — **общесистемная** величина, и на CPU-ячейке она
    включает сам замер (torch занимает свои потоки). Поэтому это не мера конкуренции, а
    сырой признак: сравнивать её между ячейками можно, читать «сколько было чужих» — нельзя.
    Для cuda она ближе к делу: там потоки заняты подготовкой батчей, а не счётом.

**Точность — объявленная точка, не «как получится»:** ``cuda`` — ``fp16``, ``cpu`` — ``fp32``.
Квантование здесь не меряется: сначала цена в объявленной точности, INT8/ONNX — отдельный
вопрос с отдельной проверкой.

**Режим питания — объявленный вход, а не вывод из признаков.** Скорость на этом хосте
зависит от питания (USB-C против комплектного БП), а программно отличить одно от другого
нельзя: ``power.limit`` мобильной карты отдаётся как ``[N/A]``, ``cpufreq`` в WSL2 не
проброшен, ``BAT1 power_now`` = 0. Поэтому режим **объявляет оператор** (``--power-mode``) и
он попадает в отчёт **как есть**: прибор не нормализует значение и не угадывает режим по
признакам. Наблюдённое (``ac_online``, ``battery_status``, частота/потребление/температура
карты, счётчик ``SW Power Capping`` в микросекундах) публикуется рядом как **контроль**.
Счётчик троттлинга публикуется **парой «до/после»** на каждую ячейку и не сворачивается в
разницу: разница теряет факт «до тоже был не ноль». Окно питания ячейки включает загрузку
весов (там же набирается троттлинг), окна соседних ячеек стыкуются. ``unknown`` (дефолт)
печатается предупреждением: отсутствие факта должно быть видно.

**Длина входа — две точки, а не одна.** ``--max-lengths`` задаёт пределы (по умолчанию 512 и
2048 токенов): «быстро» на коротких не означает «быстро» на длинных. Документ перед подачей
обрезается по символам так, чтобы совместная длина пары не превышала предел (двоичный поиск
по префиксу, см. :func:`clip_pair`), а токенизатор модели режет ещё раз как жёсткий потолок.

**Предел, который ничего не режет, ничего и не стоит.** Корпусные чанки пилота — ≈280 токенов
(максимум 1528 символов), поэтому ``max_length=2048`` на них не обрезает ни одной пары: две
«разные длины» описывают один и тот же вход, и разница между ними — это цена объявленного
потолка, а не длинного текста. Чтобы ось длины действительно работала, есть
``--long-docs N --long-target-tokens T``: документы склеиваются из соседних корпусных чанков
(текст остаётся корпусным, синтетична только граница документа) и набираются до бюджета в
**токенах этой модели**. Ячейки несут ``doc_set`` — ``corpus`` или ``long:N@T``, — поэтому
ячейка на склеенных документах не выдаёт себя за корпусную.

**Языки.** Корпус пилота — RU; EN-материал есть в чанках (идентификаторы, команды), ZH —
нет. Поэтому в набор добавляются короткие **синтетические** ZH-пары (``--zh-pairs``, по
умолчанию 3) и это называется в отчёте: «поддерживает ZH» иначе остаётся заявлением карточки
модели, а не наблюдением. Отчёт несёт и факт проверки корпуса на CJK
(:func:`core.util.cjk_lexical.is_cjk_char`), и счета ZH-пар (конечность, не «нет падения»).

**Чего прибор НЕ измеряет:** качество (recall@K/nDCG), VRAM при ``max_length`` 8192 (если не
влезает — это названо, а не экстраполировано), влияние реранка на вердикты.

Нейтрален (§1.3): идентификаторы моделей и продукта — данные, категорий продуктов нет.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sqlite3
import subprocess
import sys
import threading
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

#: Кандидаты O23, проходящие требование «RU + EN + ZH» (проверено по карточкам HF).
DEFAULT_MODELS = (
    "BAAI/bge-reranker-v2-m3",
    "Qwen/Qwen3-Reranker-0.6B",
    "mixedbread-ai/mxbai-rerank-base-v2",
)

#: Синтетические ZH-пары: в пилотном корпусе CJK-чанков нет, и «ZH поддержан» надо
#: показать наблюдением. Текст — технический и обезличенный (нейтрален по §1.3).
SYNTHETIC_ZH_PAIRS: tuple[tuple[str, str], ...] = (
    (
        "如何配置数据库的自动备份？",
        "自动备份的周期在配置文件里声明，保存位置和保留份数也在同一个区块里设置。",
    ),
    (
        "什么是逻辑复制？",
        "逻辑复制把数据变更以行级事件发送给订阅者，订阅者按照主键应用这些变更。",
    ),
    (
        "怎样查看慢查询日志？",
        "慢查询日志记录执行时间超过阈值的语句，阈值由配置项控制，日志路径同样可以配置。",
    ),
)


#: Объявленный режим питания по умолчанию: «не объявлен» — видимый факт, а не догадка.
DEFAULT_POWER_MODE = "unknown"
#: Прокси-признаки питания, наблюдаемые на этом хосте (WSL2, мобильная карта).
AC_ONLINE_PATH = Path("/sys/class/power_supply/AC1/online")
BATTERY_STATUS_PATH = Path("/sys/class/power_supply/BAT1/status")
NVIDIA_SMI = "nvidia-smi"
#: Поля снимка карты: ``None`` — не измерено (не «ноль»).
GPU_FIELDS: tuple[str, ...] = (
    "sm_clock_mhz",
    "power_draw_w",
    "temperature_c",
    "sw_power_cap_us",
)


# ---------------------------------------------------------------------------
# Режим питания: объявленный вход и наблюдённые признаки
# ---------------------------------------------------------------------------


def _read_text(path: Path) -> str | None:
    """Содержимое sysfs-файла; ``None`` — файла нет или он не читается."""
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError:
        return None


def _ac_online() -> bool | None:
    """``AC1/online``: питание от сети. ``None`` — файла нет или значение не 0/1."""
    raw = _read_text(AC_ONLINE_PATH)
    if raw in {"0", "1"}:
        return raw == "1"
    return None


def _battery_status() -> str | None:
    """``BAT1/status`` как есть (``Charging``/``Discharging``/…); ``None`` — не читается."""
    return _read_text(BATTERY_STATUS_PATH) or None


def _as_int(raw: str) -> int | None:
    """Целое из поля вывода; ``[N/A]``/пусто/мусор — ``None``, а не ``0``."""
    try:
        return int(float(raw))
    except (TypeError, ValueError):
        return None


def _as_float(raw: str) -> float | None:
    """Дробное из поля вывода; ``[N/A]``/пусто/мусор — ``None``, а не ``0.0``."""
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _nvidia_smi(args: Sequence[str], *, timeout_s: float = 5.0) -> str | None:
    """Вывод ``nvidia-smi``; ``None`` — команды нет, карты нет, отказ или таймаут."""
    try:
        completed = subprocess.run(
            [NVIDIA_SMI, *args],
            check=False,
            text=True,
            capture_output=True,
            timeout=timeout_s,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if completed.returncode != 0:
        return None
    return completed.stdout


def _parse_gpu_metrics(text: str) -> dict[str, Any]:
    """Строка ``--query-gpu=clocks.sm,power.draw,temperature.gpu`` → поля снимка."""
    metrics: dict[str, Any] = dict.fromkeys(GPU_FIELDS)
    line = next((item for item in text.splitlines() if item.strip()), "")
    parts = [part.strip() for part in line.split(",")]
    if len(parts) != 3:
        return metrics
    metrics["sm_clock_mhz"] = _as_int(parts[0])
    metrics["power_draw_w"] = _as_float(parts[1])
    metrics["temperature_c"] = _as_int(parts[2])
    return metrics


def _parse_sw_power_cap_us(text: str) -> int | None:
    """Счётчик ``SW Power Capping`` из ``nvidia-smi -q -d PERFORMANCE``, микросекунды.

    Берётся **счётчик**, а не флаг ``SW Power Cap: Active``: флаг говорит «ограничена
    сейчас», счётчик — «за время замера карта была ограничена по питанию столько-то».
    """
    for line in text.splitlines():
        label, sep, value = line.partition(":")
        if not sep or not label.strip().lower().startswith("sw power cap"):
            continue
        value = value.strip()
        if not value.endswith("us"):
            continue  # «Active»/«Not Active» — флаг, а не счётчик
        return _as_int(value[: -len("us")].strip())
    return None


def _power_snapshot(*, with_gpu: bool) -> dict[str, Any]:
    """Прокси-признаки питания на момент вызова: **контроль**, а не источник режима.

    ``with_gpu`` включается только объявленным ``cuda``: при ``cpu`` карта не опрашивается
    вовсе, и поля остаются ``None`` (не измерено), а не нулями.
    """
    snapshot: dict[str, Any] = {
        "ac_online": _ac_online(),
        "battery_status": _battery_status(),
    }
    if not with_gpu:
        snapshot.update(dict.fromkeys(GPU_FIELDS))
        return snapshot
    metrics = _nvidia_smi(
        ["--query-gpu=clocks.sm,power.draw,temperature.gpu", "--format=csv,noheader,nounits"]
    )
    snapshot.update(_parse_gpu_metrics(metrics) if metrics is not None else dict.fromkeys(GPU_FIELDS))
    counters = _nvidia_smi(["-q", "-d", "PERFORMANCE"])
    snapshot["sw_power_cap_us"] = (
        _parse_sw_power_cap_us(counters) if counters is not None else None
    )
    return snapshot


def _declared_power_mode(args: argparse.Namespace) -> str:
    """Объявленный режим питания — **только** из аргумента оператора.

    Отдельная функция ради проверяемости: режим нельзя вывести из наблюдённых признаков,
    потому что USB-C и комплектный БП на этом хосте неразличимы (``power.limit`` мобильной
    карты — ``[N/A]``, ``cpufreq`` в WSL2 не проброшен, ``BAT1 power_now`` = 0), а
    «угаданный» режим в отчёте — это ложь об условии замера.
    """
    return str(args.power_mode)


def _power_window(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    """Окно ячейки: снимки «до» и «после» рядом, **без производных**.

    Разница ``after − before`` намеренно не публикуется: она теряет факт «до тоже был не
    ноль», то есть что карта была ограничена по питанию ещё до замера.
    """
    return {"before": dict(before), "after": dict(after)}


# ---------------------------------------------------------------------------
# Память процесса
# ---------------------------------------------------------------------------


def _rss_bytes() -> int:
    """Текущий RSS процесса из ``/proc/self/status`` (без psutil)."""
    try:
        with open("/proc/self/status", encoding="ascii") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) * 1024
    except OSError:
        return 0
    return 0


class RssMonitor:
    """Пик RSS за окно замера: поток опрашивает процесс, а не монотонный ``ru_maxrss``."""

    def __init__(self, interval_s: float = 0.02) -> None:
        self._interval = interval_s
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._peak = 0

    def _loop(self) -> None:
        while not self._stop.is_set():
            self._peak = max(self._peak, _rss_bytes())
            self._stop.wait(self._interval)

    def start(self) -> None:
        self._peak = _rss_bytes()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> int:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=1.0)
        self._peak = max(self._peak, _rss_bytes())
        return self._peak


# ---------------------------------------------------------------------------
# Метрики
# ---------------------------------------------------------------------------


def percentile(values: Sequence[float], q: float) -> float:
    """Перцентиль линейной интерполяцией (как ``numpy.percentile``), без numpy.

    Пустой ряд — ``ValueError``, а не ноль: ноль читался бы как измеренное значение.
    ``q`` — доля в ``[0, 1]``.
    """
    if not values:
        raise ValueError("перцентиль пустого ряда не определён")
    ordered = sorted(float(v) for v in values)
    pos = (len(ordered) - 1) * q
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return float(ordered[lo])
    return float(ordered[lo] + (ordered[hi] - ordered[lo]) * (pos - lo))


def latency_stats(per_pair_ms: Sequence[float]) -> dict[str, float]:
    """``p50``/``p95``/``max``/``mean`` по времени на пару (мс)."""
    if not per_pair_ms:
        raise ValueError("нет замеров: статистика не определена")
    return {
        "p50": round(percentile(per_pair_ms, 0.5), 3),
        "p95": round(percentile(per_pair_ms, 0.95), 3),
        "max": round(max(per_pair_ms), 3),
        "mean": round(sum(per_pair_ms) / len(per_pair_ms), 3),
    }


# ---------------------------------------------------------------------------
# Вход: запросы и документы
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DocSample:
    """Детерминированный срез ``corpus_chunks``: адреса и тексты."""

    product_id: str
    rows_total: int
    chunk_ids: list[str]
    texts: list[str]

    @property
    def n(self) -> int:
        return len(self.texts)

    def describe(self) -> dict[str, Any]:
        lengths = [len(t) for t in self.texts]
        return {
            "product_id": self.product_id,
            "rows_total": self.rows_total,
            "chunks": self.n,
            "mean_chars": round(sum(lengths) / len(lengths), 1) if lengths else 0.0,
            "max_chars": max(lengths) if lengths else 0,
            "stride": round(self.rows_total / self.n, 3) if self.n else 0.0,
            "first_chunk_id": self.chunk_ids[0] if self.chunk_ids else None,
            "last_chunk_id": self.chunk_ids[-1] if self.chunk_ids else None,
        }


def load_documents(db: Path, product_id: str, sample: int) -> DocSample:
    """Каждый ``stride``-й чанк продукта в порядке ``chunk_id`` (не «первые N»).

    ``random`` не используется: повторный запуск на той же БД обязан дать тот же срез,
    иначе числа скорости не сравнить между запусками и между моделями.
    """
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        total = int(
            conn.execute(
                "SELECT count(*) FROM corpus_chunks WHERE product_id = ?", (product_id,)
            ).fetchone()[0]
        )
        if total == 0:
            raise SystemExit(f"corpus_chunks пуст для product_id={product_id!r}")
        n = min(sample, total)
        offsets = sorted({min(total - 1, (i * total) // n) for i in range(n)})
        marks = ",".join("?" for _ in offsets)
        sql = (
            "WITH ordered AS ("  # noqa: S608 — подставляются только «?», значения bind'ом
            "  SELECT chunk_id, text, row_number() OVER (ORDER BY chunk_id) - 1 AS rn"
            "  FROM corpus_chunks WHERE product_id = ?"
            ") "
            f"SELECT chunk_id, text FROM ordered WHERE rn IN ({marks}) ORDER BY rn"
        )
        rows = conn.execute(sql, (product_id, *offsets)).fetchall()
    finally:
        conn.close()
    return DocSample(
        product_id=product_id,
        rows_total=total,
        chunk_ids=[str(r[0]) for r in rows],
        texts=[str(r[1]) for r in rows],
    )


def load_queries(plan_inputs: Path) -> list[str]:
    """Запросы из снимка каталога прогона: ``params[].title`` в объявленном порядке.

    Дубли снимаются (два параметра с одним заголовком — один запрос), пустые пропускаются.
    """
    if not plan_inputs.exists():
        raise SystemExit(f"снимок каталога не найден: {plan_inputs}")
    payload = json.loads(plan_inputs.read_text(encoding="utf-8"))
    params = payload.get("params") if isinstance(payload, dict) else None
    if not isinstance(params, list):
        raise SystemExit(f"{plan_inputs}: нет списка params")
    out: list[str] = []
    seen: set[str] = set()
    for item in params:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or "").strip()
        if title and title not in seen:
            seen.add(title)
            out.append(title)
    if not out:
        raise SystemExit(f"{plan_inputs}: пустой список заголовков параметров")
    return out


def build_pairs(
    queries: Sequence[str],
    documents: Sequence[str],
    *,
    zh_pairs: Sequence[tuple[str, str]] = (),
    limit: int | None = None,
) -> list[tuple[str, str]]:
    """Детерминированные пары ``(запрос, документ)``.

    Запрос циклится по документам (``queries[i % len(queries)]``): документов в срезе
    больше, чем запросов, и каждый запрос обязан встретиться. Синтетические ZH-пары идут
    **первыми**, чтобы ``limit`` не выбросил их из короткой пробы. ``limit`` — жёсткий
    потолок числа пар (``None`` — без потолка).
    """
    if not queries:
        raise ValueError("пустой список запросов")
    if not documents:
        raise ValueError("пустой список документов")
    pairs: list[tuple[str, str]] = list(zh_pairs)
    pairs.extend((queries[i % len(queries)], doc) for i, doc in enumerate(documents))
    if limit is not None:
        if limit < 1:
            raise ValueError("limit обязан быть >= 1")
        pairs = pairs[:limit]
    return pairs


def build_long_documents(
    documents: Sequence[str],
    tokenizer: Any,
    query: str,
    *,
    count: int,
    target_tokens: int,
) -> list[str]:
    """Длинные документы из **реального** текста корпуса: склейка соседних чанков.

    Зачем это нужно. Пилотный корпус состоит из коротких чанков (среднее ≈280 токенов при
    пределе 1528 символов), поэтому ``max_length=2048`` не обрезает **ничего**: ось длины
    на таком материале не проверяется, и «быстро на 2048» означало бы то же, что «быстро на
    512». Склейка идёт подряд по ``chunk_id`` (текст остаётся корпусным, синтетической
    является только граница документа) до объявленного ``target_tokens``.

    Порядок детерминирован: документ ``i`` начинается с чанка ``i`` и набирает следующие по
    кругу. ``count`` больше числа чанков не даёт дублей сверх круга — набор всё равно
    определён.
    """
    if target_tokens < 1:
        raise ValueError("target_tokens обязан быть >= 1")
    if not documents:
        raise ValueError("пустой список документов")
    n = len(documents)
    out: list[str] = []
    for i in range(max(0, count)):
        parts: list[str] = []
        total = 0
        j = i
        while total < target_tokens and len(parts) < n:
            parts.append(documents[j % n])
            total = _token_count(tokenizer, query, "\n\n".join(parts))
            j += 1
        out.append("\n\n".join(parts))
    return out


# ---------------------------------------------------------------------------
# Обрезка по объявленному пределу
# ---------------------------------------------------------------------------


def _token_count(tokenizer: Any, query: str, doc: str) -> int:
    """Совместная длина пары в токенах (с спец-токенами, без обрезки)."""
    encoded = tokenizer(query, doc, add_special_tokens=True, truncation=False)
    ids = encoded["input_ids"]
    # Медленный токенизатор на паре возвращает плоский список; быстрый — тоже для одной пары.
    return len(ids) if not ids or isinstance(ids[0], int) else len(ids[0])


def clip_pair(
    pair: tuple[str, str], tokenizer: Any, max_length: int
) -> tuple[str, str, int, int]:
    """Обрезает документ по символам до бюджета ``max_length`` токенов.

    Возвращает ``(запрос, документ, символов_до, символов_после)``. Документ режется
    **префиксом** (тот же край, что у токенизаторной обрезки справа), граница ищется
    двоичным поиском по длине префикса: предикат «пара влезает» монотонен по длине с
    точностью до границ BPE-токенов, поэтому найденный префикс гарантированно влезает,
    а оставшийся хвост добивает токенизатор модели как жёсткий потолок.
    """
    query, doc = pair
    before = len(doc)
    if _token_count(tokenizer, query, doc) <= max_length:
        return query, doc, before, before
    if _token_count(tokenizer, query, "") > max_length:
        # Сам запрос длиннее предела: документ не добавляем, запрос режет токенизатор.
        return query, "", before, 0
    lo, hi = 0, before
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if _token_count(tokenizer, query, doc[:mid]) <= max_length:
            lo = mid
        else:
            hi = mid - 1
    return query, doc[:lo], before, lo


def clip_pairs(
    pairs: Sequence[tuple[str, str]], tokenizer: Any, max_length: int
) -> tuple[list[tuple[str, str]], dict[str, Any]]:
    """Обрезает все пары и возвращает статистику обрезки (доказательство, что она была)."""
    clipped: list[tuple[str, str]] = []
    chars_before: list[int] = []
    chars_after: list[int] = []
    tokens_before: list[int] = []
    tokens_after: list[int] = []
    n_clipped = 0
    # ``CrossEncoder`` объявляет ``model_max_length = max_length``, и токенизатор тогда
    # предупреждает на каждом длинном тексте. Обрезаем мы сами, поэтому на время обрезки
    # предупреждение снимается; после — прежнее значение возвращается (иначе потолок
    # токенизатора внутри ``predict`` перестал бы действовать).
    previous_max = getattr(tokenizer, "model_max_length", None)
    if previous_max is not None:
        tokenizer.model_max_length = 10**9
    try:
        for pair in pairs:
            query, doc, before, after = clip_pair(pair, tokenizer, max_length)
            clipped.append((query, doc))
            chars_before.append(before)
            chars_after.append(after)
            tokens_before.append(_token_count(tokenizer, pair[0], pair[1]))
            tokens_after.append(_token_count(tokenizer, query, doc))
            if after < before:
                n_clipped += 1
    finally:
        if previous_max is not None:
            tokenizer.model_max_length = previous_max
    stats = {
        "max_length": max_length,
        "pairs": len(clipped),
        "clipped_pairs": n_clipped,
        "doc_chars_mean_before": round(sum(chars_before) / len(chars_before), 1)
        if chars_before
        else 0.0,
        "doc_chars_mean_after": round(sum(chars_after) / len(chars_after), 1)
        if chars_after
        else 0.0,
        "doc_chars_max_after": max(chars_after) if chars_after else 0,
        "pair_tokens_mean_before": round(sum(tokens_before) / len(tokens_before), 1)
        if tokens_before
        else 0.0,
        "pair_tokens_mean_after": round(sum(tokens_after) / len(tokens_after), 1)
        if tokens_after
        else 0.0,
        "pair_tokens_max_after": max(tokens_after) if tokens_after else 0,
    }
    return clipped, stats


# ---------------------------------------------------------------------------
# Модель и замер
# ---------------------------------------------------------------------------


def _hub_cache_dir() -> Path:
    """Корень HF-кэша: ``HF_HUB_CACHE`` (его и держит библиотека), иначе дефолт."""
    try:
        from huggingface_hub.constants import HF_HUB_CACHE

        return Path(HF_HUB_CACHE)
    except Exception:  # noqa: BLE001 - версия без константы: берём объявленный дефолт
        return Path(os.environ.get("HF_HOME", str(Path.home() / ".cache" / "huggingface"))) / "hub"


def resolve_revision(model_id: str) -> str | None:
    """Ревизия (``sha``) из локального HF-кэша: имя каталога снапшота и есть ревизия.

    Сеть не трогается: пин берётся из того, что реально лежит на диске, — иначе отчёт
    назвал бы ревизию, которой в кэше нет. ``refs/main`` читается, если есть (кэш,
    скачанный без явной ревизии); иначе берётся самый свежий снапшот — загрузка с пином
    ``refs/main`` не пишет, потому что пина нет.
    """
    root = _hub_cache_dir() / f"models--{model_id.replace('/', '--')}"
    snapshots = root / "snapshots"
    if not snapshots.is_dir():
        return None
    ref = root / "refs" / "main"
    if ref.is_file():
        pinned = ref.read_text(encoding="utf-8").strip()
        if pinned and (snapshots / pinned).is_dir():
            return pinned
    dirs = sorted((d for d in snapshots.iterdir() if d.is_dir()), key=lambda p: p.stat().st_mtime)
    return dirs[-1].name if dirs else None


def load_reranker(model_id: str, *, revision: str | None, device: str, max_length: int) -> Any:
    """Загружает ``CrossEncoder`` из локального кэша в объявленной точности устройства.

    Путь инференса один для всех трёх кандидатов: ``sentence_transformers.CrossEncoder``
    (у decoder-only моделей скор берётся из логитов «yes»/«no» модулем ``LogitScore``,
    у ``bge-reranker-v2-m3`` — из логита классификатора). ``trust_remote_code`` не
    включается: ни одна карточка не требует своего кода.
    """
    from sentence_transformers import CrossEncoder

    model_kwargs: dict[str, Any] = {}
    if device == "cuda":
        import torch

        model_kwargs["dtype"] = torch.float16
    return CrossEncoder(
        model_id,
        revision=revision,
        device=device,
        max_length=max_length,
        local_files_only=True,
        trust_remote_code=False,
        model_kwargs=model_kwargs or None,
    )


def measure_cell(
    model: Any,
    pairs: Sequence[tuple[str, str]],
    *,
    batch_size: int,
    device: str,
    zh_count: int,
) -> dict[str, Any]:
    """Один замер: прогрев одним батчем, затем батчи подряд с временем каждого.

    Прогрев в числа не входит (ленивая инициализация ядер и буферов — не цена пары).
    """
    import numpy as np
    import torch

    warm = list(pairs[:batch_size])
    model.predict(warm, batch_size=len(warm), show_progress_bar=False, convert_to_numpy=True)
    if device == "cuda":
        torch.cuda.synchronize()

    per_pair_ms: list[float] = []
    scores: list[float] = []
    monitor = RssMonitor()
    monitor.start()
    if device == "cuda":
        torch.cuda.synchronize()
    wall0 = time.perf_counter()
    for start in range(0, len(pairs), batch_size):
        batch = list(pairs[start : start + batch_size])
        if not batch:
            break
        batch0 = time.perf_counter()
        out = model.predict(
            batch, batch_size=len(batch), show_progress_bar=False, convert_to_numpy=True
        )
        if device == "cuda":
            torch.cuda.synchronize()
        wall = time.perf_counter() - batch0
        per_pair_ms.append(wall * 1000.0 / len(batch))
        scores.extend(float(x) for x in np.asarray(out).reshape(-1))
    total_wall = time.perf_counter() - wall0
    peak_rss = monitor.stop()

    stats = latency_stats(per_pair_ms)
    zh_scores = scores[:zh_count] if zh_count else []
    return {
        "pairs": len(pairs),
        "batch_size": batch_size,
        "pairs_per_s": round(len(pairs) / total_wall, 4) if total_wall > 0 else 0.0,
        "wall_s": round(total_wall, 3),
        "ms_per_pair": stats,
        "batches": len(per_pair_ms),
        "peak_rss_bytes": peak_rss,
        # Нагрузка хоста в окне замера: CPU-числа на общем хосте без неё — слух.
        "load_average": [round(v, 2) for v in os.getloadavg()],
        "zh_scores": [round(s, 6) for s in zh_scores],
        "zh_scores_finite": all(math.isfinite(s) for s in zh_scores),
        "scores_finite": all(math.isfinite(s) for s in scores),
    }


def _release_cuda() -> None:
    """Освобождает VRAM между моделями (иначе пик следующей включает мусор прошлой)."""
    import gc

    import torch

    gc.collect()
    if torch.cuda.is_available():
        torch.cuda.empty_cache()


def _is_oom(exc: BaseException) -> bool:
    """Отказ по памяти: ``OutOfMemoryError`` cuda или ``MemoryError`` хоста."""
    if isinstance(exc, MemoryError):
        return True
    return "out of memory" in str(exc).lower()


def vram_oversubscribed(peak_vram_bytes: int | None, total_vram_bytes: int | None) -> bool | None:
    """Пик аллокатора больше объёма карты — признак вытеснения в память хоста.

    Драйвер (в WSL2 — наблюдалось 16.11.2026) может **не** бросить ``OutOfMemoryError``, а
    отдать больше, чем есть на карте: тогда ячейка формально измерена, но её ``pairs_per_s``
    описывает пейджинг, а не инференс. Флаг публикуется, чтобы это не читалось как «влезло».
    ``None`` — если одной из сторон нет (cpu, неизвестный объём карты): отсутствие факта не
    факт отсутствия.
    """
    if peak_vram_bytes is None or not total_vram_bytes:
        return None
    return peak_vram_bytes > total_vram_bytes


@dataclass(frozen=True)
class PairSource:
    """Объявленный набор пар: запросы, документы и правило их сборки.

    Сборка отложена до загрузки модели, потому что длинные документы набираются **по
    токенизатору этой модели** (пределы 512/2048 заданы в её токенах, а не в символах).
    """

    queries: list[str]
    documents: list[str]
    zh_pairs: list[tuple[str, str]]
    limit: int | None = None
    long_count: int = 0
    long_target_tokens: int = 2000

    def build(self, tokenizer: Any) -> list[tuple[str, str]]:
        docs: Sequence[str] = self.documents
        if self.long_count:
            docs = build_long_documents(
                self.documents,
                tokenizer,
                self.queries[0],
                count=self.long_count,
                target_tokens=self.long_target_tokens,
            )
        return build_pairs(self.queries, docs, zh_pairs=self.zh_pairs, limit=self.limit)

    @property
    def doc_set(self) -> str:
        return f"long:{self.long_count}@{self.long_target_tokens}" if self.long_count else "corpus"


def run_cell(
    model_id: str,
    *,
    revision: str | None,
    device: str,
    max_length: int,
    batch_size: int,
    source: PairSource,
    power_before: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Загрузка + обрезка + замер одного ``(модель, устройство, длина, батч)``.

    ``source.doc_set`` — происхождение документов (``corpus`` или ``long:N@T``), публикуется
    вместе с числом: ячейка на склеенных документах описывает другую длину входа.

    Окно питания — **вся ячейка**, включая загрузку весов: счётчик ``SW Power Capping``
    набирается и на загрузке тоже, и вырезать её из окна значило бы не видеть троттлинг
    первой (самой тяжёлой по памяти) фазы. ``power_before`` передаётся предыдущей ячейкой,
    чтобы окна стыковались, а не перекрывались снимком «на момент старта».
    """
    import torch

    base: dict[str, Any] = {
        "model": model_id,
        "revision": revision,
        "device": device,
        "dtype": "fp16" if device == "cuda" else "fp32",
        "max_length": max_length,
        "batch_size": batch_size,
        "doc_set": source.doc_set,
    }
    before = power_before if power_before is not None else _power_snapshot(with_gpu=device == "cuda")
    if device == "cuda":
        torch.cuda.empty_cache()
        torch.cuda.reset_peak_memory_stats()
    model = None
    result: dict[str, Any]
    try:
        model = load_reranker(model_id, revision=revision, device=device, max_length=max_length)
        rss_after_load = _rss_bytes()
        pairs = source.build(model.tokenizer)
        zh_count = min(len(source.zh_pairs), len(pairs))
        clipped, clip_stats = clip_pairs(pairs, model.tokenizer, max_length)
        cell = measure_cell(
            model, clipped, batch_size=batch_size, device=device, zh_count=zh_count
        )
        cell.update(base)
        cell["rss_after_load_bytes"] = rss_after_load
        cell["clip"] = clip_stats
        cell["peak_vram_bytes"] = (
            int(torch.cuda.max_memory_allocated()) if device == "cuda" else None
        )
        cell["peak_vram_reserved_bytes"] = (
            int(torch.cuda.max_memory_reserved()) if device == "cuda" else None
        )
        cell["vram_oversubscribed"] = vram_oversubscribed(
            cell["peak_vram_bytes"],
            int(torch.cuda.get_device_properties(0).total_memory)
            if device == "cuda"
            else None,
        )
        result = cell
    except Exception as exc:  # noqa: BLE001 - отказ публикуется, а не глотается
        entry = dict(base)
        entry["error"] = f"{type(exc).__name__}: {exc}"
        entry["error_kind"] = "out_of_memory" if _is_oom(exc) else "other"
        print(
            f"[rerank] {model_id} {device} L={max_length} B={batch_size}: ОТКАЗ — "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
            flush=True,
        )
        result = entry
    finally:
        del model
        if device == "cuda":
            _release_cuda()
    # «после» снимается после освобождения VRAM: окно закрывается и на отказе, иначе
    # счётчик троттлинга у упавшей ячейки остался бы без пары.
    result["power"] = _power_window(before, _power_snapshot(with_gpu=device == "cuda"))
    return result


# ---------------------------------------------------------------------------
# Сбор условий
# ---------------------------------------------------------------------------


def count_cjk_chunks(db: Path, product_id: str, scan_limit: int | None = None) -> dict[str, Any]:
    """Сколько чанков продукта содержат CJK (факт корпуса, а не догадка о нём).

    ``scan_limit`` ограничивает число просмотренных чанков (``None`` — все).
    """
    from core.util.cjk_lexical import is_cjk_char

    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            "SELECT chunk_id, text FROM corpus_chunks WHERE product_id = ?", (product_id,)
        )
        scanned = 0
        hits: list[str] = []
        for chunk_id, text in rows:
            if scan_limit is not None and scanned >= scan_limit:
                break
            scanned += 1
            if any(is_cjk_char(ch) for ch in str(text)):
                hits.append(str(chunk_id))
    finally:
        conn.close()
    return {"scanned": scanned, "cjk_chunks": len(hits), "examples": hits[:5]}


def schema_version(db: Path) -> str | None:
    """Версия схемы файла прогона — публикуется вместе с числами."""
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        row = conn.execute("SELECT value FROM schema_meta WHERE key = 'schema_version'").fetchone()
    except sqlite3.Error:
        return None
    finally:
        conn.close()
    return str(row[0]) if row else None


def run_id_of(db: Path) -> str | None:
    """``run_id`` из таблицы ``runs``: числа относятся к корпусу этого прогона."""
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        row = conn.execute("SELECT run_id FROM runs LIMIT 1").fetchone()
    except sqlite3.Error:
        return None
    finally:
        conn.close()
    return str(row[0]) if row else None


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_csv(raw: str) -> list[str]:
    return [part.strip() for part in raw.split(",") if part.strip()]


def _parse_ints(raw: str) -> list[int]:
    return [int(part) for part in _parse_csv(raw)]


def _measured(value: Any, unit: str = "", *, digits: int | None = None) -> str:
    """Значение с единицей; ``None`` печатается как «не измерено», а не как ноль."""
    if value is None:
        return "не измерено"
    text = f"{value:.{digits}f}" if digits is not None else str(value)
    return f"{text}{unit}"


def _tri_state(value: bool | None) -> str:
    """Тройное состояние признака: ``None`` — не измерено, а не «нет»."""
    if value is None:
        return "не измерено"
    return "да" if value else "нет"


def _counter_pair(before: Any, after: Any) -> str:
    """Счётчик троттлинга парой «до→после»: разница потеряла бы «до тоже был не ноль»."""
    if before is None and after is None:
        return "SW Power Capping не измерено"
    left = "не измерено" if before is None else str(before)
    right = "не измерено" if after is None else str(after)
    return f"SW Power Capping {left}→{right} us"


def _power_line(report: dict[str, Any]) -> str:
    """Режим питания и признаки одной строкой: объявленное и наблюдённое — раздельно."""
    mode = str(report.get("power_mode") or DEFAULT_POWER_MODE)
    declared = "не объявлен" if mode == DEFAULT_POWER_MODE else mode
    cells = [cell for cell in report.get("cells", []) if "power" in cell]
    before = (cells[0]["power"]["before"] if cells else report.get("power_start")) or {}
    after = (cells[-1]["power"]["after"] if cells else {}) or {}
    parts = [
        f"питание: режим {declared}",
        f"AC online {_tri_state(before.get('ac_online'))}",
        f"батарея {before.get('battery_status') or 'не измерено'}",
        f"SM {_measured(before.get('sm_clock_mhz'), ' МГц')}",
        f"потребление {_measured(before.get('power_draw_w'), ' Вт', digits=1)}",
        f"температура {_measured(before.get('temperature_c'), ' °C')}",
        _counter_pair(before.get("sw_power_cap_us"), after.get("sw_power_cap_us")),
    ]
    return " · ".join(parts)


def _print_power_conditions(report: dict[str, Any]) -> None:
    """Режим питания и признаки; при ``unknown`` — предупреждение.

    Молчать при необъявленном режиме нельзя: числа скорости тогда не привязаны к условию
    питания, и сравнивать их между прогонами (USB-C против комплектного БП) не с чем.
    """
    print(_power_line(report), file=sys.stderr)
    if str(report.get("power_mode") or DEFAULT_POWER_MODE) == DEFAULT_POWER_MODE:
        print(
            "ВНИМАНИЕ: режим питания не объявлен (--power-mode unknown) — числа скорости "
            "не привязаны к режиму питания и между прогонами не сравниваются.",
            file=sys.stderr,
        )


def _print_table(report: dict[str, Any]) -> None:
    """Человеческая таблица в stderr; stdout остаётся машиночитаемым JSON."""
    out = sys.stderr
    sample = report["sample"]
    print(
        f"\nвыборка: product={sample['product_id']} чанков={sample['chunks']} "
        f"из {sample['rows_total']} (шаг {sample['stride']}), "
        f"средняя длина {sample['mean_chars']} символов; "
        f"запросов {report['queries']['count']}; ZH-пар {report['zh']['pairs']} "
        f"({report['zh']['source']})\n",
        file=out,
    )
    header = (
        f"{'модель':>36} {'устр':>5} {'L':>5} {'B':>3} {'пар/с':>9} "
        f"{'p50 мс':>8} {'p95 мс':>8} {'max мс':>8} {'RSS MiB':>8} {'VRAM MiB':>9} "
        f"{'SW cap до→после':>20}"
    )
    print(header, file=out)
    for cell in report["cells"]:
        if "error" in cell:
            print(
                f"{cell['model']:>36} {cell['device']:>5} {cell['max_length']:>5} "
                f"{cell['batch_size']:>3} ОТКАЗ ({cell.get('error_kind')}): {cell['error']}",
                file=out,
            )
            continue
        lat = cell["ms_per_pair"]
        vram = cell["peak_vram_bytes"]
        marker = "!" if cell.get("vram_oversubscribed") else ""
        power = cell.get("power") or {}
        cap = _counter_pair(
            (power.get("before") or {}).get("sw_power_cap_us"),
            (power.get("after") or {}).get("sw_power_cap_us"),
        ).removeprefix("SW Power Capping ").removesuffix(" us")
        print(
            f"{cell['model']:>36} {cell['device']:>5} {cell['max_length']:>5} "
            f"{cell['batch_size']:>3} {cell['pairs_per_s']:>9.3f} "
            f"{lat['p50']:>8.2f} {lat['p95']:>8.2f} {lat['max']:>8.2f} "
            f"{cell['peak_rss_bytes'] / 2**20:>8.0f} "
            f"{('—' if vram is None else f'{vram / 2**20:.0f}{marker}'):>9} {cap:>20}",
            file=out,
        )
    # Пометка `!` — не сноска: ячейка с пиком больше объёма карты не описывает инференс,
    # и это должно быть видно в самой строке, а не выводиться читателем из VRAM.
    over = [cell for cell in report["cells"] if cell.get("vram_oversubscribed")]
    for cell in over:
        print(
            f"! {cell['model']} {cell['device']} L={cell['max_length']} B={cell['batch_size']}: "
            f"НЕ ВЛЕЗЛА; отказ драйвера не пришёл — пик аллокатора "
            f"{cell['peak_vram_bytes'] / 2**20:.0f} MiB против карты "
            f"{(report.get('cuda_total_vram_bytes') or 0) / 2**20:.0f} MiB. Число пар/с "
            "описывает пейджинг, а не инференс; в сравнения пропускной способности не входит.",
            file=out,
        )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Замер скорости и памяти реранкера на парах из сохранённого прогона"
    )
    parser.add_argument("--db", type=Path, required=True, help="cohound.db прогона (только чтение)")
    parser.add_argument("--product", required=True, help="product_id из corpus_chunks")
    parser.add_argument(
        "--plan-inputs",
        type=Path,
        default=None,
        help="plan_inputs.json прогона (по умолчанию рядом с БД): запросы = params[].title",
    )
    parser.add_argument("--pairs", type=int, default=500, help="пар в наборе")
    parser.add_argument(
        "--limit", type=int, default=None, help="жёсткий потолок числа пар (короткая проба)"
    )
    parser.add_argument("--max-lengths", default="512,2048", help="пределы входа в токенах")
    parser.add_argument("--batches", default="8,32", help="размеры батча через запятую")
    parser.add_argument("--devices", default="cpu,cuda", help="устройства через запятую")
    parser.add_argument(
        "--models", default=",".join(DEFAULT_MODELS), help="идентификаторы моделей через запятую"
    )
    parser.add_argument(
        "--zh-pairs",
        type=int,
        default=len(SYNTHETIC_ZH_PAIRS),
        help="сколько синтетических ZH-пар добавить (0 — не добавлять)",
    )
    parser.add_argument(
        "--long-docs",
        type=int,
        default=0,
        help="документов в наборе собрать склейкой соседних чанков (0 — корпусные чанки как есть)",
    )
    parser.add_argument(
        "--long-target-tokens",
        type=int,
        default=2000,
        help="до скольких токенов набирать склеенный документ (--long-docs > 0)",
    )
    parser.add_argument(
        "--cjk-scan",
        type=int,
        default=20000,
        help="сколько чанков просмотреть на CJK (0 — все)",
    )
    parser.add_argument(
        "--power-mode",
        default=DEFAULT_POWER_MODE,
        help=(
            "объявленный оператором режим питания (ac-barrel/ac-usbc/battery/…): попадает "
            "в отчёт как есть, прибор его не выводит из признаков"
        ),
    )
    parser.add_argument(
        "--json", type=Path, default=None, help="куда записать отчёт (JSON); stdout всегда JSON"
    )
    args = parser.parse_args(argv)

    if not args.db.exists():
        raise SystemExit(f"БД не найдена: {args.db}")
    if args.pairs < 1:
        raise SystemExit("--pairs обязан быть >= 1")
    if args.limit is not None and args.limit < 1:
        raise SystemExit("--limit обязан быть >= 1")
    max_lengths = _parse_ints(args.max_lengths)
    batches = _parse_ints(args.batches)
    devices = _parse_csv(args.devices)
    models = _parse_csv(args.models)
    if not max_lengths or not batches or not devices or not models:
        raise SystemExit("оси замера не могут быть пустыми")
    for device in devices:
        if device not in {"cpu", "cuda"}:
            raise SystemExit(f"неизвестное устройство: {device!r}")

    import torch
    from sentence_transformers import __version__ as st_version
    from transformers import __version__ as tf_version

    plan_inputs = args.plan_inputs or (args.db.parent / "plan_inputs.json")
    t_start = time.time()
    docs = load_documents(args.db, args.product, args.pairs)
    queries = load_queries(plan_inputs)
    zh_pairs = list(SYNTHETIC_ZH_PAIRS[: max(0, args.zh_pairs)])
    cjk = count_cjk_chunks(args.db, args.product, args.cjk_scan or None)
    if cjk["cjk_chunks"]:
        zh_source = "corpus"
    elif zh_pairs:
        zh_source = "synthetic"
    else:
        zh_source = "none"
    source = PairSource(
        queries=queries,
        documents=docs.texts,
        zh_pairs=zh_pairs,
        limit=args.limit,
        long_count=max(0, args.long_docs),
        long_target_tokens=args.long_target_tokens,
    )
    pairs = build_pairs(queries, docs.texts, zh_pairs=zh_pairs, limit=args.limit)
    zh_count = min(len(zh_pairs), len(pairs))

    report: dict[str, Any] = {
        "tool": "measure_reranker_speed",
        "python": sys.version.split()[0],
        "torch": torch.__version__,
        "transformers": tf_version,
        "sentence_transformers": st_version,
        "cuda_available": bool(torch.cuda.is_available()),
        "cuda_device": (
            str(torch.cuda.get_device_name(0)) if torch.cuda.is_available() else "none"
        ),
        "cuda_total_vram_bytes": (
            int(torch.cuda.get_device_properties(0).total_memory)
            if torch.cuda.is_available()
            else None
        ),
        "torch_num_threads": int(torch.get_num_threads()),
        "load_average_start": [round(v, 2) for v in os.getloadavg()],
        "power_mode": _declared_power_mode(args),
        "power_start": _power_snapshot(with_gpu=torch.cuda.is_available()),
        "db": str(args.db),
        "schema_version_file": schema_version(args.db),
        "run_id": run_id_of(args.db),
        "sample": docs.describe(),
        "queries": {"count": len(queries), "source": f"{plan_inputs.name}:params[].title"},
        "zh": {
            "source": zh_source,
            "pairs": zh_count,
            "corpus_scan": cjk,
            "synthetic_texts": [q for q, _ in SYNTHETIC_ZH_PAIRS[:zh_count]],
        },
        "conditions": {
            "dtype": {"cpu": "fp32", "cuda": "fp16"},
            "max_lengths": max_lengths,
            "batches": batches,
            "devices": devices,
            "models": models,
            "pairs_requested": args.pairs,
            "limit": args.limit,
            "doc_set": source.doc_set,
            "long_target_tokens": args.long_target_tokens,
        },
        "cells": [],
    }

    for device in devices:
        if device == "cuda" and not torch.cuda.is_available():
            report["cells"].append(
                {
                    "device": "cuda",
                    "error": "cuda недоступна в этом окружении",
                    "error_kind": "unavailable",
                }
            )
            print("[rerank] cuda недоступна — GPU-числа не получены", file=sys.stderr, flush=True)
            continue
        # Окна питания стыкуются: «после» предыдущей ячейки = «до» следующей.
        power_before = _power_snapshot(with_gpu=device == "cuda")
        for model_id in models:
            revision = resolve_revision(model_id)
            if revision is None:
                print(
                    f"[rerank] {model_id}: нет в локальном HF-кэше — ревизия не определена",
                    file=sys.stderr,
                    flush=True,
                )
            for max_length in max_lengths:
                for batch_size in batches:
                    print(
                        f"[rerank] {model_id}@{revision} {device} L={max_length} B={batch_size}: "
                        "загрузка и замер",
                        file=sys.stderr,
                        flush=True,
                    )
                    cell = run_cell(
                        model_id,
                        revision=revision,
                        device=device,
                        max_length=max_length,
                        batch_size=batch_size,
                        source=source,
                        power_before=power_before,
                    )
                    power_before = cell["power"]["after"]
                    report["cells"].append(cell)
                    if "error" not in cell:
                        print(
                            f"[rerank]   {cell['pairs_per_s']:.3f} пар/с, "
                            f"p50 {cell['ms_per_pair']['p50']:.2f} мс, "
                            f"RSS {cell['peak_rss_bytes'] / 2**20:.0f} MiB",
                            file=sys.stderr,
                            flush=True,
                        )
                    if args.json is not None:
                        # Отчёт пишется после каждого замера: долгий прогон не теряет числа.
                        args.json.write_text(
                            json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
                        )

    report["elapsed_s"] = round(time.time() - t_start, 1)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    _print_power_conditions(report)
    _print_table(report)
    if args.json is not None:
        args.json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
