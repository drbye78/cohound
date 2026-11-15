"""Токенный ограничитель чанка — надстройка над структурной нарезкой (O21 плана 26).

**Это не алгоритм нарезки, а ограничитель поверх неё.** Границы чанков по-прежнему задаёт
структура (заголовок markdown, абзац plain text): единица смысла — она, а токенный лимит
лишь говорит, что кусок длиннее бюджета надо поделить ещё раз, по возможности по тем же
структурным границам внутри него. Обратное (нарезать «по N токенов») потеряло бы и
заголовок, и абзац — то есть ровно то, ради чего чанкер существует.

**Зачем.** Бюджет чанкера задан в **символах**, а токенов на символ приходится разное
количество: замер 04.11.2026 на стенде показал **39.888 %** чанков plain-text чанкера
длиннее 512 токенов (`docs/reports/measurements/2026-11-04-text-chunker-exact-slices.md`),
при том что markdown-семейство укладывается в 5.18 %. Символьный бюджет этого не видит:
он один для всех алфавитов, а «токенов на символ» зависит от языка. Ограничитель вводит
величину, которую видно, и делает её **объявленным условием** (``TOKEN_LIMIT`` в
:mod:`core.adapters.chunker_family`), а не настройкой по умолчанию.

**Счётчик — объявленный вход, не догадка.** Токены считает :class:`HfTokenCounter` по
токенизатору объявленной модели (``model_id@revision``); символьный прокси не
подставляется никогда (:class:`TokenizerUnavailable` вместо подмены — тот же приём, что
``EmbedderUnavailable`` в :mod:`core.adapters.embed_local`). Единица — токены
**содержимого** (без служебных): модель добавляет ``<s>``/``</s>`` сама, и в бюджет чанка
они не входят.

**Что ограничитель обязан сохранить.**

* **P1** — склейка текстов кусков равна склейке текстов входа: ограничитель режет, а не
  переписывает. Разделитель (в том числе ``"\\n\\n"``) остаётся в предыдущем куске.
* **Точный срез** — каждый кусок остаётся срезом документа: ``char_start``/``char_end``
  пересчитываются от родителя, ``doc.text[char_start:char_end] == text``. Если у чанка
  смещений нет, резать его нечем — он остаётся целым и **видимо** попадает в
  ``LimitReport.unsplittable`` (честный отказ, а не выдуманная позиция, A3).
* **Детерминизм** — два прогона на одном входе дают один выход (A3).
* **Идемпотентность в пределах бюджета** — ``apply(apply(x))`` не меняет состав: кусок,
  уложившийся в лимит, второй раз не режется. Единственное исключение — **приклеенный
  хвост**: он по построению длиннее бюджета, и второй проход вправе его поделить. P1 и
  точные срезы держатся и в этом случае (это проверяется тестом), меняется только состав.

**Где ограничитель сознательно не строг.** Хвост короче :data:`MIN_TAIL_TOKENS` токенов
приклеивается к предыдущему куску: обрывок в 10 токенов — не цитата, а мусор в индексе.
Цена названа: такой кусок может превысить бюджет меньше чем на порог, и он **виден** в
``LimitReport.over_limit``. Это тот же объявленный компромисс, что ``min_chunk_size`` у
markdown-чанкера (там короткий хвост тоже приклеивается, а не выбрасывается).

Нейтрален (§1.3): ни продуктов, ни вендоров; идентификатор модели приходит данными.
"""

from __future__ import annotations

import bisect
import hashlib
import importlib.util
import re
from dataclasses import dataclass
from typing import Any

from core.ports.tokenizer import TokenCounterPort
from core.ports.types import Chunk
from core.util.text_utils import hard_cut

__all__ = [
    "MIN_TAIL_TOKENS",
    "TOKEN_LIMIT_FLOOR_SHARE",
    "TOKEN_LIMIT_POLICY",
    "HfTokenCounter",
    "LimitReport",
    "TokenLimiter",
    "TokenizerUnavailable",
]

#: Версия правил ограничителя. Входит в версию чанкера
#: (:func:`core.adapters.chunker_family.composite_chunker_version`), потому что меняет
#: состав чанков — то есть является ``execution_change``, а не косметикой.
TOKEN_LIMIT_POLICY = "token-limit-1"  # noqa: S105 - версия правил, не секрет

#: Порог хвоста (токены): остаток короче него приклеивается к предыдущему куску.
#: Порог — объявленная политика, а не магия: обрывок такого размера не несёт смысла
#: (ни фразы, ни строки таблицы), но занимает место в индексе и в промпте оценщика.
MIN_TAIL_TOKENS = 64

#: Нижняя граница окна поиска структурной границы — доля бюджета. Симметрично ``floor``
#: у ``chunker_markdown._pieces``: граница, стоящая ближе половины бюджета к началу,
#: кандидатом не считается — иначе кусок выродился бы в огрызок, а «экономия» обернулась
#: бы лишним чанком.
TOKEN_LIMIT_FLOOR_SHARE = 0.5

#: Конец абзаца: пустая строка (в «пустых» строках plain text часто остаются пробелы).
_PARAGRAPH_SEP_RE = re.compile(r"\n\s*\n")

#: Конец предложения: терминатор (западный и полноширинный — в CJK пробела после него
#: нет), необязательные закрывающие знаки и пробелы. Режем **после** найденного.
_SENTENCE_SEP_RE = re.compile(r"[.!?;。！？;][\"'»”’)\]]*\s*")

#: Пробельный символ — последняя по предпочтению граница.
_WHITESPACE_RE = re.compile(r"\s")


class TokenizerUnavailable(RuntimeError):
    """Счётчик токенов недоступен: нет зависимости либо не загружается токенизатор.

    Отказ **видимый** и без подмены. Символьный прокси («≈4 символа на токен») запрещён:
    он мерил бы другую величину под тем же именем и объявил бы в манифесте ограничение,
    которого не было (A3). Сеть в момент прогона не используется (``local_files_only``):
    отсутствие локального снапшота — это отсутствие условия, а не повод сходить за ним
    из ядра.
    """


def _sha256_16(text: str) -> str:
    """Локальный ``content_hash`` куска — как у обоих чанкеров (публичного хелпера нет)."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


@dataclass(frozen=True)
class LimitReport:
    """Счётчики видимости одного применения ограничителя (на документ).

    Не «статистика для красоты»: без них включение ограничителя меняло бы корпус молча.
    ``over_limit`` — единственный честный показатель того, что лимит **не** соблюдён
    (приклеенный хвост), поэтому он рядом с ``max_tokens_after``, а не спрятан в логах.
    """

    #: Сколько чанков пришло на вход.
    chunks_in: int = 0
    #: Сколько кусков вышло.
    chunks_out: int = 0
    #: Сколько входных чанков было поделено (дали больше одного куска).
    split_chunks: int = 0
    #: Сколько раз короткий хвост был приклеен к предыдущему куску.
    absorbed_tails: int = 0
    #: Сколько чанков не резалось: у них нет ``char_start``/``char_end`` (нет точного среза).
    unsplittable: int = 0
    #: Сколько выходных кусков превышает лимит **после** приклейки хвоста.
    over_limit: int = 0
    #: Наибольшее число токенов среди выходных кусков.
    max_tokens_after: int = 0


class HfTokenCounter:
    """Счётчик токенов на токенизаторе объявленной модели (``transformers``, лениво).

    :param model_id: идентификатор модели токенизатора (HF-repo) — **данные**, не категория.
    :param revision: пин ревизии (git-sha/тег). Входит в :attr:`spec` как ``repo@<sha>``:
        иначе ограничитель называл бы «какую-то» версию правил разбиения на подслова, а
        не фактические веса (та же конвенция, что ``embed_spec``).
    :param local_files_only: не ходить в сеть за токенизатором (дефолт ``True``): снапшот
        обязан быть в локальном кэше, иначе загрузка **видимо** падает. Ядро не тянет
        веса из интернета в момент прогона; оператор, которому нужен первый скачок,
        передаёт ``False`` явно — тогда это его объявленное действие.
    """

    def __init__(
        self,
        model_id: str,
        revision: str | None = None,
        local_files_only: bool = True,
    ) -> None:
        if not model_id:
            raise ValueError("model_id обязан быть непустым")
        self.model_id = model_id
        self.revision = revision
        self.local_files_only = local_files_only
        self._tokenizer: Any = None

    @property
    def spec(self) -> str:
        """``model_id@revision`` (либо ``model_id``, если ревизия не объявлена)."""
        return f"{self.model_id}@{self.revision}" if self.revision else self.model_id

    @staticmethod
    def available() -> bool:
        """Установлен ли ``transformers`` (без импорта тяжёлого пакета)."""
        try:
            return importlib.util.find_spec("transformers") is not None
        except (ImportError, ValueError):  # pragma: no cover - сломанный __spec__
            return False

    def token_ends(self, text: str) -> tuple[int, ...]:
        """Концы токенов **содержимого** в символах ``text`` (пустые отрезки отброшены).

        :raises TokenizerUnavailable: нет ``transformers``, не загрузился токенизатор или
            он не отдаёт карту смещений.
        """
        if not text:
            return ()
        tokenizer = self._load()
        try:
            encoded = tokenizer(
                text,
                # Служебные токены модель добавит сама при вызове; бюджет чанка — про
                # токены **текста**, иначе два счётчика одной величины расходились бы на
                # постоянную добавку на каждый чанк.
                add_special_tokens=False,
                # Без усечения: усечённый вход дал бы концы, не покрывающие текст, и
                # ограничитель «не увидел» бы хвост.
                truncation=False,
                return_offsets_mapping=True,
            )
            offsets = encoded["offset_mapping"]
        except Exception as exc:  # noqa: BLE001 - любая ошибка токенизатора = отказ
            raise TokenizerUnavailable(
                f"токенизатор {self.spec!r} не отдал карту смещений: "
                f"{type(exc).__name__}: {exc}. Символьный прокси не подставляется: "
                "он мерил бы другую величину (A3)."
            ) from exc
        return tuple(int(end) for start, end in offsets if int(end) > int(start))

    # ------------------------------------------------------------------
    # Внутреннее
    # ------------------------------------------------------------------

    def _load(self) -> Any:
        """Лениво загружает и кэширует токенизатор; недоступность — видимый отказ."""
        if self._tokenizer is not None:
            return self._tokenizer
        try:
            from transformers import AutoTokenizer
        except ImportError as exc:
            raise TokenizerUnavailable(
                "transformers не установлен: счётчик токенов недоступен. Установите "
                "extra, дающий transformers, или объявите ограничитель выключенным "
                "(TOKEN_LIMIT = None). Символьный прокси не подставляется (A3)."
            ) from exc
        try:
            self._tokenizer = AutoTokenizer.from_pretrained(
                self.model_id,
                revision=self.revision,
                # Сеть запрещена: снапшот обязан быть локальным, иначе это отказ, а не
                # тихий поход в интернет из ядра прогона.
                local_files_only=self.local_files_only,
            )
        except Exception as exc:  # noqa: BLE001 - любая ошибка загрузки = отказ
            raise TokenizerUnavailable(
                f"токенизатор {self.spec!r} не загружен из локального кэша "
                f"(local_files_only=True): {type(exc).__name__}: {exc}. Сеть в прогоне "
                "запрещена, символьный прокси не подставляется (A3)."
            ) from exc
        return self._tokenizer


class TokenLimiter:
    """Ограничитель: режет слишком длинные чанки, хвосты приклеивает к предыдущему куску.

    :param counter: счётчик токенов — объявленный вход (см. :class:`TokenCounterPort`).
    :param limit: бюджет куска в токенах. **Условие измерения** (гранулярность), а не
        потолок безопасности: значение объявляется вызывающим кодом.
    :param min_tail_tokens: хвост короче порога приклеивается к предыдущему куску.
    :param floor_share: нижняя граница окна поиска структурной границы, доля бюджета.
    """

    def __init__(
        self,
        counter: TokenCounterPort,
        limit: int,
        min_tail_tokens: int = MIN_TAIL_TOKENS,
        floor_share: float = TOKEN_LIMIT_FLOOR_SHARE,
    ) -> None:
        if limit <= 0:
            raise ValueError(f"limit обязан быть положительным, получено {limit!r}")
        if not 0 < floor_share < 1:
            raise ValueError(
                f"floor_share обязан быть в (0, 1), получено {floor_share!r}: "
                "окно поиска границы не может быть пустым или шире бюджета"
            )
        if min_tail_tokens < 0:
            raise ValueError(
                f"min_tail_tokens обязан быть >= 0, получено {min_tail_tokens!r}"
            )
        self._counter = counter
        self._limit = limit
        self._min_tail_tokens = min_tail_tokens
        self._floor_share = floor_share

    @property
    def limit(self) -> int:
        """Объявленный бюджет куска в токенах."""
        return self._limit

    def apply(self, chunks: list[Chunk]) -> tuple[list[Chunk], LimitReport]:
        """Делит чанки одного документа по лимиту; возвращает куски и отчёт.

        ``chunk_no``/``chunk_id`` нумеруются заново сквозным образом: после деления
        родительский номер перестаёт быть уникальным, а ``chunk_id`` обязан остаться
        уникальным внутри документа (он — адрес куска в корпусе).
        """
        out: list[Chunk] = []
        split_chunks = 0
        absorbed_tails = 0
        unsplittable = 0
        over_limit = 0
        max_tokens_after = 0
        no = 0

        for chunk in chunks:
            # Родительский ``chunk_id`` — ``{document_id}#{no}``; префикс переживает
            # деление, чтобы адреса кусков остались в том же документе.
            prefix = chunk.chunk_id.rsplit("#", 1)[0]
            if chunk.char_start is None or chunk.char_end is None:
                # Точного среза нет — резать нечем: выдуманная позиция сломала бы I22
                # (цитату нельзя проверить по документу). Отказ виден в отчёте.
                unsplittable += 1
                pieces = [(chunk.text, chunk.char_start, chunk.char_end)]
                counts = [len(self._counter.token_ends(chunk.text))]
                if counts[0] > self._limit:
                    # Чанк сверх лимита, но точного среза нет — резать нечем. Считаем
                    # честно: это тоже кусок сверх лимита, а не «всё хорошо».
                    over_limit += 1
            else:
                spans, counts, absorbed, over = self._split(chunk.text)
                absorbed_tails += absorbed
                over_limit += over
                if len(spans) > 1:
                    split_chunks += 1
                pieces = [
                    (chunk.text[start:end], chunk.char_start + start, chunk.char_start + end)
                    for start, end in spans
                ]

            for piece_text, char_start, char_end in pieces:
                out.append(
                    Chunk(
                        chunk_id=f"{prefix}#{no}",
                        chunk_no=no,
                        text=piece_text,
                        # Контекст продолжения наследуется: шапка таблицы и путь заголовка
                        # действуют и для продолжения — ровно за этим ``context_prefix``
                        # и существует.
                        heading_path=chunk.heading_path,
                        context_prefix=chunk.context_prefix,
                        content_hash=_sha256_16(piece_text),
                        char_start=char_start,
                        char_end=char_end,
                    )
                )
                no += 1

            max_tokens_after = max(max_tokens_after, max(counts, default=0))

        report = LimitReport(
            chunks_in=len(chunks),
            chunks_out=len(out),
            split_chunks=split_chunks,
            absorbed_tails=absorbed_tails,
            unsplittable=unsplittable,
            over_limit=over_limit,
            max_tokens_after=max_tokens_after,
        )
        return out, report

    # ------------------------------------------------------------------
    # Внутреннее
    # ------------------------------------------------------------------

    def _split(self, text: str) -> tuple[list[tuple[int, int]], list[int], int, int]:
        """Куски текста как ``(начало, конец)`` + число токенов куска, хвосты, переборы.

        Возвращает ``(спаны, счётчики токенов, приклеенных хвостов, кусков сверх лимита)``.
        Спаны покрывают текст без дыр и наложений, поэтому склейка кусков равна входу (P1).
        """
        limit = self._limit
        spans: list[tuple[int, int]] = []
        counts: list[int] = []
        absorbed = 0
        over = 0
        length = len(text)
        position = 0

        while True:
            ends = self._counter.token_ends(text[position:])
            if len(ends) <= limit:
                spans.append((position, length))
                counts.append(len(ends))
                break
            cut = self._cut(text, position, ends)
            if cut is None:
                # Структурной границы в окне нет вовсе (сплошной текст без пробелов) —
                # жёсткий разрез по концу ``limit``-го токена, не рвущий графемный кластер.
                cut = hard_cut(text, position, position + ends[limit - 1])
            if cut <= position:
                # Прогресс обязателен: пустой кусок зациклил бы ограничитель.
                cut = position + 1
            cut = min(cut, length)
            piece = text[position:cut]
            piece_ends = self._counter.token_ends(piece)
            # Гарантия лимита: граница, найденная по символам, может дать лишний токен —
            # подслово на стыке считается иначе, чем внутри целого текста. Отодвигаем рез
            # к предыдущему концу токена, пока кусок не уложится (шагов обычно 0–2).
            index = bisect.bisect_right(ends, cut - position) - 1
            while len(piece_ends) > limit and index >= 0:
                cut = position + ends[index]
                piece = text[position:cut]
                piece_ends = self._counter.token_ends(piece)
                index -= 1

            remaining = len(ends) - len(piece_ends)
            if remaining < self._min_tail_tokens:
                # Хвост-обрывок приклеивается: кусок в 10 токенов — не цитата. Цена —
                # возможный перебор бюджета, и он виден в ``over_limit``.
                end = length
                count = len(self._counter.token_ends(text[position:]))
                absorbed += 1
            else:
                end = cut
                count = len(piece_ends)
            spans.append((position, end))
            counts.append(count)
            if count > limit:
                over += 1
            if end >= length:
                break
            position = end

        return spans, counts, absorbed, over

    def _cut(self, text: str, position: int, ends: tuple[int, ...]) -> int | None:
        """Абсолютная позиция реза по структурной границе окна, либо ``None``.

        Окно — от ``floor_share`` бюджета до конца ``limit``-го токена. Предпочтение
        границ: абзац → предложение → пробел; ``None`` означает «границ нет», и тогда
        вызывающий режет жёстко (:func:`core.util.text_utils.hard_cut`). Рез всегда
        **после** найденного разделителя и всегда правее ``position``.
        """
        limit_char = ends[self._limit - 1]
        floor_index = max(0, int(self._limit * self._floor_share) - 1)
        window_start = ends[floor_index]
        window = text[position + window_start : position + limit_char]

        for pattern in (_PARAGRAPH_SEP_RE, _SENTENCE_SEP_RE):
            last = None
            for match in pattern.finditer(window):
                last = match
            if last is not None and last.end() > 0:
                return position + window_start + last.end()

        whitespace = None
        for match in _WHITESPACE_RE.finditer(window):
            whitespace = match
        if whitespace is not None:
            return position + window_start + whitespace.end()

        return None
