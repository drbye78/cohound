"""Поиск по корпусу прогона: слияние выдачи нескольких запросов и ответ разведки.

Роль — **разведка**, а не evidence (A3/I18). Здесь нет ни статуса ячейки, ни записи в
БД, ни проверки цитаты: результат читает человек и не попадает ни в ``cell_evidence``,
ни в отчёт, ни в ``run_id``. Поэтому модуль и живёт отдельно от ``evaluate``: тот
обязан отвечать вердиктом и подтверждать его цитатой (I1/I22), а здесь ответ на вопрос
«что вообще есть в корпусе по этому понятию» — материал для решения, не решение.

Два правила, которые нельзя переизобрести на стороне команды:

* **слияние запросов — ``max`` по счёту.** В Ш6 кросс-запросное слияние объявлено
  осью ``retrieval_params_json.cross_query_fusion`` (замер: ``per_query`` ≡ ``max`` на
  132 ячейках, план 26 O13), поэтому поиск по трём запросам одного параметра обязан
  сливать так же — иначе он показывал бы не то, что увидит оценщик;
* **ответ разведки отделён от вердикта по типу.** :class:`SearchAnswer` не совпадает с
  ``LLMCellOutput`` ни одним обязательным полем: его нельзя случайно передать туда, где
  ждут вердикт, и наоборот.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from pydantic import BaseModel, ConfigDict, Field

from core.ports.vector_index import RetrievedChunk

__all__ = [
    "ANSWER_TEMPLATE",
    "SIDE_COHORT",
    "SIDE_REFERENCE",
    "Hit",
    "SearchAnswer",
    "build_answer_prompt",
    "merge_hits",
    "render_hits",
]

#: Стороны выдачи: корпус когорты (tantivy) и reference-корпус предка (BM25).
#:
#: Счета двух движков **не сливаются**: это разные шкалы, и упорядочивание по ним
#: описывало бы не релевантность, а разницу нормировок. Поэтому сторона — поле
#: попадания, а слияние и сортировка идут только внутри своей стороны.
SIDE_COHORT = "cohort"
SIDE_REFERENCE = "reference"


@dataclass(frozen=True)
class Hit:
    """Попадание после слияния: чанк, счёт и **какие запросы** его вернули.

    ``queries`` — индексы запросов в порядке вызова. Поле нужно для ``--explain``:
    «нашлось по третьему термину» и «нашлось по всем трём» — разные факты о терминах
    каталога, и именно второй отличает настоящее совпадение понятия от случайного слова.

    ``side`` — из какого корпуса фрагмент (:data:`SIDE_COHORT`/``SIDE_REFERENCE``).
    Счёт имеет смысл только внутри стороны: лексическое плечо когорты считает tantivy,
    reference-корпус — ``Bm25Index``, и сравнивать их числа между собой нельзя.
    """

    chunk_id: str
    product_id: str
    text: str
    heading_path: str
    context_prefix: str
    content_hash: str
    build_id: str
    score: float
    queries: tuple[int, ...] = ()
    side: str = SIDE_COHORT

    def with_score(self, score: float, query: int) -> Hit:
        """Тот же чанк с большим счётом и добавленным номером запроса."""
        return Hit(
            chunk_id=self.chunk_id,
            product_id=self.product_id,
            text=self.text,
            heading_path=self.heading_path,
            context_prefix=self.context_prefix,
            content_hash=self.content_hash,
            build_id=self.build_id,
            score=max(self.score, score),
            queries=(*self.queries, query),
            side=self.side,
        )


def merge_hits(
    per_query: Sequence[Sequence[RetrievedChunk]], *, side: str = SIDE_COHORT
) -> list[Hit]:
    """Сливает выдачу нескольких запросов по правилу ``max`` (то же, что в Ш6).

    Порядок — по убыванию счёта, tie-break по ``chunk_id``: выдача обязана быть
    воспроизводимой (A3), иначе два прогона одной команды дают разные списки.

    ``side`` помечает попадания стороной и **не** меняет правило слияния: счета разных
    движков сравнимы только внутри стороны, поэтому межстороннее упорядочивание здесь
    не делается вовсе (его не существует и в выдаче).
    """
    merged: dict[str, Hit] = {}
    for index, chunks in enumerate(per_query):
        for chunk in chunks:
            existing = merged.get(chunk.chunk_id)
            hit = Hit(
                chunk_id=chunk.chunk_id,
                product_id=chunk.product_id,
                text=chunk.text,
                heading_path=chunk.heading_path,
                context_prefix=chunk.context_prefix,
                content_hash=chunk.content_hash,
                build_id=chunk.build_id,
                score=chunk.score,
                queries=(index,),
                side=side,
            )
            merged[chunk.chunk_id] = (
                hit if existing is None else existing.with_score(chunk.score, index)
            )
    return sorted(merged.values(), key=lambda hit: (-hit.score, hit.chunk_id))


class SearchAnswer(BaseModel):
    """Ответ разведки по фрагментам. **Не вердикт**: полей статуса здесь нет.

    ``insufficient`` — честный выход «в приведённых фрагментах ответа нет». Без него
    модель обязана что-то ответить, и «чего нет в корпусе» превращается в правдоподобный
    текст — ровно тот дефект, который A3 запрещает.
    """

    model_config = ConfigDict(extra="forbid")

    #: Ответ по существу вопроса, только из фрагментов.
    answer: str = Field(min_length=1)
    #: Номера фрагментов (1-based), на которых держится ответ.
    citations: list[int] = Field(default_factory=list)
    #: ``True`` — во фрагментах ответа нет; ``answer`` тогда объясняет, чего не хватает.
    insufficient: bool = False


#: Шаблон ответа разведки. Отдельно от ``evaluator_v*.txt`` намеренно: это другой
#: контракт (свободный ответ по фрагментам), и подмешивать его в шаблон оценщика
#: значило бы менять ``prompt_hash`` вердикта ради инструмента разведки.
ANSWER_TEMPLATE = """Ты помогаешь разведке. Ответь на вопрос ТОЛЬКО по приведённым фрагментам.

Правила:
- не додумывай и не пользуйся внешними знаниями о продуктах;
- если во фрагментах ответа нет — поставь "insufficient": true и напиши, чего не хватает;
- в "citations" перечисли номера фрагментов, на которых держится ответ;
- отличай заявление вендора от упоминания: цитируй формулировку, а не пересказ.

Вопрос: {question}

Фрагменты:
{fragments}
"""


def build_answer_prompt(
    question: str, hits: Sequence[Hit], *, max_chars: int = 1200
) -> str:
    """Собирает промпт разведки: вопрос + нумерованные фрагменты с адресами.

    ``max_chars`` — граница **на фрагмент**, а не на промпт: длинный кусок обрезается
    с явной меткой, чтобы модель не принимала обрыв за конец мысли. Обрезка названа в
    тексте, а не сделана молча (A3).
    """
    if not hits:
        raise ValueError("промпт разведки без фрагментов не строится: отвечать не по чему")
    blocks: list[str] = []
    for number, hit in enumerate(hits, start=1):
        text = " ".join(hit.text.split())
        if len(text) > max_chars:
            text = text[:max_chars] + " …[обрезано]"
        heading = f" | раздел: {hit.heading_path}" if hit.heading_path else ""
        blocks.append(
            f"[{number}] продукт: {hit.product_id}{heading} | чанк: {hit.chunk_id}\n{text}"
        )
    return ANSWER_TEMPLATE.format(question=question, fragments="\n\n".join(blocks))


def render_hits(
    hits: Sequence[Hit],
    *,
    urls: Mapping[str, str] | None = None,
    width: int = 320,
) -> str:
    """Текстовая выдача: счёт, продукт, раздел, адрес и начало текста.

    ``urls`` — карта ``chunk_id → url`` (из хранилища): URL в ``RetrievedChunk`` нет
    намеренно (порт отдаёт то, что нужно вердикту), а человеку без адреса выдача
    бесполезна — по ней нечего открыть.
    """
    lines: list[str] = []
    for rank, hit in enumerate(hits, start=1):
        text = " ".join(hit.text.split())
        if len(text) > width:
            text = text[:width] + "…"
        heading = f" § {hit.heading_path}" if hit.heading_path else ""
        lines.append(f"{rank:2d}. [{hit.score:7.4f}] {hit.product_id}{heading}")
        url = (urls or {}).get(hit.chunk_id, "")
        if url:
            lines.append(f"    {url}")
        lines.append(f"    {hit.chunk_id}")
        lines.append(f"    {text}")
    return "\n".join(lines)
