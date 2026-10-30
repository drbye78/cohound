"""Таксономия сбоев краула — часть **контракта** порта, а не одной реализации (R5).

**Почему отдельным модулем, а не в ``ports/crawler.py``.** Таксономия нужна пяти
потребителям, которые сам ``CrawlerPort`` не используют: ``cli/wiring`` (сборка и
деградация), ``edge`` (launcher и фабрика), ``pipeline/crawl_probe_driver`` (различает
«движок недоступен» от «тир не сработал»), ``adapters/remote_crawl_executor`` (шов DMZ) и
потребители транспорта в тестах. Если бы классы жили рядом с протоколом, каждый такой
импорт тянул бы ``ports.types`` (``FetchPlan``/``RawPage``) — то есть форму запроса ради
имени исключения. Отдельный модуль оставляет импорт узким и однозначным:
``from core.ports.crawl_errors import CrawlError``.

**Почему это порт, а не адаптер.** Класс сбоя — то, что вызывающий обязан уметь ловить
независимо от транспорта: HTTP, Scrapy, браузер, remote-прокси DMZ. Пока классы были
объявлены в ``adapters/crawl_scrapy.py``, контракт ошибок принадлежал одной реализации:
второй транспорт импортировал первый (``crawl_http`` ← ``crawl_scrapy``), удаление или
замена scrapy-адаптера ломала ядро, а ``pipeline`` не мог назвать тип и сравнивал имя
класса строкой. Теперь транспорты импортируют таксономию **отсюда** и объявляют только
свои константы исполнения.

**Что здесь есть и чего нет.** Здесь — **только** те классы, которые пересекают границу
транспорта (ловит вызывающий). Константы исполнения (потолки, таймауты, настройки
движка, ``DEFAULT_MAX_RESPONSE_BYTES``) остаются в адаптерах: это параметры реализации.

**Инварианты таксономии** (пинятся ``tests/test_crawl_error_taxonomy.py``):

* ``CrawlUrlError`` — и ``CrawlError``, и ``ValueError`` (``security.validate_url``
  поднимает ``ValueError``; вызывающий ловит одним из двух, не зная про транспорт);
* ``CrawlRefused`` несёт ``refusal_reason`` — машинный код причины отказа по политике:
  ``robots_disallow`` (сайт запретил путь) либо ``robots_unavailable`` (ленту
  ``robots.txt`` прочитать не удалось, поэтому origin не обходится — RFC 9309 §2.3.1.4);
  это **разные** факты: во втором сайте нас не запрещал, мы не смогли установить разрешение;
* ``CrawlNotFound`` несёт ``failure_kind="not_found"`` (origin ответил, что ресурса нет);
* ``CrawlPayloadTooLarge`` несёт ``failure_kind="payload_too_large"``;
* ``CrawlUnavailable`` — один тип на все транспорты: «движок/зависимость не поднялись».
  До R5 у ``crawl_http`` и ``crawl_browser`` было по **своему** классу с этим именем,
  поэтому ``except`` одного не ловил другой, а ``pipeline`` различал их сравнением
  ``type(exc).__name__``.

``http_status`` — наблюдаемость, а не классификация (см. :class:`CrawlError`).
Нейтрален (§1.3).
"""

from __future__ import annotations

from core.domain.enums import FailureKind

__all__ = [
    "NOT_FOUND_STATUSES",
    "CrawlError",
    "CrawlNotFound",
    "CrawlPayloadTooLarge",
    "CrawlRefused",
    "CrawlUnavailable",
    "CrawlUrlError",
]

#: HTTP-статусы определённого ответа «ресурса нет» (404/410). Классификация общая для
#: всех транспортов, видящих статус ответа: HTTP, Scrapy и hybrid-путь браузера.
NOT_FOUND_STATUSES: frozenset[int] = frozenset({404, 410})


class CrawlError(Exception):
    """Базовая ошибка краулера. Pipeline отображает её на ``source_unavailable``.

    ``http_status`` — **наблюдаемость**, а не классификация: фактический HTTP-статус
    ответа, если транспорт успел его увидеть (429/403/503/404 …). Он протаскивается
    до ``crawl_session_items.http_status``, чтобы оператор различал транзиентный
    rate-limit/недоступность (повтор осмыслен) и политический запрет (повтор
    бессмысленен). Класс отказа (``failure_kind``/``refusal_reason``) от статуса
    **не зависит** — таксономия ``FailureKind`` этим полем не расширяется.
    ``None`` — статус неизвестен (сетевой сбой до ответа, SSRF-отказ и т.п.).
    """

    def __init__(self, message: str = "", *, http_status: int | None = None) -> None:
        super().__init__(message)
        self.http_status = http_status


class CrawlUrlError(CrawlError, ValueError):
    """URL отклонён SSRF-валидацией (небезопасная/некорректная цель).

    Множественное наследование намеренно: вызывающий может ловить и как
    ``CrawlError``, и как ``ValueError`` (``validate_url`` поднимает ``ValueError``).
    """


class CrawlRefused(CrawlError):
    """Краул URL **отказан** по политике (не сбой транспорта). D1/§10.

    Носитель — лента ``robots.txt``, и отказ бывает двух видов, различаемых
    ``refusal_reason``:

    * ``"robots_disallow"`` — выигравшее правило ``Disallow``: **сайт запретил** обход;
    * ``"robots_unavailable"`` — ленту не удалось прочитать (5xx/сеть/таймаут), и origin
      по RFC 9309 §2.3.1.4 считается полностью запрещённым: **сайт ничего не запрещал**,
      разрешение установить не удалось. Слить их в один код значило бы утверждать запрет,
      которого никто не объявлял.

    Pipeline помечает элемент терминальным с этим кодом и **не** расширяет frontier от
    отказа. Отличается от ``CrawlError`` (недоступность источника) намеренно: отказ — не
    «источник упал», а соблюдение политики (повтор бессмысленен в обоих случаях).

    ``FailureKind`` сегодня знает только ``robots_disallow``; ``robots_unavailable``
    канонизируется в ``source_unavailable`` до появления одноимённого класса в словаре
    (``core/pipeline/corpus_persist.canonical_failure_kind``). Поэтому наблюдение дополнительно
    называет состояние ленты текстом ошибки — код не должен терять различие молча.
    """

    def __init__(
        self,
        message: str,
        *,
        refusal_reason: str = "robots_disallow",
        http_status: int | None = None,
    ) -> None:
        super().__init__(message, http_status=http_status)
        self.refusal_reason = refusal_reason


class CrawlNotFound(CrawlError):
    """Источник **определённо** ответил, что ресурса нет (HTTP 404/410).

    Это не недоступность источника: origin прочитан, и ответ окончателен. Поэтому
    ``failure_kind`` несёт ``FailureKind.NOT_FOUND``: pipeline пишет его в
    ``crawl_session_items.error_kind``, считает исход терминальным (без повторов) и
    **не** делает origin нечитаемым (``derive_corpus_coverage`` уже игнорирует
    ``not_found``). ``failed`` при этом остаётся классом сбоя, а не статусом (I4).

    Отдельно от :class:`CrawlRefused` намеренно: отказ — соблюдение политики
    (``robots.txt``), а здесь — ответ сервера об отсутствии ресурса. Смешивать их
    значило бы потерять различие «мы не пошли» / «нам ответили».

    Форма повторяет ``CrawlRefused``: машинный код — атрибут экземпляра, который
    pipeline читает через ``getattr`` (``corpus_ingest.commit_local``).
    """

    def __init__(
        self,
        message: str,
        *,
        failure_kind: str = FailureKind.NOT_FOUND.value,
        http_status: int | None = None,
    ) -> None:
        super().__init__(message, http_status=http_status)
        self.failure_kind = failure_kind


class CrawlPayloadTooLarge(CrawlError):
    """Ответ превысил свой потолок: страничный ``max_response_bytes`` либо
    потоковый ``max_stream_bytes`` терминального артефакта (P0/OOM, P6).

    Несёт ``failure_kind = payload_too_large``: pipeline помечает элемент терминальным
    ``FAILED``, **не** ретейнит байты и не запускает экстракцию. Это осознанная граница
    исполнения, а не транзиентная недоступность; повтор не помогает (I4: failed, не статус).
    """

    def __init__(
        self,
        message: str,
        *,
        failure_kind: str = FailureKind.PAYLOAD_TOO_LARGE.value,
        http_status: int | None = None,
    ) -> None:
        super().__init__(message, http_status=http_status)
        self.failure_kind = failure_kind


class CrawlUnavailable(CrawlError):
    """Транспорт **не может исполняться**: не поднялась необязательная зависимость.

    Причины по транспортам: ``httpx`` отсутствует у лёгкого HTTP-краулера; у браузерного —
    нет ``playwright``/``httpx`` либо не стартовали браузерные бинари; у Scrapy — нет
    самого движка или реактора. Один класс на все транспорты намеренно: вызывающий
    отличает «движок недоступен» (деградация/пропуск тира) от «источник недоступен»
    (``CrawlError``-сбой) **типом**, а не сравнением имени класса строкой.

    Это не сбой источника и не отказ политики: ``http_status`` здесь всегда ``None``.
    """
