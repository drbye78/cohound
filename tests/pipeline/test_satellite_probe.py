"""Драйвер разведки корней: что он называет и чего не утверждает.

Пины держат два измеренных дефекта первого реального вызова (27.11.2026):

1. **Пометка антибота не должна противоречить собственным числам.** На `jatoba.ru`
   страница отдала 121 ссылку и слово `captcha` в разметке (виджет формы), а прибор
   писал «страница отдала проверку — ссылок в ней нет по построению». Замечание
   допустимо только там, где оно объясняет пустоту.
2. **Адрес наблюдения объявляет оператор.** Свидетельство связи указывает на корень
   портала, который отдаёт заглушку Qrator; без `--seed` разведка оттуда не начинается,
   и это состояние называется `antibot_challenge`, а не «дерево пустое».
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from core.pipeline.satellite_probe import SatelliteProbePorts, probe_satellite_roots

HOST = "sat.example"


@dataclass
class _RawPage:
    url: str
    content: bytes
    status_code: int = 200


@dataclass
class _Doc:
    text: str = "текст"
    content_hash: str | None = None
    extractor_version: str = "test-1"
    error: str | None = None


class _Crawler:
    def __init__(self, content: bytes) -> None:
        self._content = content

    def fetch(self, url: str, plan: Any = None) -> _RawPage:
        assert plan is not None
        return _RawPage(url=url, content=self._content)


class _Extractor:
    def extract(self, page: Any, *args: Any, **kwargs: Any) -> _Doc:
        return _Doc()


@dataclass
class _Satellite:
    role: str
    component: str | None = None
    relationship_evidence: Any = None


@dataclass
class _Evidence:
    url: str
    quote: str = "цитата связи"


def _links(count: int) -> bytes:
    anchors = "".join(
        f'<a href="/docs/page-{index}.html">p{index}</a>' for index in range(count)
    )
    return f"<html><body>{anchors}</body></html>".encode()


def _notes(content: bytes) -> list[str]:
    ports = SatelliteProbePorts(
        http=_Crawler(content), extractor=_Extractor(), browser=None, robots=None
    )
    plans = probe_satellite_roots(
        product_id="p",
        satellites=[_Satellite(role="backup", relationship_evidence=_Evidence(f"https://{HOST}/"))],
        ports=ports,
    )
    return plans[0].notes


def test_captcha_widget_on_page_with_links_is_not_a_challenge() -> None:
    """Слово `captcha` в разметке при живых ссылках — виджет, а не заглушка."""
    notes = _notes(
        b'<html><body><form><script src="/recaptcha.js"></script>captcha</form>'
        + _links(4).split(b"<body>")[1]
    )
    assert not any(note.startswith("antibot_challenge") for note in notes)
    assert "links: 4" in notes


def test_challenge_page_without_links_is_named() -> None:
    """Заглушка без ссылок называется своим именем — иначе это «пустое дерево»."""
    notes = _notes(
        b'<html><head><script src="/__qrator/qauth.js"></script></head><body></body></html>'
    )
    assert any(note.startswith("antibot_challenge") for note in notes)
    assert "links: 0" in notes


def test_operator_seed_overrides_relationship_evidence() -> None:
    """`--seed` — объявленный вход наблюдения: он же попадает в замечания."""
    ports = SatelliteProbePorts(
        http=_Crawler(_links(4)), extractor=_Extractor(), browser=None, robots=None
    )
    plans = probe_satellite_roots(
        product_id="p",
        satellites=[_Satellite(role="backup", relationship_evidence=_Evidence(f"https://{HOST}/"))],
        ports=ports,
        seed_urls={"backup": f"https://{HOST}/docs/index.html"},
    )
    plan = plans[0]
    assert plan.seed_url == f"https://{HOST}/docs/index.html"
    assert f"seed: https://{HOST}/docs/index.html (объявлен оператором)" in plan.notes
    assert plan.candidates[0].key == f"{HOST}/docs"
