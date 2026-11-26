"""Разведка списка сателлитов: три состояния и запрет «связи без цитаты».

Класс дефекта, который пин держит: находка модели без цитаты связи — это гипотеза, а не
кандидат. Если бы такая находка попадала в `proposed`, «разведка» тихо расширяла бы
объявление, а объявление связи — авторское решение (precision-first, I6/I7).

Второе, что пин держит: объявленный и не найденный сателлит **не** понижается —
`not_rediscovered` это факт о собранных страницах, а не утверждение «сателлита нет».
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from core.domain.enums import SatelliteState
from core.domain.models import Citation, Passport, Satellite
from core.pipeline.passport_scout import DiscoveryPassportHypothesis
from core.pipeline.satellite_discovery import discover_satellites

PAGE_TEXT = (
    "Platform V Demo DB. Состав дистрибутива: Console (управление), "
    "Backup (бэкапы с FUSE), Loader (загрузка данных)."
)


@dataclass
class _Page:
    url: str
    content: bytes = b"<html>page</html>"


@dataclass
class _Doc:
    text: str = PAGE_TEXT
    content_hash: str | None = None
    extractor_version: str = "test-1"
    error: str | None = None


class _Crawler:
    def fetch(self, url: str, *args: Any, **kwargs: Any) -> _Page:
        return _Page(url=url)


class _Extractor:
    def extract(self, page: Any, *args: Any, **kwargs: Any) -> _Doc:
        return _Doc()


@dataclass
class _Hint:
    url: str | None
    quote: str | None


@dataclass
class _Hyp:
    role: str
    component: str | None
    exists_confidence: float = 0.9
    origin: str = "in_house"
    relationship: str = "bundled"
    evidence_hint: _Hint | None = None


class _LLM:
    model_id = "fake-model"

    def __init__(self, satellites: list[_Hyp]) -> None:
        self._satellites = satellites

    def complete(self, request: Any) -> Any:
        hypothesis = DiscoveryPassportHypothesis(
            satellites=[asdict(item) for item in self._satellites]
        )

        @dataclass
        class _Response:
            output: DiscoveryPassportHypothesis
            prompt_hash: str = "deadbeef"
            usage: Any = None
            reasoning_tokens: int = 0

        return _Response(output=hypothesis)


def _passport(*, with_declared: bool = True) -> Passport:
    satellites = []
    if with_declared:
        satellites = [
            Satellite(
                role="management_console",
                state=SatelliteState.APPROVED,
                component="Console",
                origin="in_house",
                relationship="bundled",
                evidence_state="verified",
                relationship_evidence=Citation(
                    url="https://docs.example.com/v1/", quote="Console (управление)"
                ),
            ),
            Satellite(
                role="backup",
                state=SatelliteState.APPROVED,
                component="Backup",
                origin="in_house",
                relationship="bundled",
                evidence_state="verified",
                relationship_evidence=Citation(
                    url="https://docs.example.com/v1/", quote="Backup (бэкапы с FUSE)"
                ),
            ),
        ]
    return Passport(
        product_id="demo",
        canonical_name="Platform V Demo DB",
        vendor="Vendor",
        jurisdiction="RU",
        doc_domains=["docs.example.com/v1"],
        satellites=satellites,
        snapshot_date="2026-11-27",  # type: ignore[arg-type]
    )


def test_confirmed_and_new_and_not_rediscovered() -> None:
    llm = _LLM(
        [
            _Hyp(
                role="management_console",
                component="Console",
                evidence_hint=_Hint(
                    url="https://docs.example.com/v1/", quote="Console (управление)"
                ),
            ),
            _Hyp(
                role="data_loader",
                component="Loader",
                evidence_hint=_Hint(
                    url="https://docs.example.com/v1/", quote="Loader (загрузка данных)"
                ),
            ),
        ]
    )
    report = discover_satellites(
        _passport(), crawler=_Crawler(), extractor=_Extractor(), discovery_llm=llm
    )
    assert report.ok
    assert [item.component for item in report.proposed] == ["Loader"]
    assert [item["component"] for item in report.confirmed] == ["Console"]
    # Backup объявлен, но на странице не подтверждён цитатой модели — состояние не понижено.
    assert [item["component"] for item in report.not_rediscovered] == ["Backup"]


def test_new_satellite_without_quote_is_not_proposed() -> None:
    llm = _LLM([_Hyp(role="data_loader", component="Loader", evidence_hint=None)])
    report = discover_satellites(
        _passport(), crawler=_Crawler(), extractor=_Extractor(), discovery_llm=llm
    )
    assert report.proposed == []
    assert any("связь не подтверждена" in note for note in report.needs_human)


def test_quote_outside_pages_is_rejected() -> None:
    llm = _LLM(
        [
            _Hyp(
                role="data_loader",
                component="Loader",
                evidence_hint=_Hint(
                    url="https://docs.example.com/v1/", quote="Loader не упоминался"
                ),
            )
        ]
    )
    report = discover_satellites(
        _passport(), crawler=_Crawler(), extractor=_Extractor(), discovery_llm=llm
    )
    assert report.proposed == []
    assert any("связь не подтверждена" in note for note in report.needs_human)


def test_no_doc_domains_is_an_error_not_an_empty_list() -> None:
    passport = _passport(with_declared=False)
    passport.doc_domains = []
    report = discover_satellites(
        passport, crawler=_Crawler(), extractor=_Extractor(), discovery_llm=_LLM([])
    )
    assert not report.ok
    assert any(error.startswith("no_doc_domains") for error in report.errors)
