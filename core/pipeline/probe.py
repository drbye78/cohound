"""coHound — шаг ``probe`` между ``crawl`` и ``extract`` (срез C2): классификация артефактов.

Шаг **ничего не извлекает** и не превращает байты в evidence: он фиксирует техническую
классификацию допущенного артефакта (класс/формат/размер), читает число страниц PDF из
заголовка и перечисляет **имена** членов контейнера. Для ``archive_descent=plan`` это
ровно и требуется: состав контейнера виден, члены не извлекаются.

Модуль чистый относительно I/O: байты передаёт вызывающий (DI через ``read_all``),
поэтому ни ФС, ни сеть здесь не трогаются (слой ``pipeline``; I19 — источник только
ретенированные байты, прочитанные вызывающим).

Инвариант среза: ``extraction_required=False`` у контейнеров — при ``plan`` они не входят
в знаменатель покрытия экстракции. При ``full`` спуск в этом срезе не исполняется, поэтому
контейнер тоже не требуется; факт честно записывается в ``note``.
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

from core.domain.enums import ArchiveDescent
from core.domain.models import ARTIFACT_CLASSES, CorpusArtifact
from core.util.document_probe import (
    MAX_MEMBER_LIST,
    detect_format,
    list_container_members,
    pdf_page_count,
)

__all__ = [
    "DEFAULT_MAX_CONTAINER_SCAN_BYTES",
    "ProbeResult",
    "as_dict",
    "probe_all",
    "probe_artifact",
]

#: Потолок чтения контейнера для перечисления состава (имена членов лежат в конце файла
#: zip/RAR, поэтому читается весь файл). Сверх потолка состав не выдумывается.
DEFAULT_MAX_CONTAINER_SCAN_BYTES = 64 * 1024 * 1024


@dataclass(frozen=True)
class ProbeResult:
    """Результат ``probe`` одного артефакта (техническая классификация, не доменный статус)."""

    artifact_class: str
    format: str | None
    page_count: int | None
    member_count: int | None
    members: tuple[str, ...]
    extraction_required: bool
    probed_at: str
    note: str | None = None

    def __post_init__(self) -> None:
        """Класс — из фиксированного технического набора (не свободная строка)."""
        if self.artifact_class not in ARTIFACT_CLASSES:
            raise ValueError(
                f"probe: artifact_class={self.artifact_class!r} вне {ARTIFACT_CLASSES}"
            )


def _now_iso() -> str:
    """UTC-метка времени в ISO-8601 (детерминированная форма провенанса)."""
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


def _classify(declared: str, fmt: str | None, head: bytes) -> str:
    """Класс артефакта: байты авторитетнее объявленного класса краула."""
    if fmt in ("zip", "7z", "rar"):
        return "container"
    if fmt == "pdf":
        return "document"
    stripped = head.lstrip()[:1024]
    if stripped.startswith(b"%PDF-"):
        return "document"
    if declared in ARTIFACT_CLASSES:
        return declared
    return "page"


def probe_artifact(
    artifact: CorpusArtifact,
    *,
    head: bytes,
    read_all: Callable[[], bytes] | None = None,
    descent: ArchiveDescent = ArchiveDescent.PLAN,
    now: str | None = None,
    max_scan_bytes: int = DEFAULT_MAX_CONTAINER_SCAN_BYTES,
) -> ProbeResult:
    """Классифицирует артефакт и (для контейнера) перечисляет имена членов.

    :param artifact: замороженная строка ``corpus_artifacts``.
    :param head: первые байты файла (``BINARY_SNIFF_BYTES`` достаточно).
    :param read_all: DI-читатель всего файла; нужен только контейнерам (имена лежат в
        конце). ``None``/исключение/превышение потолка → состав не установлен (``None``),
        а не «пусто».
    :param descent: ось ``crawl.archive_descent``; ``plan`` — перечислить состав, не
        извлекать. В срезе C2 спуск не исполняется ни при каком значении (видно в ``note``).
    :param max_scan_bytes: потолок чтения контейнера для перечисления состава.
    """
    probed_at = now or _now_iso()
    fmt = detect_format(head, artifact.url, artifact.content_type)
    artifact_class = _classify(artifact.artifact_class, fmt, head)
    page_count: int | None = None
    member_count: int | None = None
    members: tuple[str, ...] = ()
    notes: list[str] = []

    # Число страниц PDF лежит в trailer'е (конец файла), поэтому по 8-KiB ``head`` оно
    # почти всегда не определяется: читаем весь файл (в пределах потолка probe).
    data: bytes | None = None
    wants_data = fmt == "pdf" or artifact_class == "container"
    if wants_data and read_all is not None:
        if artifact.size_bytes and artifact.size_bytes > max_scan_bytes:
            notes.append(
                f"полное чтение пропущено: {artifact.size_bytes} байт > потолка "
                f"{max_scan_bytes}"
            )
        else:
            try:
                data = read_all()
            except Exception as exc:  # noqa: BLE001 — читатель инъектируется
                notes.append(f"полное чтение не удалось: {type(exc).__name__}: {exc}")

    if fmt == "pdf":
        page_count = pdf_page_count(data if data is not None else head)
        if page_count is None:
            notes.append("pdf: число страниц из заголовка не определено")

    if artifact_class == "container":
        # Контейнеры не входят в знаменатель покрытия экстракции: спуск в этом срезе
        # не исполняется (plan — по контракту, full — ещё не реализован).
        listing = (
            list_container_members(data, fmt) if data is not None and fmt else None
        )
        if listing is None:
            if fmt == "7z":
                notes.append("7z: состав не читается без распаковки заголовка")
            elif data is not None:
                notes.append(f"{fmt}: состав контейнера не разобран")
        else:
            members = tuple(listing[:MAX_MEMBER_LIST])
            member_count = len(listing)
            if len(listing) > MAX_MEMBER_LIST:
                notes.append(f"состав усечён до {MAX_MEMBER_LIST} имён")
        if descent is ArchiveDescent.PLAN:
            notes.append("archive_descent=plan: состав зафиксирован, члены не извлекались")
        else:
            notes.append(
                "archive_descent=full: спуск не исполняется в срезе C2 — "
                "контейнер не извлекался"
            )

    extraction_required = artifact_class != "container"
    return ProbeResult(
        artifact_class=artifact_class,
        format=fmt,
        page_count=page_count,
        member_count=member_count,
        members=members,
        extraction_required=extraction_required,
        probed_at=probed_at,
        note="; ".join(notes) or None,
    )


def probe_all(
    artifacts: Iterable[CorpusArtifact],
    *,
    read_head: Callable[[CorpusArtifact], bytes],
    read_all: Callable[[CorpusArtifact], bytes] | None = None,
    descent: ArchiveDescent = ArchiveDescent.PLAN,
    now: str | None = None,
    max_scan_bytes: int = DEFAULT_MAX_CONTAINER_SCAN_BYTES,
) -> list[tuple[CorpusArtifact, ProbeResult]]:
    """Прогоняет :func:`probe_artifact` по набору артефактов (детерминированный порядок).

    ``read_head`` обязателен; ``read_all`` вызывается только для контейнеров. Порядок
    входа сохраняется (вызывающий передаёт уже отсортированный набор).
    """
    probed_at = now or _now_iso()
    results: list[tuple[CorpusArtifact, ProbeResult]] = []
    for artifact in artifacts:
        result = probe_artifact(
            artifact,
            head=read_head(artifact),
            read_all=(lambda a=artifact: read_all(a)) if read_all is not None else None,
            descent=descent,
            now=probed_at,
            max_scan_bytes=max_scan_bytes,
        )
        results.append((artifact, result))
    return results


def as_dict(result: ProbeResult) -> dict[str, Any]:
    """JSON-совместимое представление (для логов/диагностики)."""
    return {
        "artifact_class": result.artifact_class,
        "format": result.format,
        "page_count": result.page_count,
        "member_count": result.member_count,
        "members": list(result.members),
        "extraction_required": result.extraction_required,
        "probed_at": result.probed_at,
        "note": result.note,
    }
