"""Порт чанкера — ``ChunkerPort`` (§3.2, Ш5).

Текст → чанки с ``heading_path`` и ``context_prefix``. ``text`` чанка — чистый текст для
строгой проверки «цитата — подстрока» (§10/C.2); ``context_prefix`` в валидацию не входит.
``chunker_version`` участвует в ``index_build_id`` (§13). Нейтрален (§1.3).
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from core.ports.types import Chunk, ExtractedDoc


@runtime_checkable
class ChunkerPort(Protocol):
    """Детерминированный чанкер. Дефолт — ``MarkdownChunker`` (heading_path)."""

    version: str

    def chunk(self, doc: ExtractedDoc, document_id: str) -> list[Chunk]: ...
