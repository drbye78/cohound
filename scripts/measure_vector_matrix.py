"""Сравнение накопителей векторов партиции: байты артефакта и пик RSS (план 26 O10).

**Зачем прибор.** `_embed_batched` держал все векторы партиции списком списков: ~32 КиБ на
вектор, то есть 3.4 ГиБ на 108 614 чанков и 6.4 ГиБ на 200 000. 09.11.2026 живой прогон
`ru-pg-dbms-f887ab549fe9` умер от OOM-killer'а именно в этой фазе (`MemAvailable` 209 MB).
Накопитель заменён на `float16`-матрицу, но замена допустима **только** если байты
`vectors.fp16.npy` не изменились: иначе это `content_change`, а не оптимизация.

**Что делает.** Читает чанки одной продукт-партиции из БД прогона (только чтение), считает
векторы **из кэша** (`EmbedCache`: промах — громкая ошибка, а не тихий пересчёт моделью),
накапливает их указанной реализацией и печатает: пик RSS процесса, размер матрицы и
sha256 байтов `float16`. Сверка с уже собранной партицией — `--compare`.

Реализации берутся не «по памяти», а из файлов: `--module-file` загружает **другую**
версию модуля (например, выгруженную `git show HEAD:core/pipeline/corpus_index.py`), поэтому
сравниваются настоящие коды, а не пересказ.

Запуск (две реализации — два процесса: пик RSS нельзя измерить дважды в одном):

    git show HEAD:core/pipeline/corpus_index.py > /tmp/opencode/corpus_index_head.py
    .venv/bin/python scripts/measure_vector_matrix.py \\
      --db ~/.cohound/stands/ru-pg-dbms/runs/ru-pg-dbms-f887ab549fe9/cohound.db \\
      --product tantor --cache ~/.cohound/data/embed_cache \\
      --module-file /tmp/opencode/corpus_index_head.py --out-json /tmp/opencode/vec-old.json
    .venv/bin/python scripts/measure_vector_matrix.py \\
      --db ~/.cohound/stands/ru-pg-dbms/runs/ru-pg-dbms-f887ab549fe9/cohound.db \\
      --product tantor --cache ~/.cohound/data/embed_cache \\
      --out-json /tmp/opencode/vec-new.json

Границы: считается **один** продукт; RSS включает интерпретатор, чанки и кэш-обвязку —
сравнивать надо две строки между собой, а не с абсолютной величиной. Модель не
загружается: эмбеддер-заглушка отказывает на промахе кэша, поэтому замер не зависит ни
от сети, ни от GPU.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import resource
import sys
from pathlib import Path
from typing import Any

import numpy as np

from core.pipeline.embed_cache import EmbedCache
from core.store.sqlite_store import SqliteStore

#: Модель и условия счёта — из объявленных осей прогона (`config/deploy.yaml:embed`).
DEFAULT_MODEL = "BAAI/bge-m3"


class _CacheOnlyEmbedder:
    """Эмбеддер, который умеет только объявить размерность: промах кэша — отказ.

    Пересчитывать моделью в этом замере нельзя: тогда сравнивались бы не накопители, а
    два прогона модели, и расхождение байтов читалось бы как дефект накопителя.
    """

    def __init__(self, dim: int, model_id: str) -> None:
        self.dim = dim
        self.model_id = model_id
        self.calls = 0

    def embed(self, texts: list[str]) -> list[list[float]]:
        self.calls += 1
        raise RuntimeError(
            f"промах кэша на {len(texts)} текстах: замер обязан идти только из кэша "
            "(иначе сравниваются не накопители, а вызовы модели)"
        )


class _SyntheticEmbedder:
    """Эмбеддер, выдающий детерминированные векторы: замер накопителя без модели и кэша.

    Значения берутся из хеша текста, поэтому два процесса (старый и новый накопитель)
    видят одни и те же числа, а сравнение байтов имеет смысл. Модель не загружается:
    предмет замера — накопитель, а не эмбеддинг.
    """

    def __init__(self, dim: int, model_id: str = "synthetic") -> None:
        self.dim = dim
        self.model_id = model_id
        self.calls = 0

    def embed(self, texts: list[str]) -> list[list[float]]:
        self.calls += 1
        rows: list[list[float]] = []
        for text in texts:
            seed = hashlib.sha256(text.encode("utf-8")).digest()
            rows.append([
                (seed[index % len(seed)] / 255.0) * 2.0 - 1.0
                for index in range(self.dim)
            ])
        return rows


class _SyntheticChunk:
    """Минимальный носитель, которого ждёт ``_embed_batched``: текст, хеш, id."""

    __slots__ = ("chunk_id", "content_hash", "text")

    def __init__(self, index: int) -> None:
        self.text = f"синтетический чанк {index}"
        self.content_hash = hashlib.sha256(self.text.encode("utf-8")).hexdigest()
        self.chunk_id = f"synthetic:{index}"


def _load_embed_batched(module_file: str | None) -> Any:
    """``_embed_batched`` из указанного файла либо из рабочего дерева."""
    if module_file is None:
        from core.pipeline.corpus_index import _embed_batched  # noqa: PLC0415

        return _embed_batched
    path = Path(module_file).expanduser()
    if not path.is_file():
        raise SystemExit(f"{path}: файла нет — модуль для сравнения не загрузить")
    spec = importlib.util.spec_from_file_location("_compared_corpus_index", path)
    if spec is None or spec.loader is None:
        raise SystemExit(f"{path}: модуль не загружается")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module._embed_batched  # noqa: SLF001 — предмет сравнения и есть приватная функция


def _peak_rss_mb() -> float:
    """Пик RSS процесса в МиБ (``ru_maxrss`` в Linux — килобайты)."""
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0, 1)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--db", default=None, help="cohound.db прогона (только чтение)")
    parser.add_argument("--product", default=None)
    parser.add_argument(
        "--synthetic",
        type=int,
        default=0,
        metavar="N",
        help="вместо БД: N синтетических чанков (замер накопителя без кэша и модели)",
    )
    parser.add_argument("--cache", default=None, help="каталог embed_cache (нужен без --synthetic)")
    parser.add_argument("--embed-model", default=DEFAULT_MODEL)
    parser.add_argument("--device", default="cuda")
    parser.add_argument("--precision", default="fp16")
    parser.add_argument("--dim", type=int, default=1024)
    parser.add_argument("--module-file", default=None, help="другая версия corpus_index.py")
    parser.add_argument("--compare", default=None, help="собранная партиция для сверки байтов")
    parser.add_argument("--out-json", default=None)
    args = parser.parse_args(argv)

    embed_batched = _load_embed_batched(args.module_file)
    if args.synthetic:
        # Синтетический материал: ровно N чанков, векторы детерминированы, кэш не нужен.
        chunks = [_SyntheticChunk(index) for index in range(args.synthetic)]
        build_id = "synthetic"
        cache = None
        embedder = _SyntheticEmbedder(args.dim)
        product = f"synthetic:{args.synthetic}"
    else:
        if not args.db or not args.product or not args.cache:
            print(
                "measure: без --synthetic нужны --db, --product и --cache",
                file=sys.stderr,
            )
            return 2
        db_path = Path(args.db).expanduser()
        if not db_path.is_file():
            print(f"measure: БД не найдена: {db_path}", file=sys.stderr)
            return 2
        store = SqliteStore(db_path, read_only=True)
        build_id = None
        for row in store.conn.execute(  # прибор читает БД только на чтение
            "SELECT DISTINCT build_id FROM corpus_chunks WHERE product_id = ?",
            (args.product,),
        ):
            build_id = str(row[0])
        if not build_id:
            print(f"measure: у продукта {args.product} нет чанков", file=sys.stderr)
            return 2
        chunks = list(store.iter_chunks(build_id, product_id=args.product))
        cache = EmbedCache(Path(args.cache).expanduser())
        embedder = _CacheOnlyEmbedder(args.dim, args.embed_model)
        product = args.product
    vectors = embed_batched(
        embedder,
        chunks,
        cache=cache,
        embed_model_id=getattr(embedder, "model_id", args.embed_model),
        embed_device=args.device,
        embed_precision=args.precision,
    )
    # Старый путь возвращает список списков, новый — уже `float16`-матрицу. Приведение
    # здесь повторяет то, что делает сборщик (`np.asarray(..., dtype=np.float16)`).
    matrix = np.asarray(vectors, dtype=np.float16)
    ids = [chunk.chunk_id for chunk in chunks]
    digest = hashlib.sha256(matrix.tobytes()).hexdigest()

    payload: dict[str, Any] = {
        "product": product,
        "build_id": build_id,
        "chunks": len(chunks),
        "shape": list(matrix.shape),
        "dtype": str(matrix.dtype),
        "matrix_mb": round(matrix.nbytes / 1024 / 1024, 1),
        "bytes_sha256": digest,
        "peak_rss_mb": _peak_rss_mb(),
        "embedder_calls": embedder.calls,
        "module": args.module_file or "рабочее дерево",
    }
    if args.compare:
        path = Path(args.compare).expanduser() / "vectors.fp16.npy"
        ids_path = Path(args.compare).expanduser() / "chunk_ids.jsonl"
        if not path.is_file():
            payload["compare"] = f"нет файла {path}"
        elif not ids_path.is_file():
            payload["compare"] = f"нет файла {ids_path}"
        else:
            # Строки сверяются **по chunk_id**, а не по позиции: сборщик пишет партицию в
            # порядке сборки (по документу), а `iter_chunks` отдаёт порядок
            # (product_id, chunk_no, chunk_id). Сравнение по позиции дало бы «все строки
            # разные» на одном и том же содержимом — измеренный факт, не дефект.
            on_disk = np.load(path)
            disk_ids = [json.loads(line)["chunk_id"] for line in ids_path.open(encoding="utf-8")]
            position = {chunk_id: index for index, chunk_id in enumerate(ids)}
            missing = [chunk_id for chunk_id in disk_ids if chunk_id not in position]
            # Приводим **свою** матрицу к порядку диска (а не наоборот): диск — то, что
            # уже подписано и лежит, его строки не переставляются.
            aligned = matrix[[position[c] for c in disk_ids if c in position]]
            payload["compare"] = {
                "path": str(path),
                "rows_on_disk": len(disk_ids),
                "rows_missing_here": len(missing),
                "same_order": disk_ids == ids,
                "bytes_equal": aligned.tobytes() == on_disk.tobytes(),
                "disk_sha256": hashlib.sha256(on_disk.tobytes()).hexdigest(),
                "here_sha256": hashlib.sha256(aligned.tobytes()).hexdigest(),
            }
    print(json.dumps(payload, ensure_ascii=False, indent=1))
    if args.out_json:
        target = Path(args.out_json).expanduser()
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(payload, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
