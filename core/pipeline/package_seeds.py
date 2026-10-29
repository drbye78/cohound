"""coHound — сценарный слой авторского seed-набора пакета (09 §2.3/§7).

Слой связывает чистые операции над ``SeedsConfig`` (:mod:`core.config.seeds`) с
файлом пакета: читает ``sources/seeds.yaml``, применяет операцию, пишет **только если
байты изменились**, и возвращает структуру-результат «что изменилось» (для
``--dry-run`` — «что было бы изменено»).

Почему отдельно от :mod:`core.pipeline.package_admin`: seed-набор — авторский вход
краула, а не таблица ``RunInputs``; он не участвует в проспективном линте, поэтому
операция здесь не пересобирает пакет целиком. Общее с ``package_admin`` — форма
результата (``changed``/``written``/``dry_run``/``warnings``) и аудит-журнал
``.authoring/apply_log.jsonl``.

Инварианты, которые держит слой:

* **`--actor` обязателен** для мутаций (:func:`_require_actor`): запись без автора —
  потерянный провенанс, а не «удобный дефолт»;
* **no-op не пишет файл**: повторная операция не «трогает» артефакт (тот же приём,
  что в ``package_add_product``), поэтому повторный вызов с теми же аргументами даёт
  байт-идентичный файл;
* **`dry_run` не пишет вообще**: ни ``seeds.yaml``, ни журнал;
* **экспорт — обратная сторона чтения**: :func:`core.config.seeds.render_corpus_urls`
  рендерит ровно тот формат, который читает :func:`core.config.seeds.load_corpus_urls`.

Нейтрален к предметной категории (§1.3): домены и продукт — данные пакета.
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from core.config.domain_profiles import load_domain_profiles
from core.config.seeds import (
    CORPUS_URLS_RELPATH,
    SEEDS_RELPATH,
    add_seed,
    load_corpus_urls,
    load_seeds,
    render_corpus_urls,
    resolve_profile_key,
    save_seeds,
    set_seed_enabled,
    validate_seed_url,
)
from core.domain.enums import SeedRole, SeedSource
from core.domain.models import ProductSeeds, SeedsConfig
from core.pipeline.digests import seeds_digest
from core.pipeline.package_admin import PackageAdminError
from core.pipeline.package_io import append_apply_log
from core.util.atomic_write import atomic_write_text

__all__ = [
    "CORPUS_URLS_FORMAT",
    "SeedsExport",
    "SeedsOpResult",
    "product_seed_rows",
    "seeds_add",
    "seeds_disable",
    "seeds_enable",
    "seeds_export",
    "seeds_set_enabled",
]

#: Единственный формат экспорта — legacy ``corpus_urls.yaml`` (§7.5).
CORPUS_URLS_FORMAT = "corpus-urls"


@dataclass
class SeedsOpResult:
    """Результат операции над seed-набором.

    :param op: имя операции (``add``/``enable``/``disable``).
    :param product_id: продукт, к которому относится операция.
    :param url: URL в нормализованном виде (как он записан в наборе).
    :param dry_run: запись не производилась (``written`` пуст).
    :param changed: состояние набора изменилось (при ``dry_run`` — изменилось **бы**).
    :param written: фактически записанные пути (пусто при ``dry_run``/no-op).
    :param product_state: ``ProductSeeds.state`` после операции (``ready``/…).
    :param seeds_digest: ``seeds_digest`` набора после операции (идентичность корпуса).
    :param seeds_digest_before: ``seeds_digest`` до операции.
    :param product_seeds: seed'ы продукта после операции (JSON-совместимо).
    :param warnings: предупреждения нормализации/резолва (напр. query в URL).
    """

    op: str
    product_id: str | None = None
    url: str | None = None
    dry_run: bool = False
    changed: bool = False
    written: list[Path] = field(default_factory=list)
    product_state: str | None = None
    seeds_digest: str | None = None
    seeds_digest_before: str | None = None
    product_seeds: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        """JSON-совместимое представление результата."""
        return {
            "ok": True,
            "op": self.op,
            "dry_run": self.dry_run,
            "changed": self.changed,
            "product": self.product_id,
            "url": self.url,
            "product_state": self.product_state,
            "seeds_digest": self.seeds_digest,
            "seeds_digest_before": self.seeds_digest_before,
            "written": [str(path) for path in self.written],
            "seeds": list(self.product_seeds),
            "warnings": list(self.warnings),
        }


@dataclass
class SeedsExport:
    """Результат экспорта seed-набора в legacy-формат.

    :param format: имя формата (единственный — :data:`CORPUS_URLS_FORMAT`).
    :param text: отрендеренный текст (то, что уходит в stdout при отсутствии ``--out``).
    :param products: число продуктов в экспорте.
    :param urls: суммарное число активных seed'ов (URL) в экспорте.
    :param out: путь записи (``None`` — только stdout).
    :param written: файл записан (``False`` при ``dry_run``/совпадении байтов).
    """

    format: str
    text: str
    products: int = 0
    urls: int = 0
    out: Path | None = None
    written: bool = False

    def as_dict(self) -> dict[str, Any]:
        """JSON-совместимое представление результата."""
        return {
            "ok": True,
            "format": self.format,
            "products": self.products,
            "urls": self.urls,
            "out": str(self.out) if self.out is not None else None,
            "written": self.written,
        }


def _now() -> str:
    """UTC-метка времени авторской операции (журнал; артефакт не трогает)."""
    return _dt.datetime.now(_dt.UTC).isoformat()


def _require_actor(actor: str | None) -> str:
    """Автор мутации обязателен (как в :func:`core.config.seeds.ensure_seeds_file`)."""
    name = str(actor or "").strip()
    if not name:
        raise PackageAdminError(
            "actor обязателен: мутация seed-набора пишется с провенансом "
            "(--actor <человек>)",
            code="usage",
        )
    return name


def _package_dir(package_dir: str | Path) -> Path:
    """Каталог пакета; отсутствующий — ошибка использования, а не «пустой пакет»."""
    pkg = Path(package_dir)
    if not pkg.is_dir():
        raise PackageAdminError(f"пакет не найден: {pkg}", code="usage")
    return pkg


def _load(pkg: Path) -> SeedsConfig:
    """Читает seed-набор; битый файл — ошибка использования, не «пустой набор»."""
    try:
        return load_seeds(pkg / SEEDS_RELPATH)
    except ValueError as exc:
        raise PackageAdminError(str(exc), code="usage") from exc


def _profiles(pkg: Path) -> dict[str, Any]:
    """Ключи ``domain_profiles.yaml`` пакета (пусто, если файла нет).

    Читается штатным загрузчиком (не приватным хелпером ``config.seeds``): битый
    профиль — ошибка пакета, а не «seed без привязки».
    """
    path = pkg / "domain_profiles.yaml"
    if not path.is_file():
        return {}
    return load_domain_profiles(path)


def product_seed_rows(
    config: SeedsConfig, product_id: str | None
) -> list[dict[str, Any]]:
    """Seed'ы продукта в JSON-совместимом виде (пусто, если продукта нет).

    Единая форма для ``--json`` операций и ``seeds list``: ``model_dump(mode="json")``
    даёт значения enum'ов строками, даты — ISO.
    """
    if product_id is None:
        return []
    product = config.products.get(product_id)
    if product is None:
        return []
    return [seed.model_dump(mode="json") for seed in product.seeds]


def _state_of(config: SeedsConfig, product_id: str | None) -> str | None:
    """``ProductSeeds.state`` продукта (``None`` — продукта нет)."""
    if product_id is None:
        return None
    product: ProductSeeds | None = config.products.get(product_id)
    return product.state.value if product is not None else None


def _apply(
    pkg: Path,
    *,
    op: str,
    actor: str,
    dry_run: bool,
    before: SeedsConfig,
    after: SeedsConfig,
    product_id: str | None,
    url: str | None,
    warnings: list[str] | None = None,
) -> SeedsOpResult:
    """Общий хвост мутации: сравнение, запись при изменении, журнал, результат."""
    changed = after != before
    written: list[Path] = []
    if changed:
        # Первая запись продукта в ``seeds.yaml`` в пакете, живущем на legacy
        # ``corpus_urls.yaml``, меняет **эффективный** набор: ``seeds.yaml[product]``
        # приоритетнее legacy (``core/pipeline/onboarding.py:load_effective_seeds``).
        # Один добавленный seed молча вытеснил бы весь прежний корпус продукта — это
        # видимая потеря, а не деталь формата, поэтому предупреждение выдаётся и в
        # ``--dry-run``. Только для **новых** продуктов набора: уже вытеснённый legacy
        # не новость, и повторять предупреждение на каждую операцию значило бы шуметь.
        shadowed = _legacy_shadowed_products(pkg, after) - set(before.products)
        if shadowed:
            names = ", ".join(sorted(shadowed))
            warnings = list(warnings or [])
            warnings.append(
                f"legacy_seeds_shadowed: {names} — в пакете есть "
                f"{CORPUS_URLS_RELPATH}, и после записи {SEEDS_RELPATH} набор из него "
                "перестанет действовать для этих продуктов (seeds.yaml приоритетнее). "
                "Перенесите остальные URL в seeds.yaml или оставьте пакет на legacy"
            )
    if changed and not dry_run:
        written.append(save_seeds(pkg / SEEDS_RELPATH, after))
        append_apply_log(
            pkg,
            {
                "op": f"seeds_{op}",
                "actor": actor,
                "changed": ["seeds"],
                "deleted": [],
                "ts": _now(),
            },
        )
    return SeedsOpResult(
        op=op,
        product_id=product_id,
        url=url,
        dry_run=dry_run,
        changed=changed,
        written=written,
        product_state=_state_of(after, product_id),
        seeds_digest=seeds_digest(after),
        seeds_digest_before=seeds_digest(before),
        product_seeds=product_seed_rows(after, product_id),
        warnings=list(warnings or []),
    )


def _legacy_shadowed_products(pkg: Path, after: SeedsConfig) -> set[str]:
    """Продукты, чей legacy-набор будет вытеснен записью ``seeds.yaml``.

    Условие — **наличие** legacy-файла с URL для продукта, который есть в новом наборе:
    ``load_effective_seeds`` берёт ``seeds.yaml[product]``, если продукт там объявлен,
    и legacy для него уже не читается. Продукты, которых в ``seeds.yaml`` нет, legacy
    продолжает питать — они не вытеснены и в предупреждение не попадают.
    """
    legacy_path = pkg / CORPUS_URLS_RELPATH
    if not legacy_path.is_file():
        return set()
    try:
        legacy = load_corpus_urls(legacy_path)
    except ValueError:
        # Битый legacy — отдельная видимая проблема чтения, не повод гадать о вытеснении.
        return set()
    return {
        product_id
        for product_id, urls in legacy.items()
        if urls and product_id in after.products
    }

def _normalized_of(url: str) -> str:
    """Нормализованный вид URL записи (что реально лежит в наборе после операции).

    Операция уже провалидировала URL, поэтому повторная нормализация идемпотентна;
    если она всё же откажет, возвращается авторский ввод — значение не выдумывается.
    """
    try:
        return validate_seed_url(url)
    except ValueError:
        return str(url).strip()


def seeds_add(
    package_dir: str | Path,
    product_id: str,
    url: str,
    *,
    role: SeedRole = SeedRole.PRIMARY,
    source: SeedSource = SeedSource.OPERATOR,
    source_ref: str | None = None,
    profile_key: str | None = None,
    actor: str | None = None,
    at: _dt.date | str | None = None,
    dry_run: bool = False,
) -> SeedsOpResult:
    """Добавляет seed продукта в ``sources/seeds.yaml`` (идемпотентно).

    ``profile_key=None`` — автопривязка к самому длинному подходящему ключу
    ``domain_profiles.yaml`` (:func:`core.config.seeds.resolve_profile_key`).
    Существующий выключенный seed включается на месте (порядок сохраняется),
    существующий активный — no-op без записи файла.

    :raises PackageAdminError: нет ``actor``; недопустимый URL/продукт; битый профиль.
    """
    pkg = _package_dir(package_dir)
    actor_name = _require_actor(actor)
    before = _load(pkg)
    warnings: list[str] = []
    resolved_key = profile_key
    if resolved_key is None:
        try:
            resolved_key = resolve_profile_key(url, _profiles(pkg))
        except ValueError as exc:
            raise PackageAdminError(
                f"domain_profiles.yaml не читается: {exc}", code="usage"
            ) from exc
        if resolved_key is None:
            # Fallback на дефолтные оси — законный режим, но не молчаливый: seed вне
            # объявленной зоны профиля краулится с дефолтным `fetch`/`extract`, и
            # оператор обязан видеть, что зоны у него нет (09 §2.1 п. 2 требует
            # резолвимости в profile-key; CLI не создаёт ключ, а называет пробел).
            warnings.append(
                "profile_unbound: у URL нет ключа в domain_profiles.yaml — краул пойдёт "
                "с дефолтными осями fetch/extract; объявите ключ или передайте "
                "--profile-key"
            )
    try:
        after = add_seed(
            before,
            product_id,
            url,
            role=role,
            source=source,
            source_ref=source_ref,
            profile_key=resolved_key,
            actor=actor_name,
            at=at,
            warnings=warnings,
        )
    except ValueError as exc:
        raise PackageAdminError(str(exc), code="usage") from exc
    normalized = _normalized_of(url)
    return _apply(
        pkg,
        op="add",
        actor=actor_name,
        dry_run=dry_run,
        before=before,
        after=after,
        product_id=product_id,
        url=normalized,
        warnings=warnings,
    )


def seeds_set_enabled(
    package_dir: str | Path,
    product_id: str,
    url: str,
    *,
    enabled: bool,
    actor: str | None = None,
    at: _dt.date | str | None = None,
    dry_run: bool = False,
) -> SeedsOpResult:
    """Включает/выключает seed (soft-disable) и пересчитывает состояние продукта.

    :raises PackageAdminError: нет ``actor``; URL отсутствует в наборе продукта;
        недопустимый URL.
    """
    pkg = _package_dir(package_dir)
    actor_name = _require_actor(actor)
    before = _load(pkg)
    warnings: list[str] = []
    try:
        after = set_seed_enabled(
            before,
            product_id,
            url,
            enabled=enabled,
            actor=actor_name,
            at=at,
            warnings=warnings,
        )
    except ValueError as exc:
        raise PackageAdminError(str(exc), code="usage") from exc
    return _apply(
        pkg,
        op="enable" if enabled else "disable",
        actor=actor_name,
        dry_run=dry_run,
        before=before,
        after=after,
        product_id=product_id,
        url=_normalized_of(url),
        warnings=warnings,
    )


def seeds_enable(
    package_dir: str | Path,
    product_id: str,
    url: str,
    *,
    actor: str | None = None,
    at: _dt.date | str | None = None,
    dry_run: bool = False,
) -> SeedsOpResult:
    """Включает seed (обёртка :func:`seeds_set_enabled` с ``enabled=True``)."""
    return seeds_set_enabled(
        package_dir, product_id, url, enabled=True, actor=actor, at=at, dry_run=dry_run
    )


def seeds_disable(
    package_dir: str | Path,
    product_id: str,
    url: str,
    *,
    actor: str | None = None,
    at: _dt.date | str | None = None,
    dry_run: bool = False,
) -> SeedsOpResult:
    """Выключает seed (soft-disable; физического удаления в модели нет)."""
    return seeds_set_enabled(
        package_dir, product_id, url, enabled=False, actor=actor, at=at, dry_run=dry_run
    )


def seeds_export(
    package_dir: str | Path,
    *,
    fmt: str = CORPUS_URLS_FORMAT,
    out: str | Path | None = None,
    dry_run: bool = False,
) -> SeedsExport:
    """Экспортирует активные seed'ы в legacy ``{product_id: [url, ...]}``.

    Без ``out`` текст только возвращается (CLI печатает его в stdout). С ``out`` файл
    пишется **только если байты изменились** (повторный экспорт не «трогает» файл) и
    только при ``not dry_run``.

    :raises PackageAdminError: неизвестный формат; пакет не найден.
    """
    if fmt != CORPUS_URLS_FORMAT:
        raise PackageAdminError(
            f"неизвестный формат экспорта: {fmt!r} (единственный — {CORPUS_URLS_FORMAT})",
            code="usage",
        )
    pkg = _package_dir(package_dir)
    config = _load(pkg)
    if not (pkg / SEEDS_RELPATH).is_file():
        # Файла нет — эффективный набор может приходить из legacy `corpus_urls.yaml`
        # (`load_effective_seeds`, 09 §7.5). Пустой `{}` в этом случае был бы ложью:
        # он читается как «seed'ов нет», хотя корпус у пакета есть. Экспорт —
        # производный артефакт **нового** носителя, поэтому он честно отказывает и
        # называет миграцию, а не выдаёт себя за состояние набора.
        raise PackageAdminError(
            f"seeds.yaml отсутствует ({SEEDS_RELPATH}); эффективный набор пакета может "
            "идти из legacy sources/corpus_urls.yaml — перенесите его командой "
            "`hound package seeds add` или `hound package add-product --apply`",
            code="not_found",
        )
    text = render_corpus_urls(config)
    products = sum(1 for product in config.products.values() if product.enabled_for())
    urls = sum(len(product.enabled_for()) for product in config.products.values())
    target = Path(out) if out is not None else None
    written = False
    if target is not None and not dry_run:
        current = (
            target.read_text(encoding="utf-8") if target.is_file() else None
        )
        if current != text:
            atomic_write_text(target, text)
            written = True
    return SeedsExport(
        format=fmt,
        text=text,
        products=products,
        urls=urls,
        out=target,
        written=written,
    )
