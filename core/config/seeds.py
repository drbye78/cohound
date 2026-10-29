"""coHound — авторский seed-набор пакета ``sources/seeds.yaml`` (09 §7, MVP-1).

Модуль отвечает за:

* загрузку/атомарную запись канонического артефакта (``schema: seeds/1``);
* нормализацию URL по §7.2 (http/https, lowercase host, default port, fragment, path
  case/trailing slash, query сохраняется с предупреждением);
* детерминированный ``seed_id``;
* привязку seed'а к самому длинному ключу ``host[+path]`` из ``domain_profiles.yaml``;
* миграцию legacy ``sources/corpus_urls.yaml`` в ``sources/seeds.yaml``;
* merge/дедуп по нормализованному URL;
* авторские операции над набором (``add_seed``/``set_seed_enabled``) и обратный
  legacy-экспорт (``render_corpus_urls``) — чистые функции над ``SeedsConfig``.

Нейтрален к предметной категории (§1.3): домены и продукт — данные пакета.
Слой ``config`` зависит только от ``domain``/``config``/``util``.
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import yaml

from core.domain.enums import OnboardingState, SeedRole, SeedSource, SeedState
from core.domain.models import ProductSeeds, SeedEntry, SeedsConfig
from core.util.atomic_write import atomic_write_text as write_atomic_text
from core.util.hashing import short_hash
from core.util.security import validate_url

__all__ = [
    "CORPUS_URLS_RELPATH",
    "SEEDS_RELPATH",
    "SEEDS_SCHEMA",
    "add_seed",
    "ensure_seeds_file",
    "load_corpus_urls",
    "load_seeds",
    "merge_seeds",
    "normalize_seed_url",
    "render_corpus_urls",
    "render_seeds",
    "resolve_profile_key",
    "save_seeds",
    "seed_id",
    "set_seed_enabled",
    "validate_seed_url",
]

#: Текущая схема файла seed-набора.
SEEDS_SCHEMA = "seeds/1"
#: Путь канонического seed-артефакта относительно каталога пакета.
SEEDS_RELPATH = "sources/seeds.yaml"
#: Legacy-экспорт ``{product_id: [url, ...]}`` (миграционный fallback §7.5).
CORPUS_URLS_RELPATH = "sources/corpus_urls.yaml"

_DEFAULT_PORTS = {"http": 80, "https": 443}


def normalize_seed_url(url: str, *, warnings: list[str] | None = None) -> str:
    """Нормализует seed-URL по §7.2.

    Схема — только ``http``/``https``; host приводится к нижнему регистру; default port
    снимается; fragment удаляется; path case и trailing slash **сохраняются** (сервер
    может отдавать разное); query сохраняется, но добавляет warning (политика seed'ов
    не любит пагинацию/дубли).

    :raises ValueError: пустой URL, не-http(s) схема, отсутствующий host, учётные данные,
        некорректный порт.
    """
    text = str(url or "").strip()
    if not text:
        raise ValueError("seed URL пуст")
    if "://" not in text:
        raise ValueError(f"seed URL без схемы: {text!r} (только http/https)")
    parts = urlsplit(text)
    scheme = parts.scheme.lower()
    if scheme not in ("http", "https"):
        raise ValueError(f"seed URL: схема только http/https, получено {parts.scheme!r}")
    host = (parts.hostname or "").strip().lower()
    if not host:
        raise ValueError(f"seed URL без hostname: {text!r}")
    if parts.username or parts.password:
        raise ValueError("seed URL: учётные данные в URL запрещены")
    try:
        port = parts.port
    except ValueError as exc:
        raise ValueError(f"seed URL: некорректный порт ({exc})") from exc
    netloc = f"[{host}]" if ":" in host else host
    if port is not None and port != _DEFAULT_PORTS[scheme]:
        netloc = f"{netloc}:{port}"
    path = parts.path or "/"
    query = parts.query
    if query and warnings is not None:
        warnings.append(
            f"seed_query: {text!r} содержит query; seed'ы с query не рекомендуются "
            "(политика дедупликации пагинации)"
        )
    suffix = f"?{query}" if query else ""
    return f"{scheme}://{netloc}{path}{suffix}"


def validate_seed_url(url: str, *, warnings: list[str] | None = None) -> str:
    """Нормализует URL и применяет SSRF-политику :func:`core.util.security.validate_url`."""
    normalized = normalize_seed_url(url, warnings=warnings)
    validate_url(normalized)
    return normalized


def seed_id(product_id: str, normalized_url: str) -> str:
    """Детерминированный id seed'а: ``short_hash(product_id \\x1f normalized_url, 24)``."""
    return short_hash(f"{product_id}\x1f{normalized_url}", 24)


def resolve_profile_key(
    url: str, profiles: Mapping[str, Any] | None
) -> str | None:
    """Самый длинный ключ ``host[+path]``, покрывающий URL (или ``None``).

    Семантика совпадает с :func:`core.pipeline.fetch_plan.select_domain_profile`:
    host равен ключу либо является его поддоменом; path (если задан) матчится по границе
    сегмента; приоритет — длина path, затем длина host, затем лексикографически меньший
    ключ.
    """
    if not profiles:
        return None
    parsed = urlsplit(str(url or ""))
    url_host = (parsed.hostname or "").strip().lower()
    if not url_host:
        return None
    url_path = parsed.path or "/"
    best_key: str | None = None
    best_rank: tuple[int, int] | None = None
    for raw_key in profiles:
        key = str(raw_key).strip()
        host_key, _, path_part = key.partition("/")
        host_key = host_key.strip().lower()
        path_key = f"/{path_part.strip('/')}" if path_part.strip("/") else ""
        if not host_key:
            continue
        if url_host != host_key and not url_host.endswith("." + host_key):
            continue
        if path_key and not (
            url_path == path_key or url_path.startswith(path_key + "/")
        ):
            continue
        rank = (len(path_key), len(host_key))
        if (
            best_rank is None
            or rank > best_rank
            or (rank == best_rank and best_key is not None and key < best_key)
        ):
            best_key, best_rank = key, rank
    return best_key


def _read_yaml(path: Path) -> Any:
    """Читает YAML; ошибки I/O/синтаксиса приводит к ``ValueError``."""
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ValueError(f"не удалось прочитать {path}: {exc}") from exc
    try:
        return yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ValueError(f"некорректный YAML в {path}: {exc}") from exc


def _atomic_write_text(path: Path, text: str) -> None:
    """Атомарная запись с сохранением режима (см. :mod:`core.util.atomic_write`)."""
    write_atomic_text(path, text)


def load_seeds(path: str | Path) -> SeedsConfig:
    """Загружает ``sources/seeds.yaml``; отсутствующий файл → пустой конфиг.

    ``path`` — **файл**, не каталог пакета: переданный каталог раньше молча давал
    пустой конфиг (``is_file()`` ложно), и вызывающий не видел ошибки. Теперь каталог —
    :class:`ValueError` с подсказкой про ``SEEDS_RELPATH``.

    :raises ValueError: передан каталог; файл не читается/не является mapping/не
        проходит схему.
    """
    target = Path(path)
    if target.is_dir():
        raise ValueError(
            f"{target}: ожидался файл seed-набора, получен каталог "
            f"(передайте {SEEDS_RELPATH})"
        )
    if not target.is_file():
        return SeedsConfig(products={})
    data = _read_yaml(target)
    if data is None:
        return SeedsConfig(products={})
    if not isinstance(data, dict):
        raise ValueError(
            f"{target}: ожидался YAML-словарь верхнего уровня, "
            f"получено {type(data).__name__}"
        )
    try:
        return SeedsConfig.model_validate(data)
    except ValueError as exc:
        raise ValueError(f"{target}: некорректный seed-набор: {exc}") from exc


def render_seeds(config: SeedsConfig) -> str:
    """Канонический YAML seed-набора (стабильный порядок продуктов).

    Отделено от записи, чтобы вызывающий мог сравнить байты и не перезаписывать файл,
    содержимое которого не изменилось (no-op apply не должен «трогать» артефакт).
    """
    data = config.model_dump(mode="json", by_alias=True, exclude_none=True)
    data["schema"] = SEEDS_SCHEMA
    products = data.get("products") or {}
    data["products"] = {key: products[key] for key in sorted(products)}
    return yaml.safe_dump(
        data, allow_unicode=True, sort_keys=False, default_flow_style=False
    )


def save_seeds(path: str | Path, config: SeedsConfig) -> Path:
    """Атомарно пишет ``sources/seeds.yaml`` (стабильный порядок продуктов)."""
    target = Path(path)
    _atomic_write_text(target, render_seeds(config))
    return target


def load_corpus_urls(path: str | Path) -> dict[str, list[str]]:
    """Читает legacy ``corpus_urls.yaml`` как ``{product_id: [url, ...]}``.

    Отсутствующий файл → ``{}``. Формат намеренно строг: значения — списки строк.
    """
    target = Path(path)
    if not target.is_file():
        return {}
    data = _read_yaml(target)
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError(f"{target}: ожидался объект {{product_id: [url, ...]}}")
    result: dict[str, list[str]] = {}
    for product_id, values in data.items():
        if not isinstance(values, list) or not all(
            isinstance(item, str) for item in values
        ):
            raise ValueError(
                f"{target}: {product_id!r} должен быть списком строк (URL)"
            )
        result[str(product_id)] = list(values)
    return result


def render_corpus_urls(config: SeedsConfig) -> str:
    """Legacy-экспорт ``{product_id: [url, ...]}`` — точная обратная сторона
    :func:`load_corpus_urls`.

    Формат совпадает с тем, что читает :func:`load_corpus_urls`: словарь верхнего
    уровня, значения — списки строк. Экспортируются **только активные** seed'ы
    (``enabled`` и ``state=enabled``), в авторском порядке внутри продукта: порядок
    корней значим для краула и входит в ``seeds_digest``. Продукты упорядочены по
    ``product_id`` (как в :func:`render_seeds`), продукты без активных seed'ов не
    эмитятся — пустой список не несёт корпуса и в legacy-файле означал бы «объявлено
    пусто» вместо «не объявлено».

    Пустой набор даёт ``{}``: :func:`load_corpus_urls` читает его как ``{}``.
    """
    products: dict[str, list[str]] = {}
    for product_id in sorted(config.products):
        urls = [seed.normalized_url for seed in config.products[product_id].enabled_for()]
        if urls:
            products[product_id] = urls
    return yaml.safe_dump(
        products, allow_unicode=True, sort_keys=False, default_flow_style=False
    )


def _profile_keys(package_dir: Path) -> dict[str, Any]:
    """Ключи ``domain_profiles.yaml`` пакета (пусто, если файла нет)."""
    path = package_dir / "domain_profiles.yaml"
    if not path.is_file():
        return {}
    data = _read_yaml(path)
    return data if isinstance(data, dict) else {}


def _seed_entry(
    *,
    product_id: str,
    url: str,
    role: SeedRole,
    source: SeedSource,
    source_ref: str | None,
    profile_key: str | None,
    added_at: _dt.date,
    actor: str,
    warnings: list[str] | None = None,
) -> SeedEntry | None:
    """Собирает валидный ``SeedEntry`` или возвращает ``None`` (с warning)."""
    try:
        normalized = validate_seed_url(url, warnings=warnings)
    except ValueError as exc:
        if warnings is not None:
            warnings.append(f"seed_invalid: {product_id}/{url}: {exc}")
        return None
    return SeedEntry(
        url=str(url).strip(),
        normalized_url=normalized,
        role=role,
        source=source,
        source_ref=source_ref,
        profile_key=profile_key,
        added_at=added_at,
        added_by=actor,
        enabled=True,
        state=SeedState.ENABLED,
    )


def ensure_seeds_file(
    package_dir: str | Path,
    actor: str,
    *,
    today: _dt.date | str | None = None,
) -> SeedsConfig:
    """Гарантирует наличие ``sources/seeds.yaml``; отсутствующий — мигрирует legacy.

    Если файл уже есть — он **не перезаписывается**. Иначе все записи
    ``sources/corpus_urls.yaml`` импортируются как ``role=primary``,
    ``source=migration``, ``enabled=true`` с разрешённым ``profile_key``; порядок
    продуктов и URL сохраняется.

    :raises ValueError: существующий файл невалиден или legacy-файл имеет неверную форму.
    """
    pkg = Path(package_dir)
    target = pkg / SEEDS_RELPATH
    if target.is_file():
        return load_seeds(target)
    snapshot = (
        _dt.date.fromisoformat(str(today)) if today is not None and not isinstance(today, _dt.date)
        else (today or _dt.date.today())
    )
    actor_name = str(actor or "").strip()
    if not actor_name:
        raise ValueError("ensure_seeds_file: actor обязателен")
    profiles = _profile_keys(pkg)
    legacy = load_corpus_urls(pkg / CORPUS_URLS_RELPATH)
    products: dict[str, ProductSeeds] = {}
    for product_id in legacy:
        entries: list[SeedEntry] = []
        for url in legacy[product_id]:
            entry = _seed_entry(
                product_id=product_id,
                url=url,
                role=SeedRole.PRIMARY,
                source=SeedSource.MIGRATION,
                source_ref=CORPUS_URLS_RELPATH,
                profile_key=resolve_profile_key(url, profiles),
                added_at=snapshot,
                actor=actor_name,
            )
            if entry is not None:
                entries.append(entry)
        products[product_id] = ProductSeeds(
            state=OnboardingState.READY if entries else OnboardingState.PENDING_SEEDS,
            seeds=entries,
        )
    config = SeedsConfig(products=products)
    save_seeds(target, config)
    return config


def merge_seeds(
    existing: SeedsConfig,
    product_id: str,
    new_entries: Iterable[SeedEntry],
) -> SeedsConfig:
    """Merge seed'ов продукта: существующие сохраняются, новые дописываются.

    Дедуп — по точному ``normalized_url`` (path case/trailing slash значимы, §7.2).
    Существующая запись побеждает (idempotent re-add не перетирает провенанс).
    """
    product = existing.products.get(product_id)
    current = list(product.seeds) if product is not None else []
    seen = {seed.normalized_url for seed in current}
    appended = False
    for entry in new_entries:
        if entry.normalized_url in seen:
            continue
        seen.add(entry.normalized_url)
        current.append(entry)
        appended = True
    state = _product_state(product, current, appended=appended)
    products = dict(existing.products)
    products[product_id] = ProductSeeds(state=state, seeds=current)
    return SeedsConfig(schema=existing.schema_, products=products)


# ---------------------------------------------------------------------------
# Авторские операции над набором (чистые функции: конфиг → конфиг)
# ---------------------------------------------------------------------------


def _snapshot_date(at: _dt.date | _dt.datetime | str | None) -> _dt.date:
    """Дата записи: явный ``at`` (ISO-строка/``date``) или текущая."""
    if at is None:
        return _dt.date.today()
    if isinstance(at, _dt.datetime):
        return at.date()
    if isinstance(at, _dt.date):
        return at
    return _dt.date.fromisoformat(str(at))


def _product_state(
    product: ProductSeeds | None, seeds: list[SeedEntry], *, appended: bool
) -> OnboardingState:
    """Состояние продукта по инварианту O1 (``ready`` требует ≥1 активного seed'а).

    ``blocked``/``retired`` — решения человека: они не понижаются до ``pending_seeds``,
    если набор не менялся (``appended=False``) и активных seed'ов по-прежнему нет.
    """
    if any(seed.is_enabled for seed in seeds):
        return OnboardingState.READY
    if product is not None and not appended and product.state in (
        OnboardingState.BLOCKED,
        OnboardingState.RETIRED,
    ):
        return product.state
    return OnboardingState.PENDING_SEEDS


def _replace_product(
    config: SeedsConfig,
    product_id: str,
    product: ProductSeeds | None,
    seeds: list[SeedEntry],
) -> SeedsConfig:
    """Новый конфиг с пересчитанным состоянием продукта (остальные продукты не тронуты)."""
    state = _product_state(product, seeds, appended=False)
    products = dict(config.products)
    products[product_id] = ProductSeeds(state=state, seeds=seeds)
    return SeedsConfig(schema=config.schema_, products=products)


def add_seed(
    config: SeedsConfig,
    product_id: str,
    url: str,
    *,
    role: SeedRole = SeedRole.PRIMARY,
    source: SeedSource = SeedSource.OPERATOR,
    source_ref: str | None = None,
    profile_key: str | None = None,
    profiles: Mapping[str, Any] | None = None,
    actor: str,
    at: _dt.date | str | None = None,
    warnings: list[str] | None = None,
) -> SeedsConfig:
    """Добавляет seed продукта (идемпотентно) и возвращает новый ``SeedsConfig``.

    Поведение:

    * URL нормализуется и проходит SSRF-политику (:func:`validate_seed_url`);
    * дедуп — по ``normalized_url`` (через :func:`merge_seeds`, порядок сохраняется);
    * уже существующий **активный** seed — no-op: конфиг возвращается как есть
      (провенанс не перетирается, ``seeds_digest`` не меняется);
    * существующий, но **выключенный** seed — включается на месте: позиция в списке
      (авторский порядок, он же ``order`` в ``seeds_digest``) не меняется, а
      ``role``/``source``/``added_*`` остаются прежними — это возврат в набор, а не
      новая запись; ``last_verified_at`` обновляется (тот же результат, что у
      :func:`set_seed_enabled` с ``enabled=True`);
    * новый URL дописывается **в конец** списка продукта.

    ``profile_key=None`` означает «привязать автоматически»: ключ берётся из
    ``profiles`` (ключи ``domain_profiles.yaml``) самым длинным совпадением
    ``host[+path]``. Явный ``profile_key`` имеет приоритет.

    :raises ValueError: пустой ``product_id``/``actor``, недопустимый URL.
    """
    pid = str(product_id or "").strip()
    if not pid:
        raise ValueError("add_seed: product_id обязателен")
    actor_name = str(actor or "").strip()
    if not actor_name:
        raise ValueError("add_seed: actor обязателен")
    normalized = validate_seed_url(url, warnings=warnings)
    product = config.products.get(pid)
    current = list(product.seeds) if product is not None else []
    for index, seed in enumerate(current):
        if seed.normalized_url != normalized:
            continue
        if seed.is_enabled:
            return config
        current[index] = seed.model_copy(
            update={
                "enabled": True,
                "state": SeedState.ENABLED,
                "last_verified_at": _snapshot_date(at),
            }
        )
        return _replace_product(config, pid, product, current)
    entry = _seed_entry(
        product_id=pid,
        url=url,
        role=role,
        source=source,
        source_ref=source_ref,
        profile_key=(
            profile_key
            if profile_key is not None
            else resolve_profile_key(normalized, profiles)
        ),
        added_at=_snapshot_date(at),
        actor=actor_name,
    )
    if entry is None:  # pragma: no cover — URL уже провалидирован выше
        raise ValueError(f"add_seed: недопустимый URL {url!r}")
    return merge_seeds(config, pid, [entry])


def set_seed_enabled(
    config: SeedsConfig,
    product_id: str,
    url: str,
    *,
    enabled: bool,
    actor: str,
    at: _dt.date | str | None = None,
    warnings: list[str] | None = None,
) -> SeedsConfig:
    """Включает/выключает существующий seed (soft-disable, идемпотентно).

    Физического удаления в модели нет (09 §3.1): выключение — ``enabled=False`` +
    ``state=disabled``. Состояние ``invalid`` при выключении сохраняется (это признак
    «URL негоден», а не «выключен человеком»); при включении ``invalid`` снимается —
    включение и есть ручная перепроверка. Включение пишет ``last_verified_at``
    (``at``/сегодня): оператор подтвердил, что seed снова в наборе.

    ``ProductSeeds.state`` пересчитывается по инварианту модели: выключение последнего
    активного seed'а переводит продукт из ``ready`` в ``pending_seeds`` (``ready`` без
    активного seed'а — противоречие O1, и ``ProductSeeds`` такое не примет).

    :raises ValueError: пустой ``product_id``/``actor``, недопустимый URL или URL,
        которого нет в наборе продукта (молчаливый no-op запрещён).
    """
    pid = str(product_id or "").strip()
    if not pid:
        raise ValueError("set_seed_enabled: product_id обязателен")
    actor_name = str(actor or "").strip()
    if not actor_name:
        raise ValueError("set_seed_enabled: actor обязателен")
    normalized = validate_seed_url(url, warnings=warnings)
    product = config.products.get(pid)
    if product is None:
        raise ValueError(f"set_seed_enabled: продукта {pid!r} нет в seed-наборе")
    current = list(product.seeds)
    for index, seed in enumerate(current):
        if seed.normalized_url != normalized:
            continue
        if enabled:
            desired: dict[str, Any] = {
                "enabled": True,
                "state": SeedState.ENABLED,
                "last_verified_at": _snapshot_date(at),
            }
        elif seed.state is SeedState.INVALID:
            # ``invalid`` — «URL негоден», а не «выключен человеком»: выключение его не
            # переписывает (иначе признак негодности тихо теряется).
            desired = {"enabled": False}
        else:
            desired = {"enabled": False, "state": SeedState.DISABLED}
        if all(getattr(seed, key) == value for key, value in desired.items()):
            return config  # уже в требуемом состоянии — no-op
        current[index] = seed.model_copy(update=desired)
        return _replace_product(config, pid, product, current)
    raise ValueError(
        f"set_seed_enabled: seed {normalized} не найден у продукта {pid!r} "
        f"(известно {len(current)} seed'ов)"
    )

