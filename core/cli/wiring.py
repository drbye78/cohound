"""coHound — сборка зависимостей CLI (composition root Z0, ручной DI, A1; ``03`` §E.3).

Единственное место, где ядро соединяет ``pipeline`` с адаптерами: ``pipeline`` только
принимает порты как аргументы и **никогда** не инстанцирует адаптеры сам (``01`` §1, A2).
Никаких фреймворков — ручной DI (A1).

**Профили развёртывания (``07``).** ``local`` (дефолт) собирает реальные адаптеры
in-process. ``split`` (T13.4) собирает remote-прокси (``RemoteCrawler``/
``RemoteSearch``/``RemoteRegistry``/``RemoteExtractor``) поверх ``HttpContentGateway``
по секции ``edge.*`` deploy-конфига. ``edge/`` и любой DMZ-код **не импортируются**
(матрица слоёв ``01`` §1: ``cli`` не зависит от ``edge``). Certificate pinning
отсутствует намеренно (``07`` §8.1): доверие — через корпоративный CA.

**Состояние W5/W13.** Большинство in-process адаптеров (краул/экстрактор/чанкер/
индекс/LLM/SERP) появятся в W7–W11; для ``local`` отсутствующие порты остаются
``None`` с явным TODO. Для ``split`` заполняются только remote-прокси веб-контура;
не-веб порты — по мере появления.

Нейтрален к предметной категории (§1.3): никаких названий продуктов/вендоров.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from core.adapters.remote_crawler import RemoteCrawler
from core.adapters.remote_extractor import RemoteExtractor
from core.adapters.remote_gateway import (
    DEFAULT_SCHEMA_VERSION,
    HttpContentGateway,
    TransportFn,
)
from core.adapters.remote_registry import RemoteRegistry
from core.adapters.remote_search import RemoteSearch
from core.config.deploy import DeployConfig, is_split, load_deploy_config
from core.pipeline import Ports, Runner
from core.pipeline.runner import PHASES, Clock, PhaseFn
from core.ports.gateway import ContentGateway
from core.store.sqlite_store import SqliteStore

__all__ = [
    "LOCAL_PROFILE",
    "SPLIT_PROFILE",
    "SUPPORTED_PROFILES",
    "DEFAULT_REGISTRY_ID",
    "PHASES",
    "WiringError",
    "run_db_path",
    "build_store",
    "build_gateway",
    "build_adapters",
    "build_runner",
    "manifest_deploy_fields",
]

#: Профиль ``local`` — один процесс, реальные адаптеры in-process (дефолт, ``07``).
LOCAL_PROFILE = "local"
#: Профиль ``split`` — веб-адаптеры в DMZ, ядро ходит через ``ContentGateway`` (``07``).
SPLIT_PROFILE = "split"
#: Известные профили развёртывания, выбираемые CLI (``03`` §D.5/D.6).
SUPPORTED_PROFILES: tuple[str, ...] = (LOCAL_PROFILE, SPLIT_PROFILE)

#: Идентификатор реестра по умолчанию для remote-прокси (данные; ``deploy.yaml``
#: в модели ``DeployConfig`` не несёт ``registry_fetchers`` — см. ГЭП в
#: :func:`manifest_deploy_fields`).
DEFAULT_REGISTRY_ID = "edge"


class WiringError(ValueError):
    """Ошибка сборки зависимостей: неизвестный или ещё не реализованный профиль."""


def run_db_path(runs_dir: str | Path, run_id: str) -> Path:
    """Путь к операционной БД прогона: ``runs/{run_id}/cohound.db`` (``03`` §A.2)."""
    return Path(runs_dir) / run_id / "cohound.db"


def build_store(
    db_path: str | Path = ":memory:", *, profile: str = LOCAL_PROFILE
) -> SqliteStore:
    """Собирает операционное хранилище прогона.

    Для обоих профилей ``StorePort`` живёт в ядре (доверенная зона ``core``, ``01`` §3.0),
    поэтому выбор профиля на хранилище не влияет — разделяются лишь веб-адаптеры
    (:func:`build_adapters`). Каталог БД создаётся при необходимости.

    :param db_path: файл БД или ``":memory:"``.
    :param profile: профиль развёртывания (проверяется на известность).
    :raises WiringError: неизвестный профиль.
    """
    if profile not in SUPPORTED_PROFILES:
        raise WiringError(f"неизвестный профиль развёртывания: {profile!r}")
    if str(db_path) != ":memory:":
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    return SqliteStore(db_path)


def build_gateway(
    deploy: DeployConfig,
    *,
    transport: TransportFn | None = None,
    extra_headers: Mapping[str, str] | None = None,
) -> HttpContentGateway:
    """Собирает HTTP-транспорт ядро→edge по секции ``edge.*`` (``07`` §7).

    Соответствие полей ``deploy.edge`` параметрам :class:`HttpContentGateway`:

    * ``base_url`` → ``base_url`` (обязателен для split-профилей, ``07`` §7);
    * ``timeout_s``/``max_retries``/``max_response_bytes`` → одноимённые лимиты;
    * ``tls.ca`` → ``ca`` (доверие к корпоративному CA, **без pinning**, ``07`` §8.1);
    * ``tls.client_cert``/``tls.client_key`` → mTLS workload-identity;
    * ``auth: oidc`` — токен выдаёт корпоративный IDP в рантайме, в ``DeployConfig``
      его нет; при необходимости заголовок передаётся через ``extra_headers``.

    ``transport`` — тестовый шов ``(op, envelope) -> envelope`` (без сети); при
    ``None`` используется реальный HTTP (ленивый ``httpx``).

    :raises WiringError: не задан ``edge.base_url``.
    """
    edge = deploy.edge
    base_url = (edge.base_url or "").strip()
    if not base_url:
        raise WiringError(
            f"профиль {deploy.profile!r} требует непустой edge.base_url (07 §7)"
        )
    return HttpContentGateway(
        base_url=base_url,
        schema_version=DEFAULT_SCHEMA_VERSION,
        transport=transport,
        timeout=edge.timeout_s,
        max_retries=edge.max_retries,
        max_response_bytes=edge.max_response_bytes,
        ca=edge.tls.ca,
        client_cert=edge.tls.client_cert,
        client_key=edge.tls.client_key,
        extra_headers=extra_headers,
    )


def build_adapters(
    *,
    profile: str = LOCAL_PROFILE,
    deploy: DeployConfig | None = None,
    transport: TransportFn | None = None,
    gateway: ContentGateway | None = None,
    registry_id: str = DEFAULT_REGISTRY_ID,
    extra_headers: Mapping[str, str] | None = None,
) -> Ports:
    """Собирает порты внешних систем по профилю развёртывания (``03`` §E.3).

    * ``local`` — реальные адаптеры in-process (W7–W11); сейчас пустой :class:`Ports`.
    * ``split`` — remote-прокси поверх :class:`HttpContentGateway` (``07`` §4): краулер,
      поиск, экстрактор и реестровый фетчер маршрутизируются в DMZ. Конфиг берётся из
      ``deploy`` или загружается из ``config/deploy.yaml``; профиль конфига обязан быть
      ``split``/``split-hardened``.

    :param deploy: типизированный deploy-конфиг; ``None`` → :func:`load_deploy_config`.
    :param transport: инъектируемый шов gateway (тесты без сокета).
    :param gateway: готовый ``ContentGateway`` (переопределяет сборку по конфигу).
    :param registry_id: идентификатор реестра для ``RemoteRegistry`` (данные).
    :param extra_headers: доп. заголовки шва (напр. OIDC bearer).
    :raises WiringError: неизвестный профиль или split без валидного deploy-конфига.
    """
    if profile == LOCAL_PROFILE:
        # TODO(W7–W11): crawler/extractor/chunker (W7), индексы/embedder (W8),
        # evaluator (W9), search (W11), registry (W6) — заполнять здесь по мере появления.
        return Ports()
    if profile == SPLIT_PROFILE:
        config = deploy if deploy is not None else load_deploy_config(None)
        if not is_split(config):
            raise WiringError(
                "профиль split требует deploy-конфиг с profile split/split-hardened, "
                f"получено {config.profile!r} (07 §6/§7)"
            )
        content_gateway = gateway or build_gateway(
            config, transport=transport, extra_headers=extra_headers
        )
        return Ports(
            crawler=RemoteCrawler(content_gateway),
            search=RemoteSearch(
                content_gateway,
                adapter_kind=(config.search.adapter or "remote"),
                provider="edge",
            ),
            extractor=RemoteExtractor(content_gateway),
            registry=RemoteRegistry(content_gateway, registry_id=registry_id),
        )
    raise WiringError(f"неизвестный профиль развёртывания: {profile!r}")


def manifest_deploy_fields(
    deploy: DeployConfig, capabilities: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    """Поля развёртывания для ``run_manifest.yaml`` (R7; ``07`` §7).

    Возвращает сведения, которые кладутся в ``RunManifest.edge_capabilities`` —
    единственное существующее поле манифеста для версий edge:

    * ``deploy_profile`` — выбранный профиль зон (``local``/``split``/``split-hardened``);
    * ``edge_schema_version`` — версия контракта шва (версионная граница §13);
    * версии edge-компонентов из ``/v1/capabilities`` (если переданы).

    **ГЭП (T13.4).** У доменного ``RunManifest`` (``core/domain/models.py``) нет
    отдельных полей ``deploy_profile`` и ``edge_schema_version``; ``SqliteStore.
    read_manifest`` не заполняет ``edge_capabilities``. Правка доменной модели/стора
    вне write-scope T13.4, поэтому поля возвращаются этим хелпером и складываются в
    ``edge_capabilities`` вызывающей стороной (манифест-писатель W10/T10.4).
    """
    fields: dict[str, Any] = {
        "deploy_profile": deploy.profile,
        "edge_schema_version": DEFAULT_SCHEMA_VERSION,
    }
    if capabilities:
        fields.update(dict(capabilities))
    return fields


def build_runner(
    store: SqliteStore,
    *,
    profile: str = LOCAL_PROFILE,
    ports: Ports | None = None,
    phases: dict[str, PhaseFn] | None = None,
    clock: Clock | None = None,
) -> Runner:
    """Собирает :class:`Runner` с внедрёнными портами (ручной DI, A1).

    ``phases`` переопределяет реестр фаз (тесты/будущие W7–W11); по умолчанию действует
    реестр ``Runner`` (echo-заглушки + реальные проверки гейтов). Порты, если не переданы,
    собираются :func:`build_adapters` по профилю.

    :raises WiringError: неизвестный/нереализованный профиль (см. :func:`build_adapters`).
    """
    resolved_ports = ports if ports is not None else build_adapters(profile=profile)
    return Runner(store, ports=resolved_ports, clock=clock, phases=phases)
