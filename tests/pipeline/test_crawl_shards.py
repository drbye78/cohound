"""F7 (17 §2): статические шарды узлов — «какие host'ы мои» и честность корпуса.

Что проверяется (по §6 F7 и §7.1 T9/T10):

1. **T9 — пересечение шардов отвергается.** Проверка конфига отдельно
   (``tests/config/test_deploy.py``), здесь — что отказ доходит до CLI как
   ``EXIT_USAGE`` и называет host'ы **и** узлы.
2. **Узел берёт только свои host'ы.** Чужой host не попадает ни в очередь, ни в
   ``crawl_session_items``; отказ виден счётчиком ``host_out_of_shard`` (точное число по
   host'ам + примеры), а не молчаливым дропом. Фильтр стоит **после** scope и не
   подменяет ``Frontier.host_in_scope``: «чужое железо» — не «вне scope продукта».
3. **Шардированный узел не объявляет корпус полным.** Сеанс закрывается ``partial``,
   ``coverage.complete is False`` с причиной ``host_out_of_shard``, и ``❌`` на таком
   покрытии **не публикуется** (I2b/I2c: ``check_absence_coverage`` понижает до ``❓``).
4. **Пустой шард видим**, а «карта объявлена, узел не выбран» — отказ ``EXIT_USAGE``:
   «взял все host'ы молча» дало бы два процесса на одних host'ах.
5. **Идентичность vs провенанс.** Карта шардов входит в ``crawl_config_digest`` (иначе
   два разных корпуса жили бы под одним ``run_id``), а **выбранный узел** — нет: в
   кластере все узлы объявляют одну карту и обязаны получить один ``run_id``.
6. **T10 — смерть узла не теряет и не удваивает документы:** просроченные аренды
   возвращаются в ``pending``, сеанс честно ``interrupted``, конверсия после
   восстановления даёт ровно по одному документу на строку.

Нейтральные данные (§1.3): ``p1``, ``docs.test``, ``h0.docs.test``. Сеть не используется.
"""

from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path
from typing import Any

import pytest
import yaml

import tests.pipeline.test_crawl_only as harness
from core.cli.context import _node_id
from core.cli.main import EXIT_OK, EXIT_USAGE, main
from core.config.deploy import (
    CrawlConfig,
    CrawlShardConfig,
    DeployConfig,
    resolve_host_shard,
)
from core.domain import (
    CrawlSessionStatus,
    DenialStrength,
    EvidenceScope,
    Status,
)
from core.domain.models import CrawlRunConfig
from core.pipeline.corpus_coverage import HOST_OUT_OF_SHARD_REASON
from core.pipeline.crawl_frontier import CrawlLimits, Frontier
from core.pipeline.evaluate import _enforce_absence_coverage
from core.pipeline.extract_queue import ExtractQueue
from core.pipeline.liveness import recover_interrupted_sessions
from core.pipeline.report import crawl_control_digest
from core.store.sqlite_store import SqliteStore
from tests.helpers.packages import valid_package

ROOT = harness.ROOT
HOSTS = harness.HOSTS

#: Объявленная карта шардов: корень — узлу A, объявленные поддомены — узлу B.
SHARDS = [
    CrawlShardConfig(node="node-a", hosts=[ROOT.split("//", 1)[1].rstrip("/")]),
    CrawlShardConfig(node="node-b", hosts=list(HOSTS)),
]


# --------------------------------------------------------------------------- #
# Обвязка: сборка корпуса с объявленным шардом (те же фейковые порты, что у F2/F5)
# --------------------------------------------------------------------------- #


def _pages() -> dict[str, bytes]:
    """Страницы: корень ссылается на объявленные поддомены (волна ссылок, max_depth=1)."""
    pages = dict(harness._two_host_pages())
    for host in HOSTS:
        pages[f"https://{host}/"] = f"<html>root-{host}</html>".encode()
    return pages


def _build(
    tmp_path: Path,
    store: SqliteStore,
    *,
    urls: dict[str, list[str]] | None = None,
    crawl_app: CrawlConfig | None = None,
    node_id: str | None = None,
    crawl_only: bool = True,
    **kwargs: Any,
) -> tuple[Any, Any]:
    """Сборка корпуса Ш5 с объявленной картой шардов; возвращает (краулер, результат)."""
    crawler = harness._Crawler(_pages())
    harness._make_run(store)
    result = harness.build_corpus(
        harness._plan(),
        crawler,
        harness._Extractor(),
        harness._Chunker(),
        store,
        urls={"p1": [ROOT]} if urls is None else urls,
        raw_root=tmp_path,
        run_id=harness.RUN_ID,
        domain_profiles=harness._profiles(),
        crawl_defaults=CrawlRunConfig(max_depth=1),
        crawl_only=crawl_only,
        crawl_app=crawl_app,
        node_id=node_id,
        **kwargs,
    )
    return crawler, result


def _provenance(store: SqliteStore) -> dict[str, Any]:
    session = store.get_crawl_session(harness.SESSION_ID)
    assert session is not None
    return json.loads(session.provenance_json or "{}")


def _root_record(prov: dict[str, Any]) -> dict[str, Any]:
    roots = (prov.get("effective_limits") or {}).get("roots") or []
    assert len(roots) == 1, roots
    return dict(roots[0])


# --------------------------------------------------------------------------- #
# (1) frontier: фильтр шарда отдельный от scope и он видим
# --------------------------------------------------------------------------- #


def test_shard_admits_only_its_hosts_and_counts_refusals() -> None:
    """Узел A: в очередь идут только его host'ы; отказ чужому host'у назван числом.

    ``host_out_of_shard`` — не «нет доступа» и не «вне scope»: материал принадлежит
    другому узлу. Поэтому отказ считается по host'ам **и** ставит усечение корня: корпус
    этого корня этим узлом не полон по построению.
    """
    limits = CrawlLimits(
        max_depth=1,
        host_shards=tuple((shard.node, tuple(shard.hosts)) for shard in SHARDS),
    )
    frontier = Frontier(ROOT, limits, scope_hosts=HOSTS, shard_hosts=("docs.test",))
    assert frontier.add_seed(ROOT) == "queued"

    frontier.offer([f"https://{host}/doc" for host in HOSTS], parent_depth=0)

    assert len(frontier) == 1  # только seed: чужие host'ы не поставлены
    assert frontier.host_out_of_shard == {host: 1 for host in HOSTS}
    assert frontier.host_out_of_shard_examples["h0.docs.test"] == [
        "https://h0.docs.test/doc"
    ]
    # Усечение корня названо причиной, а не спрятано: `host_out_of_shard` — машинный
    # токен, тот же, что у провенанса сеанса и причины неполного покрытия.
    assert frontier.truncated is True
    assert frontier.truncated_reason == HOST_OUT_OF_SHARD_REASON
    # Scope не тронут: чужой host по-прежнему **в scope продукта** (он объявлен), просто
    # не на этом железе.
    assert frontier.host_in_scope("h0.docs.test") is True
    assert frontier.host_in_shard("h0.docs.test") is False


def test_shard_key_is_the_limiter_key_not_the_scope_key() -> None:
    """``www.`` и базовый host — **разные** ключи шарда (ключ вежливости), один scope.

    У лимитера на них разные бюджеты, поэтому шард нормируется тем же правилом. Молча
    «схлопывать» их в шарде значило бы объявить чужой host своим: расхождение объявленного
    шарда с реально фетчащимся host'ом видно счётчиком, а не тихим дропом.
    """
    frontier = Frontier(
        "https://www.docs.test/",
        CrawlLimits(max_depth=0),
        scope_hosts=("docs.test",),
        shard_hosts=("docs.test",),
    )
    assert frontier.host_in_scope("www.docs.test") is True
    assert frontier.host_in_shard("www.docs.test") is False
    assert frontier.add_seed("https://www.docs.test/") == "host_out_of_shard"
    assert frontier.host_out_of_shard == {"www.docs.test": 1}


# --------------------------------------------------------------------------- #
# (2) T9: пересечение шардов — отказ CLI, называющий host и узлы
# --------------------------------------------------------------------------- #


def _deploy_yaml(tmp_path: Path, shards: list[tuple[str, list[str]]]) -> None:
    """Пишет ``config/deploy.yaml`` с объявленной картой шардов (единственный вход F7)."""
    cfg = tmp_path / "config"
    cfg.mkdir(exist_ok=True)
    payload = {
        "profile": "local",
        "crawl": {
            "shards": [
                {"node": node, "hosts": list(hosts)} for node, hosts in shards
            ]
        },
    }
    (cfg / "deploy.yaml").write_text(
        yaml.safe_dump(payload, sort_keys=False, allow_unicode=True), encoding="utf-8"
    )


def _common(package: Path, *, json_mode: bool = True) -> list[str]:
    return [
        "--run-yaml",
        str(package / "run.yaml"),
        "--package",
        str(package),
        *(["--json"] if json_mode else []),
    ]


def _payload(capsys: pytest.CaptureFixture[str]) -> dict[str, Any]:
    captured = capsys.readouterr()
    return json.loads(captured.out.strip().splitlines()[-1])


@pytest.fixture()
def home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Изолированный ``COHOUND_HOME`` + CWD и чистый ``COHOUND_NODE``."""
    root = tmp_path / "home"
    monkeypatch.setenv("COHOUND_HOME", str(root))
    monkeypatch.delenv("COHOUND_NODE", raising=False)
    monkeypatch.chdir(tmp_path)
    return root


def test_overlapping_host_shards_are_refused(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """T9 (17 §7.1): пересечение — ``EXIT_USAGE``, сообщение называет host'ы **и** узлы.

    Через CLI, а не только через модель конфига: требование — «отказ при старте», то есть
    прогон не должен успеть начаться с двумя узлами на одном host'е.
    """
    _deploy_yaml(
        tmp_path,
        [("node-a", ["docs.test"]), ("node-b", ["other.test", "docs.test"])],
    )
    package = valid_package(tmp_path)

    code = main(["plan", *_common(package, json_mode=False)])

    assert code == EXIT_USAGE
    err = capsys.readouterr().err
    assert "docs.test" in err
    assert "node-a" in err and "node-b" in err


# --------------------------------------------------------------------------- #
# (3) узел берёт только свои host'ы: очередь, элементы сеанса, счётчик
# --------------------------------------------------------------------------- #


def test_node_takes_only_its_hosts(tmp_path: Path) -> None:
    """Узел A краулит корень и **не** ставит host'ы узла B; отказ виден числом."""
    store = SqliteStore(":memory:")
    try:
        crawler, result = _build(
            tmp_path,
            store,
            crawl_app=CrawlConfig(shards=SHARDS),
            node_id="node-a",
        )

        # Чужой host не фетчился и не попал в сеанс: ровно один запрос — корень.
        assert crawler.calls == [ROOT]
        rows = {row.url for row in store.iter_crawl_session_items(harness.SESSION_ID)}
        assert rows == {ROOT}
        assert {item.url for item in store.iter_extract_queue(harness.SESSION_ID)} == {
            ROOT
        }

        prov = _provenance(store)
        root = _root_record(prov)
        assert root["host_out_of_shard"]["count"] == len(HOSTS)
        assert set(root["host_out_of_shard"]["by_host"]) == set(HOSTS)
        assert root["truncated"] is True
        assert root["truncated_reason"] == HOST_OUT_OF_SHARD_REASON

        # Провенанс развёртывания: узел и его host'ы durable, а не только в памяти.
        assert prov["shard"]["node"] == "node-a"
        assert prov["shard"]["hosts"] == ["docs.test"]
        assert prov["shard"]["filtered"] is True
        assert prov["shard"]["filtered_products"] == ["p1"]
        assert prov["shard"]["reason"] == HOST_OUT_OF_SHARD_REASON
        assert result.errors == [] or all(
            "host_out_of_shard" in line for line in result.errors
        )
    finally:
        store.close()


def test_two_nodes_cover_the_declared_map_without_overlap(tmp_path: Path) -> None:
    """Два узла с одной картой берут **свои** seed-корни: наборы не пересекаются.

    Это и есть смысл статического шарда: эксклюзивность host'а по построению, без аренды
    и без договора между процессами.
    """
    seeds = [ROOT, *(f"https://{host}/" for host in HOSTS)]
    taken: dict[str, set[str]] = {}
    partials: dict[str, bool] = {}
    for node in ("node-a", "node-b"):
        store = SqliteStore(":memory:")
        try:
            crawler, _ = _build(
                tmp_path,
                store,
                urls={"p1": seeds},
                crawl_app=CrawlConfig(shards=SHARDS),
                node_id=node,
            )
            taken[node] = set(crawler.calls)
            partials[node] = _provenance(store)["shard"]["filtered"]
        finally:
            store.close()

    assert taken["node-a"] == {ROOT}
    assert taken["node-b"] == {f"https://{host}/" for host in HOSTS}
    assert not (taken["node-a"] & taken["node-b"])
    assert set().union(*taken.values()) == set(seeds)
    # Оба узла видят **часть** объявленного корпуса, и оба говорят об этом.
    assert partials == {"node-a": True, "node-b": True}


# --------------------------------------------------------------------------- #
# (4) честность: partial-сеанс, неполное покрытие, `❌` не публикуется
# --------------------------------------------------------------------------- #


def test_sharded_node_reports_partial_session_and_incomplete_coverage(
    tmp_path: Path,
) -> None:
    """Шардированный узел: сеанс ``partial``, покрытие ``complete=False`` с причиной.

    Иначе непокрытый host читался бы как ``exhaustive``, и ``❌`` публиковался бы из
    неполного корпуса (I2b/I2c).
    """
    store = SqliteStore(":memory:")
    try:
        _build(
            tmp_path,
            store,
            crawl_app=CrawlConfig(shards=SHARDS),
            node_id="node-a",
        )

        session = store.get_crawl_session(harness.SESSION_ID)
        assert session is not None
        assert session.status is CrawlSessionStatus.PARTIAL

        coverage = store.read_corpus_coverage(harness.RUN_ID, "p1")
        assert coverage is not None
        assert coverage.complete is False
        assert coverage.reason == HOST_OUT_OF_SHARD_REASON
        assert coverage.observed_sources == 1
    finally:
        store.close()


def test_absence_is_not_published_on_a_sharded_corpus(tmp_path: Path) -> None:
    """``❌`` на неполном (шардированном) корпусе понижается до ``❓`` — I2c исполняется.

    Класс ``bounded`` без принятия риска человеком не даёт права на ``❌``: часть материала
    принадлежит другому узлу, и «искали и не нашли» здесь недоказуемо. Понижение — ``❓``
    (I12), а не ``failed`` (I4): прибор отработал, охват не тот.
    """
    store = SqliteStore(":memory:")
    try:
        _build(
            tmp_path,
            store,
            crawl_app=CrawlConfig(shards=SHARDS),
            node_id="node-a",
        )
        coverage = store.read_corpus_coverage(harness.RUN_ID, "p1")
        assert coverage is not None

        scope = EvidenceScope.model_construct(
            denial_strength=DenialStrength.BOUNDED,
            reason="объявленный конечный справочник",
            enumerated=True,
            scope_acceptance=None,
        )
        status, demoted_scope, demoted, notes = _enforce_absence_coverage(
            out_status=Status.ABSENT,
            scope=scope,
            absence_evidence=[],
            param=harness._plan().params[0],
            product_id="p1",
            coverage=coverage,
        )

        assert status is Status.UNKNOWN
        assert demoted is True
        assert notes, "причина понижения обязана быть машинной записью"
        assert demoted_scope is not None
        assert demoted_scope.denial_strength is DenialStrength.NOT_STATED
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# (5) пустой шард и узел без карты — видимо, не тихо
# --------------------------------------------------------------------------- #


def test_empty_shard_is_visible_and_crawls_nothing(tmp_path: Path) -> None:
    """Пустой шард: ничего не краулится, но это **названо** (а не тихий простой)."""
    store = SqliteStore(":memory:")
    try:
        crawler, result = _build(
            tmp_path,
            store,
            crawl_app=CrawlConfig(shards=[CrawlShardConfig(node="node-idle", hosts=[])]),
            node_id="node-idle",
        )

        assert crawler.calls == []
        assert any("объявлен без host'ов" in line for line in result.errors), result.errors
        coverage = store.read_corpus_coverage(harness.RUN_ID, "p1")
        assert coverage is not None
        assert coverage.expected_sources == 0
        assert coverage.complete is False
        # Материал есть, он на другом узле — причина называется своим токеном, а не
        # общим `all_seeds_rejected`.
        assert coverage.reason == HOST_OUT_OF_SHARD_REASON
        session = store.get_crawl_session(harness.SESSION_ID)
        assert session is not None and session.status is CrawlSessionStatus.PARTIAL
    finally:
        store.close()


def test_node_without_declared_shards_is_named_not_silent(tmp_path: Path) -> None:
    """``--node`` без карты: не ошибка (узел может стоять на целом хосте), но факт назван."""
    store = SqliteStore(":memory:")
    try:
        crawler, result = _build(
            tmp_path,
            store,
            crawl_app=CrawlConfig(),
            node_id="node-a",
        )

        # Поведение прежнее (фильтра нет), но строка в артефакте фазы есть.
        assert len(crawler.calls) == 1 + len(HOSTS)
        assert any("crawl.shards не объявлены" in line for line in result.errors)
        prov = _provenance(store)
        assert "shard" not in prov
        session = store.get_crawl_session(harness.SESSION_ID)
        assert session is not None and session.status is CrawlSessionStatus.COMPLETED
    finally:
        store.close()


def test_shards_declared_without_node_is_a_usage_error(tmp_path: Path) -> None:
    """Карта объявлена, узел не выбран — отказ на входе (``ValueError`` → ``EXIT_USAGE``)."""
    store = SqliteStore(":memory:")
    try:
        with pytest.raises(ValueError, match=r"shards:.*узел не выбран"):
            _build(tmp_path, store, crawl_app=CrawlConfig(shards=SHARDS), node_id=None)
    finally:
        store.close()

    # Та же проверка на резолвере composition root'а: ``--node`` > ``COHOUND_NODE``.
    deploy = DeployConfig(crawl=CrawlConfig(shards=SHARDS))
    with pytest.raises(ValueError, match="shards:"):
        _node_id(_args(node=None), deploy, required=True)
    assert _node_id(_args(node="node-b"), deploy, required=True) == "node-b"
    # consume_only краула не имеет: узел не требуется и не выдумывается.
    assert _node_id(_args(node=None), deploy, required=False) is None


def _args(*, node: str | None) -> Any:
    from types import SimpleNamespace

    return SimpleNamespace(node=node)


def test_crawl_run_requires_node_when_shards_declared(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``crawl run`` при объявленной карте без ``--node`` — ``EXIT_USAGE`` с именами узлов.

    Молчаливый запуск был бы двумя процессами на одних host'ах: политика вежливости §10
    нарушилась бы без единой ошибки.
    """
    _deploy_yaml(
        tmp_path,
        [("node-a", ["docs.test"]), ("node-b", ["other.test"])],
    )
    package = valid_package(tmp_path)
    common = _common(package)

    code = main(["crawl", "run", *_common(package, json_mode=False), "--until", "plan"])
    assert code == EXIT_USAGE
    err = capsys.readouterr().err
    assert "shards" in err
    assert "node-a" in err and "node-b" in err

    # Объявленный узел — та же команда проходит.
    assert main(["crawl", "run", *common, "--until", "plan", "--node", "node-a"]) == (
        EXIT_OK
    )


# --------------------------------------------------------------------------- #
# (6) идентичность: карта в дайджесте, узел — нет
# --------------------------------------------------------------------------- #


def test_shard_map_changes_identity_but_node_does_not() -> None:
    """Карта шардов — условие измерения; выбранный узел — провенанс развёртывания.

    Иначе либо два разных корпуса жили бы под одним ``run_id`` (карта вне дайджеста),
    либо один прогон, нарезанный на шарды, стал бы N измерениями (узел в дайджесте).
    """
    plain = crawl_control_digest(
        run_crawl=CrawlRunConfig(), app=CrawlConfig(), profiles={}
    )
    sharded = crawl_control_digest(
        run_crawl=CrawlRunConfig(), app=CrawlConfig(shards=SHARDS), profiles={}
    )
    assert plain != sharded

    # Порядок объявления узлов — операторский, идентичность — канон: карта одна.
    reordered = crawl_control_digest(
        run_crawl=CrawlRunConfig(),
        app=CrawlConfig(shards=list(reversed(SHARDS))),
        profiles={},
    )
    assert reordered == sharded

    # Узел выбирается операционно и в дайджест не входит.
    first = resolve_host_shard(SHARDS, "node-a")
    second = resolve_host_shard(SHARDS, "node-b")
    assert first is not None and second is not None
    assert first.node != second.node
    assert (
        crawl_control_digest(
            run_crawl=CrawlRunConfig(),
            app=CrawlConfig(shards=SHARDS),
            profiles={},
        )
        == sharded
    )


def test_plan_run_id_keeps_anchor_without_shards_and_changes_with_them(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """``plan``: без карты ``run_id`` прежний, с картой — другой, карта одна — один.

    Якорь пакета (``ru-pg-dbms-2396d2a57455``) проверяется отдельной командой; здесь
    доказывается, что карта шардов **не** молчит в дайджесте и что порядок её объявления
    не создаёт второй ``run_id``.
    """
    package = valid_package(tmp_path)
    common = _common(package)

    assert main(["plan", *common]) == EXIT_OK
    plain = _payload(capsys)["run_id"]

    _deploy_yaml(tmp_path, [("node-a", ["docs.test"]), ("node-b", ["other.test"])])
    assert main(["plan", *common]) == EXIT_OK
    sharded = _payload(capsys)["run_id"]
    assert sharded != plain

    _deploy_yaml(tmp_path, [("node-b", ["other.test"]), ("node-a", ["docs.test"])])
    assert main(["plan", *common]) == EXIT_OK
    assert _payload(capsys)["run_id"] == sharded


def test_plan_names_an_empty_shard_before_the_run(
    tmp_path: Path, home: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Пустой шард виден **до** старта: ``plan`` печатает его в предупреждениях.

    Иначе «узел объявлен, host'ов ноль» выяснялось бы только из отсутствия артефактов
    (или из ``errors`` фазы, если узел вообще выбран).
    """
    _deploy_yaml(tmp_path, [("node-a", ["docs.test"]), ("node-idle", [])])
    package = valid_package(tmp_path)

    assert main(["plan", *_common(package)]) == EXIT_OK
    payload = _payload(capsys)
    assert any(
        "node-idle" in warning and "объявлен без host'ов" in warning
        for warning in payload.get("warnings", [])
    ), payload.get("warnings")


# --------------------------------------------------------------------------- #
# (7) consume_only: краула нет — узел не нужен, честность восстанавливается
# --------------------------------------------------------------------------- #


def test_consume_only_needs_no_node_and_restores_shard_reason(tmp_path: Path) -> None:
    """Конверсия шардированного корпуса: без ``node_id``, причины неполноты — из сеанса.

    Краула в этом исполнении нет, права на host не спрашиваются; но «полно» не
    объявляется: ``truncated``/``host_out_of_shard`` — durable-факт сеанса.
    """
    store = SqliteStore(":memory:")
    try:
        _build(
            tmp_path,
            store,
            crawl_app=CrawlConfig(shards=SHARDS),
            node_id="node-a",
        )
        crawler, result = _build(
            tmp_path,
            store,
            urls={"p1": []},
            crawl_app=CrawlConfig(shards=SHARDS),
            node_id=None,
            crawl_only=False,
            consume_only=True,
            consume_worker="worker-b",
        )
        assert crawler.calls == []  # retained-байты: живого фетча нет

        coverage = store.read_corpus_coverage(harness.RUN_ID, "p1")
        assert coverage is not None
        assert coverage.complete is False
        assert coverage.reason == HOST_OUT_OF_SHARD_REASON
        docs = list(store.iter_corpus_docs(harness.RUN_ID))
        assert {doc.url for doc in docs} == {ROOT}
        assert result.status == "ok", result.errors
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# (8) T10: смерть узла оставляет восстановимую очередь
# --------------------------------------------------------------------------- #


def test_killed_node_leaves_resumable_queue(tmp_path: Path) -> None:
    """T10 (17 §7.1): обрыв узла не теряет и не удваивает документы.

    Сценарий: строки взяты в аренду мёртвым узлом (``claimed``, heartbeat в прошлом),
    сеанс остался ``running``. Recovery-on-open переводит сеанс в ``interrupted`` (не
    ``failed``: тело краула не финализировано), ``reclaim_stale`` возвращает строки в
    ``pending`` с ``attempts += 1``, и повторная конверсия даёт **по одному** документу на
    строку. Новых лизов/динамики здесь нет — механизм F6 проверяется на этом сценарии.
    """
    store = SqliteStore(":memory:")
    try:
        crawler, _ = _build(
            tmp_path,
            store,
            crawl_app=CrawlConfig(),
            node_id=None,
        )
        expected_urls = set(crawler.calls)
        assert len(expected_urls) == 1 + len(HOSTS)

        queue = ExtractQueue(store=store, session_id=harness.SESSION_ID)
        rows = list(queue.items("p1"))
        assert len(rows) == len(expected_urls)
        for row in rows:
            assert queue.claim(row, "dead-node") is True
        # Узел мёртв: аренда не продлевается, метки остались в прошлом.
        with store.conn:
            store.conn.execute(
                "UPDATE extract_queue SET claimed_at = ?, heartbeat_at = ?"
                " WHERE session_id = ?",
                ("2000-01-01T00:00:00+00:00", "2000-01-01T00:00:00+00:00", harness.SESSION_ID),
            )
        # Сеанс мёртвого узла: `running` без свежего heartbeat → `interrupted` при открытии.
        store.update_crawl_session_status(harness.SESSION_ID, CrawlSessionStatus.RUNNING)
        with store.conn:
            store.conn.execute(
                "UPDATE crawl_sessions SET heartbeat_at = ? WHERE session_id = ?",
                ("2000-01-01T00:00:00+00:00", harness.SESSION_ID),
            )
        recovered = recover_interrupted_sessions(
            store,
            now=_dt.datetime(2030, 1, 1, tzinfo=_dt.UTC),
            owner_alive=lambda _owner: False,
        )
        assert recovered == [harness.SESSION_ID]
        session = store.get_crawl_session(harness.SESSION_ID)
        assert session is not None and session.status is CrawlSessionStatus.INTERRUPTED

        reclaimed = queue.reclaim_stale(
            now="2030-01-01T00:00:00+00:00", lease_s=60
        )
        assert sorted(record["url"] for record in reclaimed) == sorted(expected_urls)
        assert {record["claimed_by"] for record in reclaimed} == {"dead-node"}
        assert store.count_extract_queue(harness.SESSION_ID) == {
            "pending": len(expected_urls)
        }

        # Перезапуск шарда: конверсия берёт восстановленные строки и не теряет ни одной.
        crawler2, result = _build(
            tmp_path,
            store,
            urls={"p1": []},
            crawl_app=CrawlConfig(),
            node_id=None,
            crawl_only=False,
            consume_only=True,
            consume_worker="worker-b",
        )
        assert crawler2.calls == []
        docs = list(store.iter_corpus_docs(harness.RUN_ID))
        assert {doc.url for doc in docs} == expected_urls
        assert len(docs) == len(expected_urls)  # ни один документ не посчитан дважды
        assert store.count_extract_queue(harness.SESSION_ID) == {
            "done": len(expected_urls)
        }
        assert result.status == "ok", result.errors
    finally:
        store.close()