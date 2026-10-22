"""Политика спуска в контейнеры: ``core/util/container_policy.py`` (срез B2).

Проверяется всё, что решается **по индексу**, без чтения тел и без бэкендов:

* нормализация пути члена — обход пространства имён архива невозможен
  (абсолютный путь, ``..``, буква диска, NUL, пустое имя, ``..\\..\\`` из Windows-zip);
* ingestible-суффиксы — из **реестра** форматов плюс текстовые цели спуска;
* каждая причина пропуска из словаря видимых пропусков;
* детерминированный порядок и представитель = наименьший нормализованный путь;
* bomb-guard: заявленный потолок урезает план (``truncated`` + причины), а фактические
  байты ловит ``bounded_iter`` (второй гейт).

Данные нейтральны (§1.3): пути, размеры, суффиксы.
"""

from __future__ import annotations

import pytest

from core.ports.container import ContainerListing, ContainerMember
from core.util.container_policy import (
    CAPACITY_REASONS,
    INGEST_SUFFIXES,
    SKIP_ENCRYPTED,
    SKIP_IS_DIRECTORY,
    SKIP_MEMBER_LIMIT,
    SKIP_MEMBER_TOO_LARGE,
    SKIP_NOT_INGESTIBLE,
    SKIP_PATH_REJECTED,
    SKIP_TOTAL_EXCEEDED,
    MemberTooLarge,
    archive_forecast,
    bounded_iter,
    is_ingestible_member,
    normalize_member_path,
    plan_members,
)
from core.util.document_formats import DOCUMENT_SUFFIXES


def _member(
    path: str,
    size: int = 10,
    *,
    compressed: int | None = None,
    crc: int | None = None,
    is_dir: bool = False,
) -> ContainerMember:
    """Член контейнера для тестов (заявленные размеры — как в индексе)."""
    return ContainerMember(
        path=path,
        size=size,
        compressed_size=compressed,
        crc32=crc,
        is_dir=is_dir,
    )


def _listing(
    *members: ContainerMember,
    format: str = "zip",
    solid: bool | None = False,
    encrypted: bool = False,
    truncated: bool = False,
    unreadable: str | None = None,
) -> ContainerListing:
    """Индекс контейнера для тестов."""
    return ContainerListing(
        format=format,
        members=tuple(members),
        solid=solid,
        encrypted=encrypted,
        truncated=truncated,
        unreadable=unreadable,
    )


# --------------------------------------------------------------------------- #
# normalize_member_path — член не выходит за пространство имён архива
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "raw",
    [
        "/etc/passwd",
        "/",
        "../secret.pdf",
        "a/../../b.pdf",
        "..",
        "a/..",
        "C:/Windows/x.pdf",
        "c:x.pdf",
        "a\x00b.pdf",
        "\x00",
        "",
        "   ",
        "./",
        "..\\..\\windows\\x.pdf",
        "\\\\server\\share\\x.pdf",
    ],
)
def test_normalize_rejects_escape(raw: str) -> None:
    """Абсолютный путь, ``..``, диск, NUL и пустое имя — не имя члена."""
    assert normalize_member_path(raw) is None


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("a.pdf", "a.pdf"),
        ("docs/a.pdf", "docs/a.pdf"),
        ("a//b.pdf", "a/b.pdf"),
        ("./a/./b.pdf", "a/b.pdf"),
        ("docs\\a.pdf", "docs/a.pdf"),
        ("dir/", "dir"),
        ("Отчёт 2026.pdf", "Отчёт 2026.pdf"),
        (".hidden", ".hidden"),
    ],
)
def test_normalize_keeps_legal_paths(raw: str, expected: str) -> None:
    """Легальные пути нормализуются, но не теряют смысла."""
    assert normalize_member_path(raw) == expected


def test_normalize_is_idempotent() -> None:
    """Повторная нормализация не меняет результат (идентичность члена стабильна)."""
    once = normalize_member_path("./docs//a.pdf")
    assert once is not None
    assert normalize_member_path(once) == once


# --------------------------------------------------------------------------- #
# is_ingestible_member — суффиксы из реестра, не вторая копия списка
# --------------------------------------------------------------------------- #


def test_ingest_suffixes_come_from_registry() -> None:
    """Документная часть набора — ровно реестр форматов (без второй копии)."""
    for suffix in DOCUMENT_SUFFIXES:
        assert suffix in INGEST_SUFFIXES


@pytest.mark.parametrize("suffix", DOCUMENT_SUFFIXES)
def test_registry_suffixes_are_ingestible(suffix: str) -> None:
    """Каждый формат реестра принимается спуском."""
    assert is_ingestible_member(f"docs/report{suffix}")


@pytest.mark.parametrize(
    "path", ["readme.md", "notes.txt", "page.html", "page.htm", "docs/a.MARKDOWN"]
)
def test_text_targets_are_ingestible(path: str) -> None:
    """Текстовые цели спуска (markdown/txt/html) принимаются."""
    assert is_ingestible_member(path)


@pytest.mark.parametrize("path", ["app.exe", "setup.msi", "data.bin", "README", "a.tar.gz"])
def test_non_ingestible_formats(path: str) -> None:
    """Исполняемые/бинарные/безсуффиксные члены — не документы корпуса."""
    assert not is_ingestible_member(path)


def test_ingestible_suffixes_are_configurable() -> None:
    """Набор суффиксов — параметр (дефолт из реестра, не единственная истина)."""
    assert not is_ingestible_member("a.csv")
    assert is_ingestible_member("a.csv", (".csv",))
    assert is_ingestible_member("a.PDF", ("pdf",))


# --------------------------------------------------------------------------- #
# plan_members — по причине на каждый пропуск
# --------------------------------------------------------------------------- #


def test_plan_accepts_ingestible_members() -> None:
    """Принятые члены сохраняют сырой путь (его ждёт ``open_member``) и размеры."""
    listing = _listing(_member("docs/b.pdf", 30), _member("a.txt", 5))
    plan = plan_members(listing)
    assert [m.path for m in plan.accepted] == ["a.txt", "docs/b.pdf"]
    assert plan.total_uncompressed == 35
    assert plan.skipped == ()
    assert plan.truncated is False
    assert plan.representative == "a.txt"


def test_plan_reason_is_directory() -> None:
    """Каталог — не член: причина ``is_directory`` (даже если имя похоже на документ)."""
    plan = plan_members(_listing(_member("docs/", 0, is_dir=True), _member("docs/a.pdf")))
    assert plan.skipped == (("docs", SKIP_IS_DIRECTORY),)
    assert [m.path for m in plan.accepted] == ["docs/a.pdf"]


def test_plan_reason_path_rejected_keeps_raw_path() -> None:
    """Отвергнутый путь показывается сырым: нормализовать его нельзя."""
    plan = plan_members(_listing(_member("../../etc/passwd.pdf", 10)))
    assert plan.skipped == (("../../etc/passwd.pdf", SKIP_PATH_REJECTED),)
    assert plan.accepted == ()


def test_plan_reason_encrypted_covers_every_member() -> None:
    """Шифрование — свойство контейнера: ни один член не принимается."""
    listing = _listing(_member("a.pdf"), _member("b.txt"), encrypted=True)
    plan = plan_members(listing)
    assert plan.accepted == ()
    assert {reason for _, reason in plan.skipped} == {SKIP_ENCRYPTED}
    assert len(plan.skipped) == 2


def test_plan_reason_not_ingestible() -> None:
    """Не тот формат — причина ``not_ingestible_format``."""
    plan = plan_members(_listing(_member("a.pdf"), _member("tool.exe")))
    assert plan.skipped == (("tool.exe", SKIP_NOT_INGESTIBLE),)
    assert [m.path for m in plan.accepted] == ["a.pdf"]


def test_plan_reason_member_too_large_and_truncated() -> None:
    """Член сверх потолка не принимается и урезает план (``truncated``)."""
    plan = plan_members(
        _listing(_member("big.pdf", 100), _member("small.pdf", 10)),
        max_member_bytes=50,
    )
    assert plan.skipped == (("big.pdf", SKIP_MEMBER_TOO_LARGE),)
    assert [m.path for m in plan.accepted] == ["small.pdf"]
    assert plan.truncated is True


def test_plan_reason_member_limit() -> None:
    """Исчерпанный потолок числа членов — причина ``member_limit_exceeded``."""
    plan = plan_members(
        _listing(_member("a.pdf"), _member("b.pdf"), _member("c.pdf")),
        max_members=1,
    )
    assert [m.path for m in plan.accepted] == ["a.pdf"]
    assert plan.skipped == (
        ("b.pdf", SKIP_MEMBER_LIMIT),
        ("c.pdf", SKIP_MEMBER_LIMIT),
    )
    assert plan.truncated is True


def test_plan_bomb_guard_declared_total() -> None:
    """Заявленная сумма сверх потолка: часть принята, остальное — причины + ``truncated``."""
    listing = _listing(
        _member("a.pdf", 60),
        _member("b.pdf", 60),
        _member("c.pdf", 60),
    )
    plan = plan_members(listing, max_total_uncompressed=100)
    assert [m.path for m in plan.accepted] == ["a.pdf"]
    assert plan.total_uncompressed == 60
    assert plan.skipped == (
        ("b.pdf", SKIP_TOTAL_EXCEEDED),
        ("c.pdf", SKIP_TOTAL_EXCEEDED),
    )
    assert plan.truncated is True


def test_plan_non_capacity_skips_do_not_truncate() -> None:
    """Пропуски «по природе» (каталог/формат/шифрование) план не урезают."""
    plan = plan_members(
        _listing(
            _member("docs/", 0, is_dir=True),
            _member("app.exe"),
            _member("a.pdf"),
        )
    )
    assert plan.truncated is False
    assert {reason for _, reason in plan.skipped} <= {
        SKIP_IS_DIRECTORY,
        SKIP_NOT_INGESTIBLE,
    }
    # Бюджетные причины — ровно три оси: всё остальное «по природе».
    assert CAPACITY_REASONS == {
        SKIP_MEMBER_TOO_LARGE,
        SKIP_MEMBER_LIMIT,
        SKIP_TOTAL_EXCEEDED,
    }


def test_plan_propagates_listing_truncation() -> None:
    """Неполный индекс (``listing.truncated``) урезает и план."""
    plan = plan_members(_listing(_member("a.pdf"), truncated=True))
    assert plan.truncated is True
    assert plan.accepted  # состав, который индекс успел отдать, всё равно планируется


def test_plan_order_is_deterministic_and_sorted_by_normalized_path() -> None:
    """Порядок не зависит от порядка записей в архиве (сортировка по норм. пути)."""
    members = [
        _member("zeta.pdf"),
        _member("docs/b.pdf"),
        _member("./docs/a.pdf"),
        _member("Beta.pdf"),
    ]
    forward = plan_members(_listing(*members))
    backward = plan_members(_listing(*reversed(members)))
    assert [m.path for m in forward.accepted] == [m.path for m in backward.accepted]
    normalized = [normalize_member_path(m.path) or "" for m in forward.accepted]
    assert normalized == sorted(normalized)
    assert [m.path for m in forward.accepted] == [
        "Beta.pdf",
        "./docs/a.pdf",
        "docs/b.pdf",
        "zeta.pdf",
    ]


def test_plan_representative_is_lowest_normalized_path() -> None:
    """Представитель — наименьший нормализованный путь среди принятых."""
    plan = plan_members(_listing(_member("b.pdf"), _member("./a.pdf"), _member("c.pdf")))
    assert plan.representative == "a.pdf"
    assert plan.representative == normalize_member_path(plan.accepted[0].path)


def test_plan_representative_is_none_without_accepted() -> None:
    """Нет принятых — нет представителя."""
    plan = plan_members(_listing(_member("app.exe")))
    assert plan.representative is None


def test_plan_representative_ignores_rejected_and_skipped() -> None:
    """Представитель не может быть отвергнутым/пропущенным членом."""
    plan = plan_members(
        _listing(_member("../x.pdf"), _member("app.exe"), _member("docs/a.pdf"))
    )
    assert plan.representative == "docs/a.pdf"


def test_plan_reasons_cover_whole_taxonomy() -> None:
    """Все причины словаря наблюдаемы (нет мёртвых ветвей политики)."""
    plan = plan_members(
        _listing(
            _member("dir/", 0, is_dir=True),
            _member("../x.pdf"),
            _member("a.exe"),
            _member("big.pdf", 1000),
            _member("b.pdf", 10),
            _member("c.pdf", 10),
        ),
        max_member_bytes=100,
        max_members=1,
    )
    reasons = {reason for _, reason in plan.skipped}
    assert reasons == {
        SKIP_IS_DIRECTORY,
        SKIP_PATH_REJECTED,
        SKIP_NOT_INGESTIBLE,
        SKIP_MEMBER_TOO_LARGE,
        SKIP_MEMBER_LIMIT,
    }
    encrypted = plan_members(_listing(_member("a.pdf"), encrypted=True))
    assert {reason for _, reason in encrypted.skipped} == {SKIP_ENCRYPTED}


# --------------------------------------------------------------------------- #
# archive_forecast — форма записи до извлечения
# --------------------------------------------------------------------------- #


def test_forecast_shape() -> None:
    """Прогноз несёт состав, гистограмму, причины пропусков и потолки-факты."""
    listing = _listing(
        _member("docs/a.pdf", 100, compressed=50, crc=1),
        _member("docs/b.pdf", 20),
        _member("notes.txt", 5),
        _member("tool.exe", 7),
        solid=False,
    )
    plan = plan_members(listing, max_member_bytes=50)
    forecast = archive_forecast(listing, plan)
    assert forecast == {
        "format": "zip",
        "members_total": 4,
        "members_accepted": 2,
        "formats": {".pdf": 1, ".txt": 1},
        "total_uncompressed": 25,
        "solid": False,
        "encrypted": False,
        "skipped": {
            "member_too_large": {"count": 1, "examples": ["docs/a.pdf"]},
            "not_ingestible_format": {"count": 1, "examples": ["tool.exe"]},
        },
        "truncated": True,
        "representative": "docs/b.pdf",
        "unreadable": None,
    }


def test_forecast_is_json_serializable() -> None:
    """Запись обязана лечь в ``archive_observations`` без конвертеров."""
    import json

    listing = _listing(_member("a.pdf"))
    forecast = archive_forecast(listing, plan_members(listing))
    assert json.loads(json.dumps(forecast)) == forecast


def test_forecast_examples_are_bounded() -> None:
    """Счётчик полный, примеры ограничены (как ``artifacts_outside_prefix``)."""
    members = [_member(f"f{i:02d}.exe") for i in range(30)]
    listing = _listing(*members)
    forecast = archive_forecast(listing, plan_members(listing))
    bucket = forecast["skipped"][SKIP_NOT_INGESTIBLE]
    assert bucket["count"] == 30
    assert len(bucket["examples"]) < 30
    assert bucket["examples"] == sorted(bucket["examples"])


def test_forecast_keeps_unreadable_reason() -> None:
    """Нечитаемый контейнер объясняется, а не выглядит «пустым архивом»."""
    listing = _listing(format="7z", solid=None, unreadable="backend_unavailable: py7zr ...")
    forecast = archive_forecast(listing, plan_members(listing))
    assert forecast["members_total"] == 0
    assert forecast["unreadable"] == "backend_unavailable: py7zr ..."
    assert forecast["solid"] is None


def test_forecast_encrypted_container() -> None:
    """Шифрованный контейнер: все члены в одной причине, план не урезан."""
    listing = _listing(_member("a.pdf"), _member("b.pdf"), encrypted=True)
    forecast = archive_forecast(listing, plan_members(listing))
    assert forecast["encrypted"] is True
    assert forecast["members_accepted"] == 0
    assert forecast["skipped"][SKIP_ENCRYPTED]["count"] == 2
    assert forecast["truncated"] is False


# --------------------------------------------------------------------------- #
# bounded_iter — второй гейт bomb-guard
# --------------------------------------------------------------------------- #


def test_bounded_iter_passes_within_limit() -> None:
    """В пределах потолка поток отдаётся как есть."""
    assert list(bounded_iter([b"ab", b"cd"], 4)) == [b"ab", b"cd"]


def test_bounded_iter_raises_on_excess() -> None:
    """Пробитие потолка — типизированный ``MemberTooLarge`` (не тихое усечение)."""
    with pytest.raises(MemberTooLarge) as excinfo:
        list(bounded_iter([b"ab", b"cd"], 3))
    assert excinfo.value.limit == 3
    assert excinfo.value.consumed == 4


def test_bounded_iter_does_not_yield_overflowing_chunk() -> None:
    """Пробивший чанк не отдаётся: частичное тело выглядело бы как успех."""
    seen: list[bytes] = []
    with pytest.raises(MemberTooLarge):
        for chunk in bounded_iter([b"aaa", b"bbb"], 4):
            seen.append(chunk)
    assert seen == [b"aaa"]


def test_bounded_iter_is_lazy() -> None:
    """Итератор ленив: без потребления источник не читается (нет префетча)."""
    consumed: list[int] = []

    def source():
        for i in range(3):
            consumed.append(i)
            yield b"x"

    iterator = bounded_iter(source(), 10)
    assert consumed == []
    next(iterator)
    assert consumed == [0]


def test_bounded_iter_rejects_negative_limit() -> None:
    """Отрицательный потолок — ошибка вызова, а не «всё слишком большое»."""
    with pytest.raises(ValueError):
        list(bounded_iter([b"x"], -1))


def test_bounded_iter_zero_limit_allows_empty_stream() -> None:
    """Потолок 0 допустим: пустой член проходит, любой байт — отказ."""
    assert list(bounded_iter([], 0)) == []
    with pytest.raises(MemberTooLarge):
        list(bounded_iter([b"x"], 0))
