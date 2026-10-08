# e2e fixture package

Минимальный категорийно-нейтральный пакет для сквозного walking-skeleton e2e:
один модуль (`m1`), одна роль (`role_r1`), один продукт (`p1`, он же якорь) и один
параметр (`feature.alpha`, `fact`, absence-правило `explicit`).

Фикстура существует **только** для локального прогона (без сети и GPU): её наполняют
in-memory заглушки из `tests/fakes/ports.py`. Настоящие продуктовые данные здесь
намеренно отсутствуют — идентификаторы нейтральны (§1.3 регламента).

Состав: `run.yaml`, `package.yaml`, `catalog.csv`, `baselines.csv`, `cohort.yaml`,
`golden.csv`. Обязательные артефакты раскладки пакета (`03` §A.1); `domain_profiles.yaml`
и `scope_attestation.yaml` не требуются.
