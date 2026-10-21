#!/usr/bin/env bash
# coHound — единая точка входа CI (§11.6). Провайдер-нейтрально.
#
# Запуск (из корня репозитория):
#   bash scripts/ci.sh
#
# Гейты: автономный selfcheck (словарь/схемы/DDL/инварианты + импорт-граф §1 + нейтральность
# §1.3) → pytest с порогом покрытия ядра (W1/T1.5). Ненулевой код = провал сборки.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

py="${PYTHON:-python3}"

echo "== selfcheck (автономный, без pytest) =="
"$py" tests/selfcheck.py

echo
echo "== pytest + coverage ядра (порог в pyproject.toml) =="
# Параллельный прогон, если доступен pytest-xdist; иначе — последовательно
# (окружение минимально и провайдер-нейтрально, поэтому деградация мягкая).
# Ограничение BLAS-потоков: тесты работают на малых массивах (индексы/векторы),
# многопоточность не нужна, а под xdist она ведёт к переподписке ядер.
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 NUMEXPR_NUM_THREADS=1
jobs=()
if "$py" -c "import xdist" >/dev/null 2>&1; then
  # Число воркеров xdist ограничено **памятью**, а не ядрами: `-n auto` берёт `nproc`
  # (12 на дефолтном хосте), и прогон упирается в OOM/ресурсные флаки раньше, чем в CPU.
  # Замер: 4 воркера ≈ 0.6 GiB RSS каждый, полный набор 3496 тестов за ~74 s; при
  # `-n auto` на 6.9 GiB наблюдалась флака `test_edge_launcher[forkserver]` (в изоляции
  # проходит). `COHOUND_CI_WORKERS=N` — переопределение; `COHOUND_CI_WORKERS=auto` —
  # вернуть прежнее поведение (осмысленно только на машине с запасом RAM).
  workers="${COHOUND_CI_WORKERS:-}"
  if [ -z "$workers" ]; then
    cores="$("$py" -c 'import os; print(os.cpu_count() or 1)')"
    if [ "$cores" -gt 4 ]; then workers=4; else workers="$cores"; fi
  fi
  if [ "$workers" = "auto" ]; then
    jobs=(-n auto --dist worksteal)
    echo "   pytest-xdist: COHOUND_CI_WORKERS=auto → -n auto (нужен запас RAM)"
  else
    jobs=(-n "$workers" --dist worksteal)
    echo "   pytest-xdist: $workers воркер(ов) (-n $workers --dist worksteal)"
  fi
else
  echo "   pytest-xdist не установлен → последовательный прогон"
fi
if "$py" -c "import pytest" >/dev/null 2>&1; then
  if "$py" -c "import pytest_cov" >/dev/null 2>&1; then
    "$py" -m pytest -q ${jobs[@]+"${jobs[@]}"} --cov=core --cov-report=term-missing
  else
    echo "pytest-cov не установлен — запускаю pytest без порога покрытия."
    "$py" -m pytest -q ${jobs[@]+"${jobs[@]}"}
  fi
else
  echo "pytest не установлен — пропускаю (selfcheck уже прошёл)."
  echo "Установка: uv sync --frozen --extra dev"
fi

echo
echo "CI: всё зелёное."
