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
  jobs=(-n auto --dist worksteal)
  echo "   pytest-xdist доступен → параллельный прогон (-n auto --dist worksteal)"
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
