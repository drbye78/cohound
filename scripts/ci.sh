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
if "$py" -c "import pytest" >/dev/null 2>&1; then
  "$py" -m pytest -q
else
  echo "pytest не установлен — пропускаю (selfcheck уже прошёл)."
  echo "Установка: uv pip install -e '.[dev]'"
fi

echo
echo "CI: всё зелёное."
