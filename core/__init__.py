"""coHound — ядро (категорийно-нейтральный код).

Ядро не содержит терминологии предметных категорий (§1.3). Все подпакеты
структурно соответствуют целевому расщеплению (§7 регламента / ADR-0012):
domain · ports · pipeline · adapters · store · config · cli · util.
"""

__all__ = ["schemas", "db", "util", "adapters", "ports", "domain", "store", "pipeline", "config", "cli"]
