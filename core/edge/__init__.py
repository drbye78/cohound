"""coHound — edge (Z1/DMZ): composition root веб-функций + gateway-сервер шва.

Содержит только веб-адаптеры за application-layer швом (07): край **не** получает
доступ к ``store``/``domain`` (нет веб-эгрессии в ядре, I18/I19). Ядро инициирует
``/v1/*``; edge отвечает. Нейтрален (§1.3).
"""

__all__ = ["server", "seam_schema", "auth", "audit"]
