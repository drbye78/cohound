"""Пробник CUDA-устройства — **один** ответ на вопрос «есть ли карта» (P9/R5).

Зачем отдельный модуль, а не две копии. Вопрос «доступно ли CUDA-устройство и сколько у
него памяти» задают три места: ядро (окно воркеров и preflight `crawl.pdf_device`,
`core/pipeline/resources.py`), composition root (`core/cli/wiring.py`) и DMZ
(`core/edge/extract_service.py`). Две копии одной пробы расходятся молча: ядро и шов
начали бы по-разному решать исполнимость **одного объявленного входа**, и отказ на одной
стороне сочетался бы с тихой работой на другой. Поэтому реализация одна, а вызывающие
делегируют.

Почему `util`, а не `pipeline`. Матрица зависимостей слоёв (`tests/selfcheck.py::
_import_graph`) не позволяет `edge → pipeline` и `adapters → pipeline`: DMZ обязан не
зависеть от конвейера. `util` разрешён всем слоям, поэтому общая проба живёт здесь, а не
там, где исторически появилась.

Импорт `torch` — **ленивый** и только внутри функций: он стоит сотни MB RSS и тянет весь
стек моделей, а нужен лишь тогда, когда оператор объявил `cuda`. Отсутствие пакета или
ошибка драйвера — честный ``False``/``None``, а не исключение: решение «отказ или работа»
принимает вызывающий (R5: `cuda` при недоступном устройстве — `EXIT_USAGE`, не тихий
CPU-фолбэк).
"""

from __future__ import annotations

__all__ = ["cuda_available", "cuda_total_vram_mb"]

def cuda_available() -> bool:
    """Доступно ли CUDA-устройство — **ленивый** импорт torch (не на старте процесса).

    Отсутствие torch/драйвера — ``False``, а не исключение: это факт окружения, а не
    сбой программы.
    """
    try:  # тяжёлая необязательная зависимость
        import torch
    except Exception:  # noqa: BLE001 — нет пакета/битые библиотеки: устройство недоступно
        return False
    try:
        return bool(torch.cuda.is_available())
    except Exception:  # noqa: BLE001 — драйвер отвечает ошибкой: устройство недоступно
        return False

def cuda_total_vram_mb() -> int | None:
    """Полный объём видеопамяти устройства 0, MB; ``None`` — устройства нет.

    Источник — ``torch.cuda.get_device_properties(0).total_memory`` (факт устройства), а
    не свободная память: свободная меняется от чужих процессов, и окно воркеров не может
    зависеть от того, открыт ли у соседа браузер. Резерв вычитается отдельно
    (:func:`core.pipeline.resources.read_usable_vram_mb`), поэтому формула называет обе
    величины.
    """
    try:  # тяжёлая необязательная зависимость
        import torch
    except Exception:  # noqa: BLE001 — нет пакета
        return None
    try:
        if not bool(torch.cuda.is_available()):
            return None
        total = int(torch.cuda.get_device_properties(0).total_memory)
    except Exception:  # noqa: BLE001 — драйвер/инициализация отвечают ошибкой
        return None
    return total // (1024 * 1024)
