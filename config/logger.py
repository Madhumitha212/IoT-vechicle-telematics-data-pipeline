from loguru import logger

logger.remove()

logger.add(
    "logs/telemetry.log",
    rotation="50 MB",
    level="INFO",
    format="{time} | {level} | {name}:{function}:{line} | {message} | {extra}",
)