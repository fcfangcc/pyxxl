from enum import Enum


class executorBlockStrategy(Enum):
    SERIAL_EXECUTION = "SERIAL_EXECUTION"  # 单机串行
    DISCARD_LATER = "DISCARD_LATER"  # 丢弃后续调度
    COVER_EARLY = "COVER_EARLY"  # 关闭上次执行改为这次执行，不推荐
