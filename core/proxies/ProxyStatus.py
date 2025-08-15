from enum import Enum


class ProxyStatus(Enum):
    UNKNOWN     = 'unknown'
    REACHABLE   = 'reachable'
    UNREACHABLE = 'unreachable'
