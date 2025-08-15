from enum import Enum


class ProxyType(Enum):
    HTTP   = 'http'
    HTTPS  = 'https'
    SOCKS4 = 'socks4'
    SOCKS5 = 'socks5'

