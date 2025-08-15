import random
from os import path
from urllib.parse import urlparse, unquote
from threading import Timer
from requests import Session as ClientSession

from core.proxies.ProxyServer import ProxyServer
from core.proxies.ProxyType import ProxyType
from core.proxies.ProxyStatus import ProxyStatus
from core.logs import logger as log


class ProxiesPool:
    def __init__(self, file_path: str = path.dirname(path.abspath(__file__)) + '/proxies.txt'):
        self.proxy_pool = []
        self.reachable_proxy_pool = []
        self.enabled = True
        self.load_from_file(file_path)
        self.local_proxy = ProxyServer('localhost')

    def load_from_file(self, file_path):
        with open(file_path, 'r') as file:
            for line in file:
                try:
                    url = urlparse(line.strip())
                    proxy_type = ProxyType(url.scheme)
                    username = unquote(url.username) if url.username else None
                    password = unquote(url.password) if url.password else None
                    host = url.hostname
                    port = url.port
                    self.proxy_pool.append(ProxyServer(host, port, username, password, proxy_type))
                except Exception:
                    log.error(f'Ошибка добавления прокси из строки "{line}"')

    async def refresh(self, session: ClientSession, urls_pool: list[str] = None):
        if not self.enabled:
            return
        log.info(f'Обновление пула прокси-серверов ({len(self.proxy_pool)})')
        self.reachable_proxy_pool = []
        for proxy in self.proxy_pool:
            if await proxy.check_connection(session, urls_pool) is ProxyStatus.REACHABLE:
                self.reachable_proxy_pool.append(proxy)
        log.info(f'Пул прокси обновлен. Доступных серверов: {len(self)}/{len(self.proxy_pool)}')
        if len(self) == 0:
            log.critical(f'Пул прокси пуст. Переключение на резервный')

    def get_random_proxy(self) -> ProxyServer:
        if self.reachable_proxy_pool and self.enabled :
            return random.choice(self.reachable_proxy_pool) 
        else:
            Timer(20.0, self.get_random_proxy).start()

    def activate_server(self, proxy: ProxyServer):
        self.reachable_proxy_pool.append(proxy)

    def disable(self, proxy : ProxyServer):
        proxy.disable()
        self.reachable_proxy_pool.remove(proxy)
        
        Timer(interval=20.0, function=self.activate_server, kwargs=[self, proxy]).start()

    def __repr__(self):
        return f'ProxiesPool<{[proxy.status for proxy in self.proxy_pool]}>'

    def __len__(self):
        return len(self.reachable_proxy_pool)
