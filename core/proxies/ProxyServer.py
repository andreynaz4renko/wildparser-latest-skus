from __future__ import annotations
from asyncio import gather
from requests import Session as ClientSession

from core.proxies.ProxyType import ProxyType
from core.proxies.ProxyStatus import ProxyStatus
from core.logs import logger as log


class ProxyServer:
    def __init__(
            self,
            host: str,
            port: int | None = None,
            username: str | None = None,
            password: str | None = None,
            proxy_type: ProxyType = ProxyType.HTTP
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.proxy_type = proxy_type
        self.status = ProxyStatus.UNKNOWN

    def as_string(self) -> dict | None:
        if self.username:
            res = {}
            res[self.proxy_type.value]= f"{self.proxy_type.value}://{self.username}:{self.password}@{self.host}:{self.port}"
            return res
        if self.port:
            res = {}
            res[self.proxy_type.value]=  f"{self.proxy_type.value}://{self.host}:{self.port}"
            return res
        if self.host == '127.0.0.1' or self.host == 'localhost' and self.port is None:
            return None
        res = {}
        res[self.proxy_type.value]= f"{self.proxy_type.value}://{self.host}"
        return res

    def disable(self):
        self.status = ProxyStatus.UNREACHABLE

    async def check_connection(
            self,
            session: ClientSession,
            urls_pool: list[str] = None
    ) -> ProxyStatus:
        if urls_pool is None:
            urls_pool = [
                'https://www.wildberries.ru/',
                'https://card.wb.ru/',
                'https://wb.ru/',
                *[f'https://basket-{number:02d}.wb.ru/' for number in range(1, 12)]
            ]

        async def check_url(url):
            try:
                prox = self.as_string()
                session.proxies.update(prox)
                with session.get(
                        url=url,
                        verify=False
                        #proxies=self.as_string()
                ) as response:
                    if response.status_code < 500 and response.status_code != 429:
                        return ProxyStatus.REACHABLE
                    else:
                        log.error(f'Сервер {self.as_string()}: FAIL {url}')
                        return ProxyStatus.UNREACHABLE
            except Exception as e:
                log.error(f'Сервер {self.as_string()}: FAIL {url} ({type(e)}: {e})')
                return ProxyStatus.UNREACHABLE

        tasks = [check_url(url) for url in urls_pool]
        results = await gather(*tasks)

        if all([result is ProxyStatus.REACHABLE for result in results]):
            self.status = ProxyStatus.REACHABLE
            log.info(f'Сервер {self.as_string()}: ОК')
        else:
            self.status = ProxyStatus.UNREACHABLE

        return self.status
