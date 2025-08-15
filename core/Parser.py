from asyncio import sleep
from time import time

from core.data.CatalogsPool import CatalogsPool
from core.proxies.ProxiesPool import ProxiesPool
from core.logs import logger as log

from requests import Session as ClientSession

from core.utils import create_csv, archive_report, send_report_sftp, clear_duplicates, get_menu


class Parser:
    def __init__(self, ifBySkuList: bool = False):
        log.info('Инициализация пула прокси')
        self.proxies_pool = ProxiesPool()
        log.info('Инициализация пула каталогов')
        self.catalogs_pool = CatalogsPool(get_menu(),ifBySkuList)
        log.info('Парсер инициализирован')

    async def prepare_catalogs_pool(self, session: ClientSession, is_retry: bool = False, ifBySkuList: bool = False):
        await self.catalogs_pool.prepare_catalogs(session, self.proxies_pool, is_retry, ifBySkuList)

    async def parse(
            self,
            session: ClientSession,
            enable_proxies: bool = True,
            retry_timeout_secs: int = 2 * 60 * 60,
            ifBySkuList: bool = False
    ):
        log.success('Начало парсинга')

        self.proxies_pool.enabled = enable_proxies
        await self.proxies_pool.refresh(session)
        await self.prepare_catalogs_pool(session, ifBySkuList=ifBySkuList)

        # create_csv()
        # start_time = time()

        # await self.catalogs_pool.parse(session, self.proxies_pool)

        # catalogs_count = len(self.catalogs_pool.catalogs_pool)
        # retry_catalogs_count = len(self.catalogs_pool.retry_catalogs_pool)
        # success_catalogs_count = catalogs_count - retry_catalogs_count
        # success_catalogs_percent = success_catalogs_count / catalogs_count * 100

        # message = f'Парсинг завершился за {(time() - start_time) / 60:.2f} мин. ' \
        #           f'Собранных каталогов: {success_catalogs_count}/{catalogs_count} ({success_catalogs_percent:.2f}%)'

        # log.success(message) if success_catalogs_percent > 90 else log.critical(message)

        # if retry_catalogs_count:
        #     start_time = time()
        #     log.success(f'Ожидание повторного парсинга ({retry_timeout_secs / 60:.2f} мин.)')
        #     await sleep(retry_timeout_secs)
        #     log.success(f'Начало повторного парсинга ({retry_catalogs_count} каталогов)')
        #     await self.proxies_pool.refresh(session)
        #     await self.prepare_catalogs_pool(session, True, ifBySkuList)
        #     await self.catalogs_pool.parse(session, self.proxies_pool, True, ifBySkuList)
        #     log.success(f'Повторный парсинг завершился за {(time() - start_time) / 60:.2f} мин.')

         #clear_duplicates()
         #archive_report()
        # #send_report_sftp()
        # log.send_log_file()
