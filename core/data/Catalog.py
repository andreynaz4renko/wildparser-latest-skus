from typing import AsyncIterable
from tqdm.asyncio import tqdm_asyncio as tqdm
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from threading import Timer
import json
from asyncio import gather, Semaphore
from requests import Session as ClientSession
from core.data.CatalogFilter import CatalogFilter
from core.data.CatalogStatus import CatalogStatus, CatalogType
from core.data.Product import Product
from core.proxies.ProxiesPool import ProxiesPool
from core.utils import generate_pages_for_filter, api_filters, api_brand_filters

from core.logs import logger as log


class Catalog:
    def __init__(
            self,
            name: str,
            address: str|None = None,
            query: str|None = None,
            shard: str|None = None,
            xsubject: str|None = None,
            skus_pool: list[int] = [],
            catalog_type: CatalogType = CatalogType.CATALOG,
            brand_id: str|None = None
    ):
        self.total_pages_count = 0
        self.total_items_count = len(skus_pool)
        self.total_items_count_percent = 0
        self.parsed_items_count = 0
        self.parsed_items_percentages = 0
        self.parsed_items = []
        self.name = name
        self.query = query
        self.shard = shard
        self.xsubject = xsubject
        self.catalog_type = catalog_type
        self.brand_id = brand_id
        self.source_address = address
        self.filters_pool: list[CatalogFilter] = []
        self.skus_pool: list[int] = skus_pool
        self.status = CatalogStatus.ENQUEUED

    def __str__(self):
        return f"{self.name} {self.total_items_count} тов. {self.source_address}"
    
    @staticmethod
    def build_url_with_params(address: str, params: dict):
        url_parts = list(urlparse(address))
        query = dict(parse_qs(url_parts[4]))
        for param in params:
            param_value = params[param]
            if param_value is not None:
                query[param] = param_value
        url_parts[4] = urlencode(query, doseq=True)
        return urlunparse(url_parts)

    async def fetch_json_response(self, session: ClientSession, address: str, proxies: ProxiesPool):
        for spp in [0, 30, None]:
            for curr in [None, 'rub']:
                for app_type in [1, None, 30, 2, 3]:
                    new_address = self.build_url_with_params(
                        address,
                        {
                            'appType': app_type,
                            'curr': curr,
                            'spp': spp
                        }
                    )
                    proxy = proxies.get_random_proxy()
                    prox = proxy.as_string()
                    session.proxies.update(prox)
                    try:
                        with session.get(
                                new_address,
                                verify=False
                                #proxy=proxies.get_random_proxy().as_string()
                        ) as response:
                            if response.status_code == 200:
                                return json.loads(response.text), new_address
                    except Exception as e:
                        log.error(e)
        return None, new_address

    async def prepare_catalog(
            self,
            session: ClientSession,
            proxies: ProxiesPool
    ):
        log.info(f'Инициализация пула фильтров каталога {self.name}')
        await self.fetch_filters_pool(session, proxies)
        log.info(f'Пул фильтров каталога {self.name} инициализирован')
        log.info(f'Инициализация пула идентификаторов продуктов каталога {self.name}')
        await self.fetch_skus_pool(session, proxies)
        log.info(f'Пул идентификаторов продуктов каталога {self.name} инициализирован')

    async def fetch_filters_pool(
            self,
            session: ClientSession,
            proxies: ProxiesPool,
            address: str|None = None
    ):
        if self.catalog_type == CatalogType.BRAND:
            if address is None:
                address = api_brand_filters(self.brand_id, 0, 100_000_000, self.xsubject)
        else:
            if address is None:
                address = api_filters(self.shard, self.query, 0, 100_000_000, self.xsubject)
        try:
            json_response, new_address = await self.fetch_json_response(session, address, proxies)

            if json_response is not None:
                total_products = json_response \
                    .get("data", {}) \
                    .get("total", 0)

                url_parts = list(urlparse(address))
                query = dict(parse_qs(url_parts[4]))
                min_price, max_price = [int(x) for x in query.get('priceU', ['0;100000000'])[0].split(';')]
                mid_price = (min_price + max_price) // 2

                if total_products > 1000 and max_price - min_price > 2:
                    lower_price_url = self.build_url_with_params(address, {'priceU': f'{min_price};{mid_price}'})
                    await self.fetch_filters_pool(session, proxies, lower_price_url)
                    higher_price_url = self.build_url_with_params(address, {'priceU': f'{mid_price + 1};{max_price}'})
                    await self.fetch_filters_pool(session, proxies, higher_price_url)
                elif total_products != 0:
                    total_pages = total_products // 100 + 1
                    self.total_items_count += total_products
                    self.total_pages_count += total_pages
                    self.filters_pool.append(
                        CatalogFilter(
                            name=self.name,
                            total_pages=total_pages,
                            total_items=total_products,
                            min_price=min_price,
                            max_price=max_price
                        )
                    )
                else:
                    self.status = CatalogStatus.FAILURE
            else:
                self.status = CatalogStatus.FAILURE
        except Exception as e:
            self.status = CatalogStatus.FAILURE

    async def fetch_skus_pool(
            self,
            session: ClientSession,
            proxies: ProxiesPool
    ):
        self.skus_pool = []
        for catalog_filter in self.filters_pool:
            if catalog_filter.total_items == 0:
                continue
            for catalog_page in generate_pages_for_filter(catalog_filter, self.shard, self.query, self.xsubject, self.catalog_type, self.brand_id):
                async for product_sku in self.parse_product_skus(catalog_page, session, proxies):
                    self.skus_pool.append(product_sku)

        if self.total_items_count == 0:
            log.critical(f'В каталоге {self.name} собрано 0 продуктов')
            return

        self.total_items_count_percent = len(self.skus_pool) / self.total_items_count * 100

        log_fun = log.info
        if self.total_items_count_percent < 90:
            log_fun = log.critical

        log_fun(f'Подготовлено продуктов: {len(self.skus_pool)}/{self.total_items_count} '
                f'({self.total_items_count_percent:.2f}%) для каталога {self.name}')

    async def parse_product_skus(
            self,
            page_address: str,
            session: ClientSession,
            proxies: ProxiesPool
    ) -> AsyncIterable[int]:
        try:
            response_json, new_address = await self.fetch_json_response(session, page_address, proxies)

            products = response_json \
                .get('data', {}) \
                .get('products', [])

            for product in products:
                yield product['id']

        except Exception as e:
            log.error(f'Ошибка парсинга страницы {page_address}. {type(e)}: {e}')

    async def parse(
            self,
            session: ClientSession,
            proxies: ProxiesPool,
            user_settings: str,
            start_time: str
    ):
        log.info(f'Начало парсинга {self.name}')

        catalog_products_coroutines = []
        for sku in self.skus_pool:
            catalog_products_coroutines.append(
                Product.parse(
                    session=session,
                    proxies=proxies,
                    sku=sku,
                    user_settings=user_settings,
                    catalog_name=self.name,
                    start_time=start_time
                )
            )

        catalog_parsed_products = await gather_with_concurrency(45, *catalog_products_coroutines)
        catalog_successful_parsed_products = [product for product in catalog_parsed_products if product.status]
        self.parsed_items += catalog_successful_parsed_products
        parsed_items_count = len(catalog_successful_parsed_products)

        if parsed_items_count == 0 and self.total_items_count > 100:
            if self.status is CatalogStatus.DONE:
                log.critical(f'Не удалось собрать данные каталога {self.name}. Требуется перезапуск')
            else:
                self.status = CatalogStatus.FAILURE
                log.error(f'Не удалось собрать данные каталога {self.name}')
        else:
            collected = parsed_items_count / self.total_items_count * 100 if self.total_items_count else 0
            log.info(f'В каталоге {self.name} собрано {parsed_items_count}/{self.total_items_count} '
                     f'({collected:.2f}%) продуктов')

        if self.total_items_count > 0:
            self.parsed_items_count = parsed_items_count
            self.parsed_items_percentages = parsed_items_count / self.total_items_count * 100

        log.info(f'Конец парсинга {self.name}. Собрано {parsed_items_count}/{self.total_items_count} '
                 f'({self.parsed_items_percentages:.2f}%) продуктов')


async def gather_with_concurrency(count, *coroutines):
    semaphore = Semaphore(count)

    async def coroutine_semaphore(coroutine):
        async with semaphore:
            return await coroutine

    return await tqdm.gather(*(coroutine_semaphore(coroutine) for coroutine in coroutines))


