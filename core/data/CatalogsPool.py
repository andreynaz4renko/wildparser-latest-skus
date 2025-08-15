from __future__ import annotations
import json
from urllib.parse import urlparse, parse_qs
import csv
from requests import Session as ClientSession
from core.utils import catalog_groups
from core.data.Catalog import Catalog
from core.data.CatalogStatus import CatalogStatus, CatalogType
from core.proxies.ProxiesPool import ProxiesPool
from core.utils import datetime_product, api_user_settings, api_default_header, serialize_products, catalogs, brands, _filepath
from core.logs import logger as log


class CatalogsPool:
    def __init__(self,menu: dict, ifBySkuList: bool):
        self.catalogs_pool: list[Catalog] = []
        self.retry_catalogs_pool: list[Catalog] = []
        self.menu = menu
        if not ifBySkuList:
            self.load_from_file()
            self.load_brands_from_file()

    async def prepare_catalogs(
            self,
            session: ClientSession,
            proxies: ProxiesPool,
            is_retry: bool = False,
            ifBySkuList: bool = False
    ):
        log.info('Подготовка каталогов')
        if not ifBySkuList:
            for catalog in self.retry_catalogs_pool if is_retry else self.catalogs_pool:
                await catalog.prepare_catalog(session, proxies)
            log.info('Каталоги подготовлены')
            with open(
                _filepath("skus_id.csv"), 'a', newline='', encoding='utf-8'
                ) as f:
                writer = csv.writer(f, delimiter=';')
                skus = [['catalog_name','sku']]
                for catalog in self.catalogs_pool:
                    for sku in catalog.skus_pool:
                        skus.append([catalog.name, sku])
                #skus = self.remove_duplicates_by_id(skus)
                writer.writerows(skus)
            with open(
                _filepath("catalogs_status.csv"), 'a', newline='', encoding='utf-8'
                ) as f:
                writer = csv.writer(f, delimiter=';')
                skus = []
                for catalog in self.catalogs_pool:
                    skus.append([catalog.name, catalog.total_items_count, catalog.total_items_count_percent])
                writer.writerows(skus)
        else:
            for group in catalog_groups():
                group_name, group_data = group
                self.catalogs_pool.append(
                    Catalog(
                        name= group_name,
                        skus_pool=list(set(group_data['sku']))
                    )
                )
            log.info('Каталоги подготовлены')

    def remove_duplicates_by_id(self,data):
        seen_ids = set()
        result = []
        for item in data:
            if item[1] not in seen_ids:
                seen_ids.add(item[1])
                result.append(item)
        return result

    def load_from_file(self):
        for catalog in catalogs():
            name = catalog['name']
            address = catalog['url']
            menu_item = self.get_menu_item(address)
            query = menu_item.get('query')
            shard = menu_item.get('shard')
            xsubject = self.get_xsubject(address)
            self.catalogs_pool.append(
                Catalog(
                    name=name,
                    address=address,
                    query=query,
                    shard=shard,
                    xsubject=xsubject,
                    catalog_type = CatalogType.CATALOG
                )
            )
    
    def load_brands_from_file(self):
        for catalog in brands():
            name = catalog['category_name']
            brand_id = catalog['brand id']
            xsubject = catalog['xsubject']
            self.catalogs_pool.append(
                Catalog(
                    name=name,
                    brand_id=brand_id,
                    xsubject=xsubject,
                    catalog_type = CatalogType.BRAND
                )
            )

    def next_catalog(
            self,
            is_retry: bool = False
    ):
        catalogs = self.retry_catalogs_pool if is_retry else self.catalogs_pool
        for catalog in catalogs:
            if catalog.status is CatalogStatus.ENQUEUED and catalog.total_items_count \
                    or catalog.status is CatalogStatus.FAILURE:
                catalog.status = CatalogStatus.DONE
                yield catalog

    async def parse(
            self,
            session: ClientSession,
            proxies: ProxiesPool,
            is_retry: bool = False
    ):
        user_settings = await get_user_settings(session, proxies)
        for catalog in self.next_catalog(is_retry):
            await catalog.parse(session, proxies, user_settings, datetime_product())
            if catalog.parsed_items_percentages < 90 and not is_retry:
                # catalog.clear()
                log.critical(f'Запланирован повторный парсинг: {str(catalog)}')
                catalog.status = CatalogStatus.FAILURE
                self.retry_catalogs_pool.append(catalog)
                continue
            serialize_products(catalog.parsed_items)
            # if catalog.total_items_count > 500:
            #     await proxies.refresh(session)

    def get_menu_item(self, address):
        path = urlparse(address).path
        return self.menu.get(path, {})

    @staticmethod
    def get_xsubject(address):
        url = urlparse(address)
        query = dict(parse_qs(url.query))
        return query.get('xsubject', [None])[0]


async def get_user_settings(
        session: ClientSession,
        proxies: ProxiesPool
) -> str | None:
    try:
        proxy = proxies.get_random_proxy()
        prox = proxy.as_string()
        session.proxies.update(prox)
        with session.post(
                url=api_user_settings(),
                headers=api_default_header(),
                verify=False
                #proxies=proxies.get_random_proxy().as_string()
        ) as response:
            response_json = json.loads(response.text)
            return await response_json.get('xinfo')
    except:
        return 'appType=1&curr=rub&dest=-1255987&regions=80,38,4,64,83,33,68,70,69,30,86,75,40,1,66,110,22,31,48,71,114&spp=0'
