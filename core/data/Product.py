from __future__ import annotations
from requests import Session as ClientSession, ConnectionError as ClientProxyConnectionError

import json

from core.proxies.ProxiesPool import ProxiesPool
from core.utils import *
from core.logs import logger as log


class Product:
    def __init__(self, sku: int):
        self.sku          : int         = sku
        self.url          : str         = api_product_url(sku)
        self.title        : str         = ''
        self.full_price   : int | None  = None
        self.sale_price   : int | None  = None
        self.quantity     : int | None  = None
        self.feedbacks    : int | None  = None
        self.brand_id     : int         = 0
        self.brand_name   : str         = ''
        self.date_create  : str         = ''
        self.date_parse   : str         = datetime_product()
        self.sold_qty     : int | None  = None
        self.sub_catalog  : str         = ''
        self.catalog_name : str         = ''
        self.merchant_name: str         = ''
        self.merchant_ogrn: str         = ''
        self.subject      : str | None  = None
        self.ean          : str         = ''
        self.status       : bool        = True

    @staticmethod
    async def parse(
            session: ClientSession,
            proxies: ProxiesPool,
            sku: int,
            user_settings: str,
            catalog_name: str,
            start_time: str
    ):
        """
        Получение информации о продукте.

        :param session: Сессия для создания HTTP-запросов
        :param proxies: Пул прокси для создания HTTP-запросов
        :param sku: Идентификатор продукта
        :param user_settings: Пользовательские настройки
        :param catalog_name: Наименование каталога
        :param start_time: Дата и время начала парсинга

        :return::class:`Product` Заполненный продукт
        """

        product = Product(sku)
        product.date_parse = start_time
        product.catalog_name = catalog_name
        product.date_create = datetime_product()

        try:
            proxy = proxies.get_random_proxy()
            with session.get(
                    api_product_card(user_settings, sku),
                    verify=False,
                    proxies=proxy.as_string()
            ) as card_response:
                card_response_text = card_response.text
                card_response_json = json.loads(card_response_text)
                products = card_response_json.get('data', {}).get('products', [])
                del card_response_text, card_response_json
                for item in products:
                    product.extract_price__brand__title(item)
                    product.extract_quantity_feedbacks(item)

                try:
                    proxy = proxies.get_random_proxy()
                    prox = proxy.as_string()
                    session.proxies.update(prox)
                    with session.get(
                            api_product_info_new(sku),
                            verify=False
                            #proxies=proxy.as_string()
                    ) as static_response:
                        if static_response.status_code == 200:
                            static_response_text = static_response.text
                            static_response_json = json.loads(static_response_text)
                            product.extract_full_name__subject__ean(static_response_json)
                            del static_response_text, static_response_json
                except ClientProxyConnectionError as e:
                    log.error(f'Ошибка парсинга {sku}, не удалось собрать данные. {type(e)}: {e}')
                    product.status = False
                    if proxy:
                        proxies.disable(proxy)
                except Exception as e:
                    log.error(f'Ошибка парсинга {sku}, не удалось собрать данные. {type(e)}: {e}')
                    product.status = False
                    return product

                try:
                    proxy = proxies.get_random_proxy()
                    prox = proxy.as_string()
                    session.proxies.update(prox)
                    with session.get(
                            api_merchant_info(sku),
                            verify=False
                            #proxies=proxies.get_random_proxy().as_string()
                    ) as merchant_response:
                        if merchant_response.status_code == 200:
                            merchant_response_text = merchant_response.text
                            merchant_response_json = json.loads(merchant_response_text)
                            product.extract_merchant(merchant_response_json)
                            del merchant_response_text, merchant_response_json
                except Exception as e:
                    log.error(f'Ошибка парсинга {sku}, не удалось собрать продавца. {type(e)}: {e}')

                try:
                    proxy = proxies.get_random_proxy()
                    prox = proxy.as_string()
                    session.proxies.update(prox)
                    with session.get(
                            api_product_info(sku, product.subject, product.brand_id),
                            verify=False,
                            headers=api_default_header()
                            #proxies=proxies.get_random_proxy().as_string()
                    ) as info_response:
                        if info_response.status_code == 200:
                            info_response_text = info_response.text
                            info_response_json = json.loads(info_response_text)
                            product.extract_sub_catalog(info_response_json)
                            del info_response_text, info_response_json
                except Exception as e:
                    log.error(f'Ошибка парсинга {sku}, не удалось собрать подкаталог. {type(e)}: {e}')

                try:
                    proxy = proxies.get_random_proxy()
                    prox = proxy.as_string()
                    session.proxies.update(prox)
                    with session.get(
                            api_product_orders(sku),
                            verify=False
                            #proxies=proxies.get_random_proxy().as_string()
                    ) as orders_response:
                        if orders_response.status_code == 200:
                            orders_response_text = orders_response.text
                            orders_response_json = json.loads(orders_response_text)
                            product.extract_orders(orders_response_json)
                            del orders_response_text, orders_response_json
                except Exception as e:
                    log.error(f'Ошибка парсинга {sku}, не удалось собрать кол-во продаж. {type(e)}: {e}')
                    product.sold_qty = 0
        except ClientProxyConnectionError as e:
            log.error(f'Ошибка парсинга {sku}, не удалось собрать данные. {type(e)}: {e}')
            product.status = False
            if proxy:
                proxy.disable()
        except Exception as e:
            log.error(f'Ошибка парсинга {sku}, не удалось собрать данные. {type(e)}: {e}')
            product.status = False

        return product

    @staticmethod
    def get_sub_catalog(
            breadcrumbs: list[dict]
    ) -> str | None:
        if not breadcrumbs:
            return None
        if breadcrumbs[-1]['id'] == 0:
            sub_catalog = breadcrumbs[-2]['name']
        else:
            sub_catalog = breadcrumbs[-1]['name']
        return sub_catalog

    def extract_price__brand__title(self, item_json: dict):
        """
        Извлечение цен, бренда и наименования продукта из JSON.

        :param item_json: JSON-ответ сервера
        """

        self.full_price = item_json.get('priceU', 0) // 100
        self.sale_price = item_json.get('salePriceU', 0) // 100

        self.brand_id = item_json.get('brandId', 0)
        self.brand_name = item_json.get('brand', '')

        self.title = item_json.get('name', '').replace('\n', ' ')

    def extract_full_name__subject__ean(self, static_json: dict):
        """
        Извлечение полного наименования, id категории и EAN-кода из JSON.

        :param static_json: JSON-ответ сервера
        """

        title = static_json.get('imt_name', '').replace('\n', ' ')
        if len(title):
            self.title = title

        data = static_json.get('data', {})
        skus = data.get('skus', [])
        if len(skus):
            self.ean = skus[0]
        self.subject = data.get('subject_id', None)

    def extract_quantity_feedbacks(self, item_json: dict):
        """
        Извлечение остатков на складах и количества оценок из JSON.

        :param item_json: JSON-ответ сервера
        """
        self.feedbacks = item_json.get('feedbacks', 0)
        quantity = 0
        for size in item_json.get('sizes', []):
            for stock in size.get('stocks', []):
                quantity += stock.get('qty', 0)
        self.quantity = quantity

    def extract_merchant(self, merchant_json: dict | list):
        """
        Извлечение продавца из JSON.

        :param merchant_json: JSON-ответ сервера
        """

        merchant_name = merchant_json.get('supplierName', '')
        merchant_ogrn = merchant_json.get('ogrn', '')
        if merchant_ogrn in ['0', 'NULL']:
            merchant_ogrn = ''
        self.merchant_name = merchant_name
        self.merchant_ogrn = merchant_ogrn

    def extract_sub_catalog(self, info_json: dict | list):
        """
        Извлечение подкаталога из JSON.

        :param info_json: JSON-ответ сервера
        """

        site_path = info_json.get('value', {}) \
            .get('data', {}) \
            .get('sitePath', [])
        sub_catalog = self.get_sub_catalog(site_path)
        if sub_catalog:
            self.sub_catalog = sub_catalog

    def extract_orders(self, orders_json: dict | list):
        """
        Извлечение кол-ва заказов из JSON.

        :param orders_json: JSON-ответ сервера
        """

        if isinstance(orders_json, list) and len(orders_json):
            orders = orders_json[0].get('qnt', 0)
            self.sold_qty = orders

    def __iter__(self):
        self.title = f'{self.brand_name} / {self.title}'
        return iter([
            self.date_parse,
            self.sku,
            self.title,
            self.url,
            self.sale_price,
            self.full_price,
            self.quantity,
            self.date_create,
            self.sold_qty,
            self.sub_catalog,
            self.catalog_name,
            self.merchant_name,
            self.merchant_ogrn,
            self.ean,
            self.feedbacks
        ])

    def __repr__(self):
        return f'<Product sku={self.sku}>'
