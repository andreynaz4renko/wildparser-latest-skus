import asyncio as aio
from random import sample

from aiohttp import ClientSession, TCPConnector
import requests as req
from time import time

import json

import platform

from core.random_proxy import proxy
from core.utils import *
from core.logs import logger as log
from core import Product


processed_skus = set()
unprocessed_skus_lock = aio.Lock()
unprocessed_skus_count = 0


# получение пользовательских настроек
@log.log.catch
def get_user_settings() -> str | None:
    """Возвращает пользовательские настройки от донора."""

    response = req.post(url=api_user_settings(), headers=api_default_header())
    return response.json().get('xinfo')


# получение url каталога с номером страницы
@log.log.catch
async def get_catalog_with_page(
        catalog_url: str,
        session: ClientSession,
        min_price: int = 0,
        max_price: int = 30000
):
    """
    Получение URL каталога с ценовым фильтром и номером страницы.
    Цена фильтруется по методу половинного деления (рекурсивно),
    остановка - <100 страниц в каталоге.

    :param catalog_url: URL каталога
    :param session: Сессия для создания HTTP-запросов
    :param min_price: Минимальная цена в каталоге
    :param max_price: Максимальная цена в каталоге
    """

    async with session.post(
            catalog_url,
            proxy=proxy.random_proxy()
    ) as response:
        catalog_response_json = await response.json()
        model = catalog_response_json.get('value', {}) \
                                     .get('data',  {}) \
                                     .get('model', {})
        pages_count = model.get('pagerModel', {}) \
                           .get('pagingInfo', {}) \
                           .get('totalPages', 0)
        if pages_count > 0:
            yield api_catalog_with_page(catalog_url, 1)


# получение sku продуктов
@log.log.catch
async def get_page_products_skus(
        page_url: str,
        session: ClientSession
):
    """
    Получение списка SKU на странице каталога.

    :param page_url: URL каталога с номером страницы.
    :param session: Сессия для создания HTTP-запросов
    :return: SKU продукта
    """

    async with session.post(
            page_url,
            proxy=proxy.random_proxy()
    ) as response:
        j = await response.json()
        products = j.get('value', {}) \
                    .get('data',  {}) \
                    .get('model', {}) \
                    .get('products', [])
        for product in products:
            yield product['nmId']


# обработка одного продукта
@log.log.catch
async def get_product(
        sku: int,
        user_settings: str,
        session: ClientSession,
        catalog_name: str,
        start_datetime: str
):
    """
    Получение информации о продукте.

    :param sku: Идентификатор продукта
    :param user_settings: Пользовательские настройки
    :param session: Сессия для создания HTTP-запросов
    :param catalog_name: Наименование каталога
    :param start_datetime: Дата и время начала парсинга

    :return::class:`Product` Заполненный продукт
    """

    product = Product(sku)
    product.date_parse = start_datetime
    product.catalog_name = catalog_name

    try:
        async with session.get(
                api_product_card(user_settings, sku),
                proxy=proxy.random_proxy()
        ) as card_response:
            card_response_text = await card_response.text()
            card_response_json = json.loads(card_response_text)
            products = card_response_json.get('data', {}).get('products', [])
            del card_response_text, card_response_json
            for item in products:
                product.extract_price__brand__title(item)
                product.extract_quantity(item)

            try:
                async with session.get(
                        api_static_card(sku),
                        proxy=proxy.random_proxy()
                ) as static_response:
                    if static_response.status == 200:
                        static_response_text = await static_response.text()
                        static_response_json = json.loads(static_response_text)
                        product.extract_full_name__subject__ean(static_response_json)
                        del static_response_text, static_response_json
            except Exception as e:
                log.error(f'Ошибка парсинга {sku}, не удалось собрать данные. {e}')
                product.status = False
                product.date_create = datetime_product()
                return product

            try:
                async with session.get(
                        api_merchant_info(sku),
                        proxy=proxy.random_proxy()
                ) as merchant_response:
                    if merchant_response.status == 200:
                        merchant_response_text = await merchant_response.text()
                        merchant_response_json = json.loads(merchant_response_text)
                        product.extract_merchant(merchant_response_json)
                        del merchant_response_text, merchant_response_json
            except Exception as e:
                log.error(f'Ошибка парсинга {sku}, не удалось собрать продавца. {e}')

            try:
                async with session.get(
                        api_product_info(sku, product.subject, product.brand_id),
                        headers=api_default_header(),
                        proxy=proxy.random_proxy()
                ) as info_response:
                    if info_response.status == 200:
                        info_response_text = await info_response.text()
                        info_response_json = json.loads(info_response_text)
                        product.extract_sub_catalog(info_response_json)
                        del info_response_text, info_response_json
            except Exception as e:
                log.error(f'Ошибка парсинга {sku}, не удалось собрать подкаталог. {e}')

            try:
                async with session.get(
                        api_product_orders(sku),
                        proxy=proxy.random_proxy()
                ) as orders_response:
                    if orders_response.status == 200:
                        orders_response_text = await orders_response.text()
                        orders_response_json = json.loads(orders_response_text)
                        product.extract_orders(orders_response_json)
                        del orders_response_text, orders_response_json
            except Exception as e:
                log.error(f'Ошибка парсинга {sku}, не удалось собрать кол-во продаж. {e}')
                product.sold_qty = 0
    except Exception as e:
        log.error(f'Ошибка парсинга {sku}, не удалось собрать данные. {e}')
        product.status = False

    product.date_create = datetime_product()
    return product


# обработка одного каталога
@log.log.catch
async def catalog_process(
        catalog_url: str,
        user_settings: str,
        session: ClientSession,
        catalog_name: str,
        start_datetime: str
) -> int:
    """
    Обработка каталога.

    :param catalog_url: Исходная ссылка на каталог
    :param user_settings: Пользовательские настройки
    :param session: Сессия для создания HTTP-запросов
    :param catalog_name: Наименование каталога
    :param start_datetime: Дата и время начала парсинга
    """
    global unprocessed_skus_count

    catalog_url = catalog_url.replace('/catalog/', '/webapi/catalogdata/')
    catalog_pages_coroutines = []

    products_processed = 0
    page_index = 0
    async for page_url in get_catalog_with_page(catalog_url, session):
        page_index += 1
        if page_url:
            page_products_coroutines = []
            async for sku in get_page_products_skus(page_url, session):
                if sku not in processed_skus:
                    processed_skus.add(sku)
                    page_products_coroutines.append(
                        get_product(
                            sku,
                            user_settings,
                            session,
                            catalog_name,
                            start_datetime
                        )
                    )
                else:
                    async with unprocessed_skus_lock:
                        unprocessed_skus_count += 1
            catalog_pages_coroutines += page_products_coroutines
        if page_index % 20 == 0:
            try:
                products_list = await aio.gather(*catalog_pages_coroutines)
                catalog_pages_coroutines = []
                async with unprocessed_skus_lock:
                    products_processed += serialize_products(products_list)
            except Exception as e:
                log.critical(f'⛔️ Ошибка в каталоге {catalog_name} на странице {page_index}. {e}', check=True)
                exit()

    try:
        products_list = await aio.gather(*catalog_pages_coroutines)
        async with unprocessed_skus_lock:
            products_processed += serialize_products(products_list)
    except Exception as e:
        log.critical(f'⛔️ Ошибка в каталоге {catalog_name}. {e}', check=True)
        exit()
    return products_processed


@log.log.catch
async def main():
    log.info('Статус проверки: RUN')

    user_settings = get_user_settings()
    connector = TCPConnector(limit=60, ssl=False)
    async with ClientSession(connector=connector) as session:
        start_datetime = datetime_product()
        for catalog_index, catalog in enumerate(sample(catalogs(), 5)):
            start_catalog = time()
            catalog_url = catalog['url']
            catalog_name = catalog['name']
            log.info(catalog_name)
            try:
                products_processed = await catalog_process(
                    catalog_url,
                    user_settings,
                    session,
                    catalog_name,
                    start_datetime
                )

                end_catalog = time() - start_catalog
                print_stats(products_processed, end_catalog, catalog_name, catalog_index)
            except Exception as e:
                log.critical(f'Статус проверки: FAIL\n{e}', check=True)
                exit()
    log.success(f'Health Checker Status: ОК', check=True)


if __name__ == '__main__':
    try:
        if 'Win' in platform.system():
            aio.set_event_loop_policy(aio.WindowsSelectorEventLoopPolicy())
        aio.run(main())
    except Exception as e:
        log.critical(f'⛔️ ⛔️ ⛔️ ЗАПУСК НЕ УДАЛСЯ! ⛔️ ⛔️ ⛔️ \n{e}', check=True)
