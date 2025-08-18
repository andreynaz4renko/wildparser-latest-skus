from __future__ import annotations
from datetime import datetime as dt
import os
import json
import csv
import zipfile as zf
from functools import lru_cache
import pandas as pd

import pysftp

from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from core.data.CatalogFilter import CatalogFilter
from core.data.CatalogStatus import CatalogType
from core.logs import logger as log

from requests import get

# Получение настроек SFTP из переменных окружения
_PARSER_SFTP_HOST = os.getenv('PARSER_SFTP_HOST', '')
_PARSER_SFTP_PORT = int(os.getenv('PARSER_SFTP_PORT', '22'))
_PARSER_SFTP_USER = os.getenv('PARSER_SFTP_USER', '')
_PARSER_SFTP_FKEY = os.getenv('PARSER_SFTP_FKEY', '')
_PARSER_SFTP_CERT = os.getenv('PARSER_SFTP_CERT', '')
_PARSER_SFTP_PATH = os.getenv('PARSER_SFTP_PATH', '')
_PARSER_SFTP_PASS = os.getenv('PARSER_SFTP_PASS', None)

# Получение настроек файла отчета из переменных окружения
_PARSER_FILE_DIR = os.getenv('PARSER_FILE_DIR', 'csv')
_PARSER_FILE_PREFIX = os.getenv('PARSER_FILE_PREFIX', 'd_parsed_data_')

# Получение пути к файлу списка каталогов из переменных окружения
_PARSER_CATALOGS_PATH = os.getenv('PARSER_CATALOGS_PATH', 'csv/catalogs.csv')
_PARSER_BRANDS_PATH = os.getenv('PARSER_BRANDS_PATH', 'csv/brands.csv')
_PARSER_SKUS_PATH = os.getenv('PARSER_SKUS_PATH', 'csv/skus_id.csv')

# Константы с API URL
_API_USER_XINFO = 'https://www.wildberries.ru/webapi/user/get-xinfo-v2'
_API_PRODUCT_CARD = 'https://card.wb.ru/cards/v2/detail?{}&nm={}'
_API_STATIC_CARD = 'https://wbx-content-v2.wbstatic.net/ru/{}.json'
_API_PRODUCT_URL = 'https://www.wildberries.ru/catalog/{}/detail.aspx'
_API_PRODUCT_INFO = 'https://www.wildberries.ru/webapi/product/{}/data?'
_API_PRODUCT_ORDERS = 'https://product-order-qnt.wildberries.ru/v2/by-nm/?nm={}'
_API_MERCHANT_INFO = 'https://www.wildberries.ru/webapi/seller/data/short/{}'
_API_FILTERS = 'https://catalog.wb.ru/catalog/{shard}/v4/filters?{query}&dest=-1299031'
_API_BRANDS = 'https://catalog.wb.ru/brands/v4/filters?brand={brand_id}&dest=-1257786'
_API_PRODUCTS = 'https://catalog.wb.ru/catalog/{shard}/v2/catalog?{query}&dest=-1299031&page=1&sort=popular'
_API_BRAND_PRODUCTS = 'https://catalog.wb.ru/brands/v2/catalog?brand={brand_id}&dest=-1257786&page=1&sort=popular'
_MENU_URL = 'https://static-basket-01.wbbasket.ru/vol0/data/main-menu-ru-ru-v3.json'

MAX_PAGES = 100  # WB обычно не даёт больше ~100 страниц

def print_stats(
        count: int,
        elapsed: float,
        catalog_name: str,
        catalog_index: int
):
    log.info(f'Обработано {str(count).rjust(5)} продуктов за '
             f'{str(round(elapsed, 2)).rjust(7)} сек. <- {catalog_name} ({catalog_index + 1})')


def api_user_settings() -> str:
    """Возвращает URL к пользовательским настройкам."""

    return _API_USER_XINFO


def api_product_card(
        user_settings: str,
        sku: int
) -> str:
    """
    Возвращает URL к карточке продукта.

    :param user_settings: Настройки пользователя
    :param sku: Идентификатор продукта
    """

    return _API_PRODUCT_CARD.format(user_settings, sku)


def api_static_card(sku: int) -> str:
    """
    Возвращает URL к статической карточке продукта.

    :param sku: Идентификатор продукта
    """

    return _API_STATIC_CARD.format(sku)


def api_product_url(sku: int) -> str:
    """
    Возвращает URL к продукту.

    :param sku: Идентификатор продукта
    """

    return _API_PRODUCT_URL.format(sku)


def api_product_info(
        sku: int,
        subject: str | None = None,
        brand_id: int | None = None
) -> str:
    """
    Возвращает URL к информации о продукте.

    :param sku: Идентификатор продукта
    :param subject: Идентификатор каталога
    :param brand_id: Идентификатор бренда
    """

    info_url = _API_PRODUCT_INFO.format(sku)
    if subject:
        info_url += f'&subject={subject}'
    if brand_id:
        info_url += f'&brand={brand_id}'
    return info_url


def api_product_orders(sku: int) -> str:
    """
    Возвращает URL к информации о кол-ве заказов.

    :param sku: Идентификатор продукта
    """

    return _API_PRODUCT_ORDERS.format(sku)


@lru_cache
def _vol_host(vol: int) -> str:
    """
    Возвращает домен S3, в котором лежит информация о продукте.
    Алгоритм взят из JS на сайте-доноре.

    :param vol: Часть идентификатора продукта
    """

    ranges = [
        (0, 143, "basket-01.wbbasket.ru"),
        (144, 287, "basket-02.wbbasket.ru"),
        (288, 431, "basket-03.wbbasket.ru"),
        (432, 719, "basket-04.wbbasket.ru"),
        (720, 1007, "basket-05.wbbasketru"),
        (1008, 1061, "basket-06.wbbasket.ru"),
        (1062, 1115, "basket-07.wbbasket.ru"),
        (1116, 1169, "basket-08.wbbasket.ru"),
        (1170, 1313, "basket-09.wbbasket.ru"),
        (1314, 1601, "basket-10.wbbasket.ru"),
        (1602, 1655, "basket-11.wbbasket.ru"),
        (1656, 1919, "basket-12.wbbasket.ru"),
        (1920, 2045, "basket-13.wbbasket.ru"),
        (2046, 2189, "basket-14.wbbasket.ru"),
        (2190, 2405, "basket-15.wbbasket.ru"),
        (2406, 2621, "basket-16.wbbasket.ru"),
        (2622, 2837, "basket-17.wbbasket.ru"),
        (2838, 3053, "basket-18.wbbasket.ru"),
        (3054, 3269, "basket-19.wbbasket.ru"),
        (3270, 3485, "basket-20.wbbasket.ru"),
        (3486, 3701, "basket-21.wbbasket.ru"),
        (3702, 3917, "basket-22.wbbasket.ru"),
        (3918, 4133, "basket-23.wbbasket.ru"),
        (4134, 4349, "basket-24.wbbasket.ru"),
        (4350, 4565, "basket-25.wbbasket.ru"),
        (4566, float("inf"), "basket-26.wbbasket.ru"),
    ]

    for start, end, domain in ranges:
        if start <= vol <= end:
            return domain

    return "basket-01.wbbasket.ru"


def _construct_host(sku: int) -> str:
    """
    Возвращает URL к S3, в котором лежит информация о продукте.
    Алгоритм взят из JS на сайте-доноре.

    :param sku: Идентификатор продукта
    """

    vol = int(sku // 1e5)
    part = int(sku // 1e3)
    host = _vol_host(vol)
    return f'https://{host}/vol{vol}/part{part}/{sku}'


def api_merchant_info(sku: int) -> str:
    """
    Возвращает URL до информации о продавце.

    :param sku: Идентификатор продукта
    """

    return f'{_construct_host(sku)}/info/sellers.json'


def api_product_info_new(sku: int) -> str:
    """
    Возвращает URL до информации о продукте.

    :param sku: Идентификатор продукта
    """

    return f'{_construct_host(sku)}/info/ru/card.json'


def api_catalog_with_price(
        catalog_url: str,
        min_price: int,
        max_price: int
) -> str:
    """
    Возвращает URL каталога с ценовым диапазоном.

    :param catalog_url: URL каталога
    :param min_price: Минимальная цена
    :param max_price: Максимальная цена
    """

    catalog_url = catalog_url.rstrip('/')
    parse_result = urlparse(catalog_url)
    query_params_dict = parse_qs(parse_result.query)
    price_u = f'{min_price}00;{max_price}00'
    query_params_dict['priceU'] = [price_u]
    query_params = urlencode(query_params_dict, doseq=True)
    parse_result = parse_result._replace(query=query_params)
    return urlunparse(parse_result)


def api_catalog_with_page(
        catalog_url: str,
        page
) -> str:
    """
    Возвращает URL каталога с номером страницы (page=n).

    :param catalog_url: URL каталога
    :param page:
    """

    catalog_url = catalog_url.rstrip('/')
    parse_result = urlparse(catalog_url)
    query_params_dict = parse_qs(parse_result.query)
    query_params_dict['page'] = page
    query_params = urlencode(query_params_dict, doseq=True)
    parse_result = parse_result._replace(query=query_params)
    return urlunparse(parse_result)


def api_brand_filters(
        brand_id: str,
        min_price: int,
        max_price: int,
        xsubject: str | None = None
):
    """
    Generate the API URL for requesting products.

    :param brand_id: The brand_id to be used in the URL.
    :param min_price: The minimum price for filtering products.
    :param max_price: The maximum price for filtering products.
    :param xsubject: Optional parameter for filtering by subject.
    :return: The generated API URL.
    """
    url = _API_BRANDS.format(brand_id=brand_id)
    url_parts = list(urlparse(url))
    query = dict(parse_qs(url_parts[4]))
    query['priceU'] = f'{min_price};{max_price}'
    if xsubject:
        query['xsubject'] = xsubject
    url_parts[4] = urlencode(query, doseq=True)
    return urlunparse(url_parts)


def api_filters(
        shard: str,
        query: str,
        min_price: int,
        max_price: int,
        xsubject: str | None = None
):
    """
    Generate the API URL for requesting products.

    :param page: The page number for pagination.
    :param shard: The shard to be used in the URL.
    :param query: The search query.
    :param min_price: The minimum price for filtering products.
    :param max_price: The maximum price for filtering products.
    :param xsubject: Optional parameter for filtering by subject.
    :return: The generated API URL.
    """

    url = _API_FILTERS.format(shard=shard, query=query)
    url_parts = list(urlparse(url))
    query = dict(parse_qs(url_parts[4]))
    query['priceU'] = f'{min_price};{max_price}'
    if xsubject:
        query['xsubject'] = xsubject
    url_parts[4] = urlencode(query, doseq=True)
    return urlunparse(url_parts)


def api_products(
        page: int,
        shard: str,
        query: str,
        min_price: int,
        max_price: int,
        xsubject: str | None = None,
        catalog_type: CatalogType = CatalogType.CATALOG,
        brand_id: str | None = None
):
    if catalog_type == CatalogType.CATALOG:
        url = _API_PRODUCTS.format(shard=shard, query=query)
    else:
        url = _API_BRAND_PRODUCTS.format(brand_id=brand_id)
    url_parts = list(urlparse(url))
    query = dict(parse_qs(url_parts[4]))
    query['page'] = page
    query['priceU'] = f'{min_price};{max_price}'
    if xsubject:
        query['xsubject'] = xsubject
    url_parts[4] = urlencode(query, doseq=True)
    return urlunparse(url_parts)


def api_default_header() -> dict:
    """Возвращает заголовки X-Requested-With и x-spa-version."""

    return {
        'X-Requested-With': 'XMLHttpRequest',
        'x-spa-version': '9.3.73.3'
    }


def datetime_product() -> str:
    """Возвращает дату и время в формате `YYYY-mm-dd HH:MM:SS`."""

    return dt.now().strftime('%Y-%m-%d %H:%M:%S')


def _datetime_file() -> str:
    """Возвращает дату в формате `YYYY-mm-dd`."""

    return dt.now().strftime('%Y-%m-%d')


def _filename(file_postfix: str = '.csv') -> str:
    """
    Возвращает название файла CSV-отчета.

    :param file_postfix: Расширение файла, csv по умолчанию
    """

    file_prefix = _PARSER_FILE_PREFIX
    file_name = file_prefix + _datetime_file() + file_postfix
    return file_name


def _filepath(filename: str = _filename()) -> str:
    """
    Возвращает путь для CSV-отчета.

    :param filename: Название файла
    """

    file_directory = _PARSER_FILE_DIR
    file_path = os.path.join(file_directory, filename)
    return file_path


def create_csv():
    """Создание CSV-файла."""

    with open(_filepath(), 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(csv_header())


def serialize_products(products_list) -> int:
    """
    Сохранение списка продуктов в файл.

    :param products_list: Список продуктов
    """

    bad_products = [product.sku for product in products_list if product and not product.status]
    if len(bad_products):
        log.error(f'Ошибки парсинга возникли с товарами: {bad_products}')
    products_list = [product for product in products_list if product and product.status]

    with open(
            _filepath(), 'a', newline='', encoding='utf-8'
    ) as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerows(products_list)

        log.info(f'Продуктов записано в файл: {len(products_list)}')

    return len(products_list)


def serialize_catalogs(catalogs_list):
    with open(
            _filepath(), 'a', newline='', encoding='utf-8'
    ) as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerows(catalogs_list)


def clear_duplicates():
    """Очистка отчета от дубликатов."""

    try:
        log.info(f'Очистка отчета от дубликатов')
        filepath_csv = _filepath()
        report = pd.read_csv(filepath_csv, encoding='utf-8', delimiter=';', on_bad_lines='skip')
        log.info(f'Строк в файле до очистки: {len(report)}')
        report.sort_values(by='date_create', inplace=True)
        report.drop_duplicates(subset=['catalog_name', 'sku'], keep='first', inplace=True)
        log.info(f'Строк в файле после очистки: {len(report)}')
        report.to_csv(filepath_csv, index=False)
        log.success(f'Отчет очищен от дубликатов')
    except Exception as e:
        log.critical(f'Ошибка очистки отчета от дубликатов. {type(e)}: {e}')


def archive_report():
    """Упаковка отчета в ZIP-архив."""

    try:
        log.info(f'Упаковка отчета')
        filename_csv = _filename()
        filepath_csv = _filepath()
        filename_zip = _filename('.zip')
        filepath_zip = _filepath(filename_zip)
        compression = zf.ZIP_BZIP2
        with zf.ZipFile(
                filepath_zip, 'w', compression
        ) as archive:
            archive.write(filepath_csv, filename_csv)
            if os.path.exists(filepath_zip):
                os.remove(filepath_csv)
        log.success(f'Отчет упакован')
    except Exception as e:
        log.critical(f'Ошибка упаковки отчета. {type(e)}: {e}')


def send_report_sftp():
    """Отправка отчета на SFTP-сервер."""

    try:
        log.info(f'Отправка отчета')
        host = _PARSER_SFTP_HOST
        port = _PARSER_SFTP_PORT
        user = _PARSER_SFTP_USER
        fkey = _PARSER_SFTP_FKEY
        pswd = _PARSER_SFTP_PASS
        cert = _PARSER_SFTP_CERT
        path = _PARSER_SFTP_PATH

        filename_zip = _filename('.zip')
        filepath_zip_local = _filepath(filename_zip)

        cnopts = pysftp.CnOpts()
        if len(cert):
            cnopts = pysftp.CnOpts(knownhosts=cert)
        cnopts.postcheck = False

        with pysftp.Connection(
                host=host,
                port=int(port),
                username=user,
                password=pswd,
                cnopts=cnopts
        ) if pswd and len(pswd) else pysftp.Connection(
            host=host,
            port=int(port),
            username=user,
            private_key=fkey,
            cnopts=cnopts
        ) as sftp:
            sftp.blocksize = 1024 * 64
            sftp.cwd(path)
            sftp.put(filepath_zip_local, filename_zip, preserve_mtime=False)
            log.success('Отчет отправлен')
    except OSError as e:
        log.success('Отчет отправлен?')
    except Exception as e:
        log.error(f'Ошибка отправки отчета. {type(e)}: {e}')


def csv_header() -> list:
    """Возвращает заголовок для CSV-отчета."""

    return [
        'date_parse',
        'sku',
        'title',
        'url',
        'price',
        'old_price',
        'qty',
        'date_create',
        'sold_qty',
        'sub_catalog',
        'catalog_name',
        'merchant',
        'details',
        'ean',
        'nmark'
    ]


def catalogs() -> list[dict]:
    """Чтение каталогов из CSV-файла."""

    filepath = _PARSER_CATALOGS_PATH
    catalogs_list = []
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            catalogs_list.append(row)
    return catalogs_list


def brands() -> list[dict]:
    """Чтение каталогов для брендов из CSV-файла."""
    filepath = _PARSER_BRANDS_PATH
    catalogs_list = []
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            catalogs_list.append(row)
    return catalogs_list

def generate_pages_for_filter(
        catalog_filter: CatalogFilter,
        shard: str,
        query: str,
        xsubject: str | None,
        catalog_type: CatalogType,
        brand_id: str
) -> str:
    """
    Генерирует URL'ы страниц для каталога, но не более MAX_PAGES.
    """
    # безопасный предел страниц
    try:
        total = int(catalog_filter.total_pages)
    except Exception:
        total = 0

    safe_pages = min(total, MAX_PAGES)
    if safe_pages <= 0:
        return

    for page in range(1, safe_pages + 1):
        yield api_products(page, shard, query, catalog_filter.min_price, catalog_filter.max_price, xsubject, catalog_type, brand_id)



def _remove_childs(obj):
    if isinstance(obj, dict):
        if 'childs' in obj:
            del obj['childs']
        for key in obj:
            _remove_childs(obj[key])
    elif isinstance(obj, list):
        for item in obj:
            _remove_childs(item)


def _flatten_categories(categories):
    flattened = {}
    for item in categories:
        flattened.update({item['url']: item})
        if 'childs' in item:
            flattened.update(_flatten_categories(item['childs']))
    _remove_childs(flattened)
    return flattened


def get_menu() -> dict:
    response = get(_MENU_URL)
    if response.status_code == 200:
        return _flatten_categories(json.loads(response.text))


def catalog_groups():
    """Чтение sku из CSV-файла."""
    groups = pd.read_csv(_PARSER_SKUS_PATH, delimiter=';', encoding='utf-8')
    return groups.groupby('catalog_name')