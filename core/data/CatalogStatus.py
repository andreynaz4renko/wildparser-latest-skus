from enum import Enum


class CatalogStatus(Enum):
    ENQUEUED = 'enqueued'
    FAILURE = 'failure'
    DONE = 'done'

class CatalogType(Enum):
    CATALOG = 'catalog'
    BRAND = 'brand'
