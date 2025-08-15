from dataclasses import dataclass


@dataclass
class CatalogFilter:
    name:           str
    total_pages:    int
    total_items:    int
    min_price:      int
    max_price:      int

    def __str__(self):
        return f"{self.name} {self.total_pages} стр. {self.total_items} тов. от {self.min_price // 100}₽ до {self.max_price // 100}₽"
