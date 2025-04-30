from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def product_category_pairs(products: DataFrame,
                           categories: DataFrame,
                           product_categories: DataFrame) -> DataFrame:
    """
    Возвращает датафрейм со всеми парами
    "Имя продукта – Имя категории" и именами продуктов без категорий.

    Аргументы:
        products - DataFrame с продуктами,
            должен содержать столбцы: product_id, product_name
        categories - DataFrame с категориями,
            должен содержать столбцы: category_id, category_name
        product_categories- DataFrame с связями "продукт-категория",
            должен содержать столбцы: product_id, category_id

    Возвращает DataFrame с двумя столбцами:
      - product_name
      - category_name (NULL, если категории нет)
    """

    # Соединяем продукты с категориями через связи
    # (left join, чтобы сохранить продукты без категорий)
    joined = products.alias('p') \
        .join(product_categories.alias('pc'),
              F.col('p.product_id') == F.col('pc.product_id'), 'left') \
        .join(categories.alias('c'),
              F.col('pc.category_id') == F.col('c.category_id'), 'left') \
        .select(F.col('p.product_name'), F.col('c.category_name'))

    return joined
