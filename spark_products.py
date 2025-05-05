from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit

def get_product_category_pairs(products_df: DataFrame, categories_df: DataFrame, product_category_links_df: DataFrame) -> DataFrame:
    """
    Возвращает датафрейм со всеми парами "Имя продукта – Имя категории" и продуктами без категорий.
    
    Параметры:
    - products_df: Датафрейм продуктов (должен содержать колонки 'product_id' и 'product_name')
    - categories_df: Датафрейм категорий (должен содержать колонки 'category_id' и 'category_name')
    - product_category_links_df: Датафрейм связей продукт-категория 
                               (должен содержать колонки 'product_id' и 'category_id')
    
    Возвращает:
    - Датафрейм с колонками 'product_name' и 'category_name', где category_name будет NULL для продуктов без категорий
    """
    # Соединяем продукты с их категориями через таблицу связей
  
    products_with_categories = (
        products_df.join(
            product_category_links_df, 
            on="product_id", 
            how="left"
        )
        .join(
            categories_df, 
            on="category_id", 
            how="left"
        )
    )
    
    # Выбираем нужные колонки и добавляем продукты без категорий
    result_df = (
        products_with_categories
        .select(
            col("product_name"),
            col("category_name")
        )
        .union(
            products_df
            .join(
                product_category_links_df,
                on="product_id",
                how="left_anti"
            )
            .select(
                col("product_name"),
                lit(None).alias("category_name")
            )
        )
    )
    
    return result_df

# Пример входных данных
products_data = [("p1", "Product 1"), ("p2", "Product 2"), ("p3", "Product 3")]
categories_data = [("c1", "Category 1"), ("c2", "Category 2")]
links_data = [("p1", "c1"), ("p1", "c2"), ("p2", "c1")]

# Создаем датафреймы
products_df = spark.createDataFrame(products_data, ["product_id", "product_name"])
categories_df = spark.createDataFrame(categories_data, ["category_id", "category_name"])
links_df = spark.createDataFrame(links_data, ["product_id", "category_id"])

# Вызываем функцию
result = get_product_category_pairs(products_df, categories_df, links_df)
result.show()
