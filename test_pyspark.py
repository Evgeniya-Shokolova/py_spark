import os
from pyspark.sql import SparkSession


def product_category_pairs(products, categories, product_categories):
    joined_df = products.join(product_categories,
                              "product_id", "left").join(categories,
                                                         "category_id", "left")
    return joined_df.select("product_name", "category_name")


os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-8-openjdk-amd64'
spark = SparkSession.builder.appName("test").getOrCreate()

products = spark.createDataFrame([
    (1, "Продукт A"),
    (2, "Продукт B"),
    (3, "Продукт C")
], ["product_id", "product_name"])

categories = spark.createDataFrame([
    (10, "Категория X"),
    (20, "Категория Y")
], ["category_id", "category_name"])

product_categories = spark.createDataFrame([
    (1, 10),
    (1, 20),
    (2, 10)
], ["product_id", "category_id"])

result = product_category_pairs(products, categories, product_categories)
result.show(truncate=False)
