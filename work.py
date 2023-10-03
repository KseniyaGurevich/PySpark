from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ProductCategory").getOrCreate()

products_data = [(1, "product 1"), (2, "product 2"), (3, "product 3")]
categories_data = [(1, "Category 1"), (1, "Category 2"), (1, "Category 3")]
product_category_data = [(1, 1), (1, 2), (2, 1), (1, 3)]

products_df = spark.createDataFrame(products_data, ["ProductID", "ProductName"])
categories_df = spark.createDataFrame(categories_data, ["CategoryID", "CategoryName"])
product_category_df = spark.createDataFrame(product_category_data, ["ProductID", "CategoryID"])

result_df = products_df.join(product_category_df, on=["ProductID"], how="left")
result_df = result_df.join(categories_df, on=["CategoryID"], how="left")

result_df = result_df.select("ProductName", "CategoryName")

result_df.show()