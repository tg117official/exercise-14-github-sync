# Databricks notebook source
from pyspark.sql import functions as F

records = [
    ("C101", "Anaya", "Active", 1200),
    ("C102", "Rahul", "Inactive", 800),
    ("C103", "Riya", "Active", 0),
    ("C104", "Meera", "Pending", 500),
    (None, "Tarun", "Active", 900),
    ("C106", "Zoya", "Active", -100),
    ("C107", "Arjun", "Active", 650),
]

customers = spark.createDataFrame(
    records,
    "customer_id STRING, customer_name STRING, status STRING, order_amount INT"
)

basic_rule = (
    F.col("customer_id").isNotNull()
    & F.col("status").isin("Active", "Inactive")
)

valid_rows = customers.filter(basic_rule)
rejected_rows = customers.filter(~basic_rule)

print("Basic checks - valid:", valid_rows.count())
print("Basic checks - rejected:", rejected_rows.count())

# COMMAND ----------
# Accepted customer orders must have an amount greater than zero.
amount_rejections = valid_rows.filter(F.col("order_amount") <= 0)
final_valid = valid_rows.filter(F.col("order_amount") > 0)
final_rejected = rejected_rows.unionByName(amount_rejections)

print("After amount check - valid:", final_valid.count())
print("After amount check - rejected:", final_rejected.count())
final_valid.show(truncate=False)
