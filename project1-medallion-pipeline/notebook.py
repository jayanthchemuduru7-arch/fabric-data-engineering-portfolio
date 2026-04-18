# Project 1 - Medallion Architecture Pipeline
# Microsoft Fabric Lakehouse | PySpark | Delta Lake

# ── Cell 1: Inspect Raw Data ──────────────────────────
df = spark.read.option("header", True) \
    .option("inferSchema", True) \
    .csv("Files/sample_employee_data.csv")

df.printSchema()
df.show(5)

# ── Cell 2: Bronze Layer ──────────────────────────────
df_bronze = spark.read.option("header", True) \
    .option("inferSchema", True) \
    .csv("Files/sample_employee_data.csv")

df_bronze.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_employees")

print("Bronze layer done:", df_bronze.count(), "rows")

# ── Cell 3: Silver Layer ──────────────────────────────
from pyspark.sql.functions import col, when

df_silver = spark.read.format("delta").table("bronze_employees")

df_silver = df_silver \
    .dropna(subset=["EmployeeID", "Salary", "Department"]) \
    .withColumn("Salary", col("Salary").cast("double")) \
    .withColumn("SalaryBand", when(col("Salary") < 55000, "Low")
                              .when(col("Salary") < 70000, "Mid")
                              .otherwise("High"))

df_silver.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_employees")

print("Silver layer done:", df_silver.count(), "rows")
df_silver.show()

# ── Cell 4: Gold Layer ────────────────────────────────
from pyspark.sql.functions import avg, count, round, max, min

df_gold = spark.read.format("delta").table("silver_employees")

df_gold = df_gold.groupBy("Department") \
    .agg(
        count("EmployeeID").alias("total_employees"),
        round(avg("Salary"), 2).alias("avg_salary"),
        max("Salary").alias("max_salary"),
        min("Salary").alias("min_salary")
    ).orderBy("avg_salary", ascending=False)

df_gold.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_department_summary")

print("Gold layer done!")
df_gold.show()

# ── Cell 5: Verify All Layers ─────────────────────────
print("=== BRONZE ===")
spark.read.format("delta").table("bronze_employees").show(3)

print("=== SILVER ===")
spark.read.format("delta").table("silver_employees").show(3)

print("=== GOLD ===")
spark.read.format("delta").table("gold_department_summary").show()
