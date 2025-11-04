from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, regexp_replace, sum as _sum, expr
from pyspark.sql.types import IntegerType
import argparse
import re

def main(input_path, output_path):
    spark = SparkSession.builder \
        .appName("COVID_Cases_Transform") \
        .config("spark.sql.debug.maxToStringFields", 2000) \
        .getOrCreate()

    # Read raw Johns Hopkins confirmed cases file, store it as Spark DataFrame object
    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(input_path)
    )

    print("Loaded input data")
    df.printSchema()

    # Identify date columns (format like 1/22/20)
    date_cols = [c for c in df.columns if re.match(r"^\d{1,2}/\d{1,2}/\d{2}$", c)]
    non_date_cols = [c for c in df.columns if c not in date_cols]

    print(f"Date columns detected: {len(date_cols)}")
    print(f"Non-date columns: {non_date_cols}")

    # Unpivot safely using stack()
    n = len(date_cols)
    stack_expr = "stack({0}, {1}) as (date_raw, confirmed_raw)".format(
        n, ", ".join([f"'{c}', `{c}`" for c in date_cols])
    )

    df_unpivot = df.select(*non_date_cols, expr(stack_expr))

    # Rename and clean
    df_clean = (
        df_unpivot
        .withColumnRenamed("Country_Region", "country")
        .withColumnRenamed("Province_State", "state")
        .withColumn("date", to_date(regexp_replace(col("date_raw"), r"\.", "/"), "M/d/yy"))
        .withColumn("confirmed", col("confirmed_raw").cast(IntegerType()))
        .drop("date_raw", "confirmed_raw")
        .dropna(subset=["date", "confirmed"])
    )

    print("After cleaning schema:")
    df_clean.printSchema()

    # Aggregate to country level
    df_country_daily = (
        df_clean.groupBy("country", "date")
        .agg(_sum("confirmed").alias("total_confirmed"))
        .orderBy("country", "date")
    )

    print("Writing output to GCS...")
    df_country_daily.write.mode("overwrite").parquet(f"{output_path}/usa_daily")

    print("Transform complete — data written to:")
    print(f"   {output_path}/usa_daily")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transform COVID wide-format confirmed cases data")
    parser.add_argument("--input", required=True, help="Input CSV path")
    parser.add_argument("--output", required=True, help="Output directory")
    args = parser.parse_args()

    main(args.input, args.output)
