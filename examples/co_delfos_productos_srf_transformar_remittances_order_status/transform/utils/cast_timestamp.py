from pyspark.sql import functions as F
from pyspark.sql import DataFrame


def cast_column_timestamp(df: DataFrame, column_name: str) -> DataFrame:
    if column_name not in df.columns:
        df = df.withColumn(column_name, F.lit(None))

    df = df.withColumn(
        column_name,
        F.when(
            # yyyyMMdd
            F.col(column_name).rlike(r"^\d{8}$"),
            F.to_timestamp(F.col(column_name), "yyyyMMdd")
        ).when(
            # Si es un número en milisegundos (probable timestamp en UTC)
            F.col(column_name).rlike(r"^\d+$"),
            F.from_utc_timestamp(
                F.from_unixtime(F.col(column_name).cast("long") / 1000).cast("timestamp"),
                "America/Bogota"
            )
        ).when(
            # Si la fecha tiene la "Z" (indica UTC), la convertimos a UTC-5
            F.col(column_name).rlike(r"Z$"),
            F.from_utc_timestamp(
                F.to_timestamp(F.col(column_name), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
                "America/Bogota"
            )
        ).otherwise(
            # Si no tiene "Z", asumimos que ya está en la zona horaria correcta y la dejamos igual
            F.to_timestamp(F.col(column_name))
        )
    )

    return df
