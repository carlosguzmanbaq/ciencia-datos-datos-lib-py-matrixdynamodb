from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import DataFrame


def cast_column_to_timestamp(df: DataFrame, column_name: str) -> DataFrame: # pragma: no cover
    """
    Convierte una columna a timestamp manejando múltiples formatos de fecha.

    Args:
        df: DataFrame de PySpark
        column_name: Nombre de la columna a convertir

    Returns:
        DataFrame con la columna convertida a timestamp
    """
    if column_name not in df.columns:
        df = df.withColumn(column_name, F.lit(None))

    df = df.withColumn(
        column_name,
        F.when(
            # Formato ISO con Z: 2024-12-12T17:29:34.042653176Z
            F.col(column_name).rlike(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z$"),
            F.from_utc_timestamp(
                F.to_timestamp(
                    F.regexp_replace(
                        F.col(column_name),
                        r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.)\d*(Z)",
                        "$1000$2"
                    ),
                    "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
                ),
                "America/Bogota"
            )
        ).when(
            # Formato con microsegundos y PM/AM: 12/12/24 04:31:52,211651540 PM
            F.col(column_name).rlike(r"\d{1,2}/\d{1,2}/\d{2}\s+\d{1,2}:\d{2}:\d{2},\d+\s+(AM|PM)$"),
            F.from_unixtime(
                F.unix_timestamp(
                    F.regexp_replace(
                        F.col(column_name),
                        r"(\d{1,2})/(\d{1,2})/(\d{2})\s+(\d{1,2}:\d{2}:\d{2}),\d*\s+(AM|PM)",
                        "$1/$2/20$3 $4 $5"
                    ),
                    "d/M/yyyy h:mm:ss a"
                )
            )
        ).when(
            # Si es numérico y tiene más de 10 dígitos → milisegundos
            F.col(column_name).rlike(r"^\d{13,}$"),
            F.from_utc_timestamp(
                F.from_unixtime(F.col(column_name).cast("double") / 1000),
                "America/Bogota"
            )
        ).when(
            # Si es numérico y tiene 10 dígitos → segundos
            F.col(column_name).rlike(r"^\d{10}$"),
            F.from_utc_timestamp(
                F.from_unixtime(F.col(column_name).cast("double")),
                "America/Bogota"
            )
        ).otherwise(
            F.to_timestamp(F.col(column_name))
        ).cast(T.TimestampType())
    )

    return df


def cast_column_to_date(df: DataFrame, column_name: str) -> DataFrame:
    """
    Convierte una columna a tipo Date manejando múltiples formatos de fecha.

    Args:
        df: DataFrame de PySpark
        column_name: Nombre de la columna a convertir

    Returns:
        DataFrame con la columna convertida a Date
    """
    if column_name not in df.columns:
        df = df.withColumn(column_name, F.lit(None))

    df = df.withColumn(
        column_name,
        F.when(
            # Formato simple: 12/12/24 -> date YYYY-MM-dd
            F.col(column_name).rlike(r"^\d{1,2}/\d{1,2}/\d{2}$"),
            F.to_date(
                F.regexp_replace(
                    F.col(column_name),
                    r"(\d{1,2})/(\d{1,2})/(\d{2})",
                    "20$3-$2-$1"
                ),
                "yyyy-MM-dd"
            )
        ).when(
            # Formato con año completo: 18/10/2024
            F.col(column_name).rlike(r"^\d{1,2}/\d{1,2}/\d{4}$"),
            F.to_date(
                F.regexp_replace(
                    F.col(column_name),
                    r"(\d{1,2})/(\d{1,2})/(\d{4})",
                    "$3-$2-$1"
                ),
                "yyyy-MM-dd"
            )
        ).when(
            # Formato ISO: 2024-10-18
            F.col(column_name).rlike(r"^\d{4}-\d{2}-\d{2}$"),
            F.to_date(F.col(column_name), "yyyy-MM-dd")
        ).otherwise(
            F.to_date(F.col(column_name))
        ).cast(T.DateType())
    )

    return df