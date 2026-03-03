from dataclasses import dataclass
import logging

import yaml

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField

from dynamodb_curated_library.core.config.constants import Constants
from dynamodb_curated_library.core.config.job_config import JobConfig
from dynamodb_curated_library.utils.make_path import get_metadata

logger = logging.getLogger(__name__)


@dataclass
class MetadataConfig:
    """
    Configuration for schema metadata loading and application.

    Args:
        file_name: Name of the YAML metadata file
        module: Module reference for path resolution
        target_module: Target module name for path resolution
    """
    file_name: str
    constants: Constants
    target_module: str

    def load_metadata(self) -> dict:
        """
        Load schema metadata from YAML file.

        Returns:
            dict: Schema metadata
        """
        metadata_path = get_metadata(
            file_name=f"{self.file_name}_metadata",
            module=self.constants.metadata_module,
            target_module=f"{self.constants.package_name}/{self.target_module}"
        )
        logger.info("Loading metadata from: %s", metadata_path)

        with open(metadata_path, 'r', encoding='utf-8') as file:
            return yaml.safe_load(file)

    def apply_to_dataframe(self, df: DataFrame) -> DataFrame:
        """
        Apply schema metadata to DataFrame.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with metadata applied
        """
        metadata = self.load_metadata()

        # Crear diccionario de comentarios por columna
        column_comments = {}
        for column_info in metadata.get('columns', []):
            column_name = column_info['name']
            description = column_info['description']
            if column_name in df.columns:
                column_comments[column_name] = description

        # Aplicar comentarios al schema
        if column_comments:
            new_fields = []
            for field in df.schema.fields:
                if field.name in column_comments:
                    new_field = StructField(
                        field.name,
                        field.dataType,
                        field.nullable,
                        {"comment": column_comments[field.name]}
                    )
                    new_fields.append(new_field)
                else:
                    new_fields.append(field)

            new_schema = StructType(new_fields)
            df = df.sparkSession.createDataFrame(df.rdd, new_schema)

        return df

    def add_metadata_columns(self, df: DataFrame, job_config: JobConfig) -> DataFrame:
        """
        Add metadata columns and apply schema metadata from YAML.

        Args:
            df: Input DataFrame
            job_config: Job configuration with process information
            partition_field: Optional field name for partitioning columns

        Returns:
            DataFrame with metadata applied
        """
        # Reorder columns: move flag_event_status before momento_ingestion if exists

        # Agregar columnas base de metadata
        df = (
            df
            .withColumn("momento_ingestion", F.from_utc_timestamp(F.current_timestamp(), "America/Bogota"))
            .withColumn("job_process_date", F.lit(job_config.start_process_date))
            .withColumn("job_process_type", F.lit(job_config.process_type))
        )

        # Agregar columnas de partición si se especifica partition_field
        if job_config.constants.partition_field:
            df = (
                df
                .withColumn("year", F.year(F.col(job_config.constants.partition_field)).cast("string"))
                .withColumn("month", F.format_string("%02d", F.month(F.col(job_config.constants.partition_field))))
                .withColumn("day", F.format_string("%02d", F.dayofmonth(F.col(job_config.constants.partition_field))))
            )

        # Aplicar metadatos del schema
        return self.apply_to_dataframe(df)
