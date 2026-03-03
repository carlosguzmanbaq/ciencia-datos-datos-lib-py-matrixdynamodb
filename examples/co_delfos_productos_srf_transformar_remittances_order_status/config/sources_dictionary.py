from dynamodb_curated_library.models.storage_models import TableSourceConfig


def get_raw_dynamodb_source(process_type: str) -> TableSourceConfig:
    """
    Get DynamoDB source configuration based on process type.

    Args:
        process_type: 'FULL' or 'INC'

    Returns:
        TableSourceConfig with appropriate partitioning
    """
    base_prefix = (
        "{domain}_uncrawlable/{current_subdomain}/{country}_internal_expody/"
        "{export_folder_name}/{current_dynamodb_process_type_folder}/"
        "year={process_year}/month={process_month}/day={process_day}/"
    )

    # Add hour partition for incremental loads
    if process_type == "INC":
        base_prefix += "hour={process_hour}/"

    return {
        "bucket": "{raw_bucket}",
        "prefix": base_prefix
    }
