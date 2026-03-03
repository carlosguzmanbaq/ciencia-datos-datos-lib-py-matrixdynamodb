from dataclasses import dataclass

@dataclass
class S3File:
    """
    Dataclass for handle:
     :bucket Database name
     :prefix Table Name
     :origin Aditional options o paramters for data extraction
    """
    bucket: str
    prefix: str

    @property
    def s3_uri(self) -> str:
        return f"s3://{self.bucket}/{self.prefix}"


@dataclass
class Table:
    """
    Dataclass for handle:
     :database Database name
     :table_name Table Name
     :additional_options  Aditional options o paramters for data extraction
    """
    database: str
    table_name: str
