from pydantic import BaseModel


class FileStructureInfo(BaseModel):
    rows_count: int
    columns_count: int
    columns: list[str]
    normalized_columns: list[str]
    detected_user_id_column: str
    standardized_user_id_column: str
    requires_period_detection: bool
    sheets: list[str] | None = None
