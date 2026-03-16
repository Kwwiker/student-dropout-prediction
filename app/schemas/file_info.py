from pydantic import BaseModel


class FileStructureInfo(BaseModel):
    rows_count: int
    columns_count: int
    columns: list[str]
    normalized_columns: list[str]
    detected_user_id_column: str
    standardized_user_id_column: str
    progress_special_format: bool
    pbi_filters_found: bool
    pbi_filters_preview: list[str] | None = None
    period_date_from: str | None = None
    period_date_to: str | None = None
    detected_period_types: list[str] | None = None
    period_warning: str | None = None
    sheets: list[str] | None = None
