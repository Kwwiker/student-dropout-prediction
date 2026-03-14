from pydantic import BaseModel


class FileStructureInfo(BaseModel):
    rows_count: int
    columns_count: int
    columns: list[str]
    sheets: list[str] | None = None
