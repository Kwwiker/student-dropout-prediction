from pydantic import BaseModel


class UploadResponse(BaseModel):
    message: str
    original_filename: str
    stored_filename: str
    file_path: str
