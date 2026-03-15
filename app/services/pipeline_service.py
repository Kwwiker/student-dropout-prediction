from fastapi import HTTPException
from app.services.dataframe_service import DataFrameService
from app.services.readers import CSVReader, ExcelReader


class PipelineService:

    @staticmethod
    def analyze_uploaded_file(file_path: str, extension: str):
        if extension == ".csv":
            df = CSVReader.read(file_path)
            return DataFrameService.extract_file_structure_info(df)

        elif extension == ".xlsx":
            df, sheet_names = ExcelReader.read(file_path)
            return DataFrameService.extract_file_structure_info(df, sheet_names)

        raise HTTPException(
            status_code=400,
            detail=f"Неподдерживаемое расширение файла: {extension}"
        )
