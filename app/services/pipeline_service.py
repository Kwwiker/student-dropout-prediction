from http.client import HTTPException

import pandas as pd
from fastapi import HTTPException

from app.schemas.file_info import FileStructureInfo

class PipelineService:
    @staticmethod
    def process_uploaded_file(file_path: str, extension: str):
        if extension == ".csv":
            return PipelineService._process_csv(file_path)
        elif extension == ".xlsx":
            return PipelineService._process_excel(file_path)
        raise HTTPException(status_code=400, detail=f"Неподдерживаемое расширение файла: {extension}")

    @staticmethod
    def _process_csv(file_path: str):
        try:
            df = pd.read_csv(file_path)
        except pd.errors.EmptyDataError:
            raise HTTPException(status_code=400, detail="CSV-файл пустой")
        except Exception:
            raise HTTPException(status_code=400, detail="Не удалось прочитать CSV-файл")

        if df.empty:
            raise HTTPException(status_code=400, detail="CSV-файл не содержит строк данных")

        return FileStructureInfo(
            rows_count=df.shape[0],
            columns_count=df.shape[1],
            columns=df.columns.tolist(),
            sheets=None,
        )

    @staticmethod
    def _process_excel(file_path: str):
        try:
            excel_file = pd.ExcelFile(file_path)
        except ValueError:
            raise HTTPException(status_code=400, detail="Не удалось открыть Excel-файл")
        except Exception:
            raise HTTPException(status_code=400, detail="Ошибка при чтении Excel-файла")

        sheet_names = excel_file.sheet_names

        if not sheet_names:
            raise HTTPException(status_code=400, detail="Excel-файл не содержит листов")

        first_sheet_name = sheet_names[0]

        try:
            df = pd.read_excel(file_path, sheet_name=first_sheet_name)
        except Exception:
            raise HTTPException(status_code=400, detail=f"Не удалось прочитать первый лист Excel-файла: {first_sheet_name}")

        if df.empty:
            raise HTTPException(status_code=400, detail=f"Первый лист Excel-файла пустой: {first_sheet_name}")

        return FileStructureInfo(
            rows_count=df.shape[0],
            columns_count=df.shape[1],
            columns=df.columns.tolist(),
            sheets=sheet_names,
        )