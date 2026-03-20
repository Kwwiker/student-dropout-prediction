import pandas as pd
from fastapi import HTTPException
from app.services.extractors import ActivityExtractor


class CSVReader:
    @staticmethod
    def read(file_path: str):
        try:
            df = pd.read_csv(file_path)
        except pd.errors.EmptyDataError:
            raise HTTPException(
                status_code=400,
                detail="CSV-файл пустой"
            )
        except Exception:
            raise HTTPException(
                status_code=400,
                detail="Не удалось прочитать CSV-файл"
            )

        if df.empty:
            raise HTTPException(
                status_code=400,
                detail="CSV-файл не содержит строк данных"
            )

        return df


class ExcelReader:
    @staticmethod
    def read(file_path: str):
        try:
            excel_file = pd.ExcelFile(file_path)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Не удалось открыть Excel-файл"
            )
        except Exception:
            raise HTTPException(
                status_code=400,
                detail="Ошибка при чтении Excel-файла"
            )

        sheet_names = excel_file.sheet_names
        if not sheet_names:
            raise HTTPException(
                status_code=400,
                detail="Excel-файл не содержит листов"
            )

        first_sheet_name = sheet_names[0]
        raw_df = pd.read_excel(file_path, sheet_name=first_sheet_name, header=None)
        if raw_df.empty:
            raise HTTPException(
                status_code=400,
                detail=f"Первый лист Excel-файла пустой: {first_sheet_name}"
            )

        return raw_df

    # @staticmethod
    # def _read_raw(file_path: str, sheet_name: str):
    #     try:
    #         return pd.read_excel(file_path, sheet_name=sheet_name, header=None)
    #     except Exception:
    #         raise HTTPException(
    #             status_code=400,
    #             detail=f"Не удалось прочитать лист Excel-файла: {sheet_name}"
    #         )
