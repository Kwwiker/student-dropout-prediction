from http.client import HTTPException

import pandas as pd
from fastapi import HTTPException

from app.schemas.file_info import FileStructureInfo
from app.utils.dataframe import normalize_column_names

class PipelineService:
    USER_ID_PATTERN = "user_id"
    STANDARD_USER_ID_COLUMN = "user_id"
    PERIOD_DEPENDENT_METRICS = {
        "процент_сдачи_дз_с_решением_до_выбранной_даты",
        "средний_балл_по_всем_дз_с_решением_до_выбранной_даты",
    }

    @staticmethod
    def process_uploaded_file(file_path: str, extension: str):
        if extension == ".csv":
            return PipelineService._process_csv(file_path)
        elif extension == ".xlsx":
            return PipelineService._process_excel(file_path)
        raise HTTPException(status_code=400,
                            detail=f"Неподдерживаемое расширение файла: {extension}"
                            )

    @staticmethod
    def _process_csv(file_path: str):
        try:
            df = pd.read_csv(file_path)
        except pd.errors.EmptyDataError:
            raise HTTPException(status_code=400,
                                detail="CSV-файл пустой"
                                )
        except Exception:
            raise HTTPException(status_code=400,
                                detail="Не удалось прочитать CSV-файл"
                                )

        if df.empty:
            raise HTTPException(status_code=400,
                                detail="CSV-файл не содержит строк данных"
                                )

        return PipelineService._build_file_structure_info(df)

    @staticmethod
    def _process_excel(file_path: str):
        try:
            excel_file = pd.ExcelFile(file_path)
        except ValueError:
            raise HTTPException(status_code=400,
                                detail="Не удалось открыть Excel-файл"
                                )
        except Exception:
            raise HTTPException(status_code=400,
                                detail="Ошибка при чтении Excel-файла"
                                )

        sheet_names = excel_file.sheet_names

        if not sheet_names:
            raise HTTPException(status_code=400,
                                detail="Excel-файл не содержит листов"
                                )

        first_sheet_name = sheet_names[0]

        try:
            df = pd.read_excel(file_path, sheet_name=first_sheet_name)
        except Exception:
            raise HTTPException(status_code=400,
                                detail=f"Не удалось прочитать первый лист Excel-файла: {first_sheet_name}"
                                )

        if df.empty:
            raise HTTPException(status_code=400,
                                detail=f"Первый лист Excel-файла пустой: {first_sheet_name}"
                                )

        return PipelineService._build_file_structure_info(df, sheet_names)

    @staticmethod
    def _build_file_structure_info(df: pd.DataFrame, sheet_names: list[str] | None = None):
        original_columns = df.columns.tolist()

        prepared_df, normalized_columns, detected_user_id_column = PipelineService._prepare_dataframe(df)

        requires_period_detection = PipelineService._requires_period_detection(
            normalized_columns
        )

        return FileStructureInfo(
            rows_count=prepared_df.shape[0],
            columns_count=prepared_df.shape[1],
            columns=original_columns,
            normalized_columns=normalized_columns,
            detected_user_id_column=detected_user_id_column,
            standardized_user_id_column=PipelineService.STANDARD_USER_ID_COLUMN,
            requires_period_detection=requires_period_detection,
            sheets=sheet_names,
        )

    @staticmethod
    def _prepare_dataframe(df: pd.DataFrame):
        normalized_columns = normalize_column_names(df.columns.tolist())

        prepared_df = df.copy()
        prepared_df.columns = normalized_columns

        detected_user_id_column = PipelineService._find_user_id_column(
            prepared_df.columns.tolist()
        )

        prepared_df = PipelineService._standardize_user_id_column(
            df=prepared_df,
            detected_user_id_column=detected_user_id_column
        )

        return prepared_df, normalized_columns, detected_user_id_column

    @staticmethod
    def _find_user_id_column(columns: list[str]):
        for column in columns:
            if PipelineService.USER_ID_PATTERN in column:
                return column

        raise HTTPException(status_code=400,
                            detail=f"В файле отсутствует колонка с идентификатором ученика: в названии должно быть 'user_id'"
                            )

    @staticmethod
    def _standardize_user_id_column(df: pd.DataFrame, detected_user_id_column: str):
        if detected_user_id_column != PipelineService.STANDARD_USER_ID_COLUMN:
            df = df.rename(columns={detected_user_id_column: PipelineService.STANDARD_USER_ID_COLUMN})

        return df

    @staticmethod
    def _requires_period_detection(columns: list[str]) -> bool:
        return any(metric in columns for metric in PipelineService.PERIOD_DEPENDENT_METRICS)
