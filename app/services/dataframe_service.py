import re
from datetime import datetime, date

import pandas as pd
from fastapi import HTTPException

from app.schemas.file_info import FileStructureInfo
from app.utils.dataframe import normalize_column_names


class DataFrameService:
    USER_ID_PATTERN = "user_id"
    STANDARD_USER_ID_COLUMN = "user_id"

    PERIOD_DEPENDENT_METRICS = {
        "процент_сдачи_дз_с_решением_до_выбранной_даты",
        "средний_балл_по_всем_дз_с_решением_до_выбранной_даты",
    }

    @staticmethod
    def prepare_dataframe(df: pd.DataFrame):
        normalized_columns = normalize_column_names(df.columns.tolist())

        prepared_df = df.copy()
        prepared_df.columns = normalized_columns

        detected_user_id_column = DataFrameService._find_user_id_column(prepared_df.columns.tolist())
        prepared_df = DataFrameService._standardize_user_id_column(prepared_df, detected_user_id_column)

        return prepared_df, normalized_columns, detected_user_id_column

    @staticmethod
    def extract_file_structure_info(df: pd.DataFrame, sheet_names: list[str] | None = None):
        original_columns = df.columns.tolist()

        prepared_df, normalized_columns, detected_user_id_column = DataFrameService.prepare_dataframe(df)

        requires_period_detection = DataFrameService.requires_period_detection(normalized_columns)

        pbi_filters_preview = None
        pbi_filters_found = False
        period_date_from = None
        period_date_to = None
        detected_period_types = None
        period_warning = None

        if requires_period_detection:
            pbi_filters_preview = DataFrameService.extract_pbi_filters_preview(df)
            pbi_filters_found = pbi_filters_preview is not None

            if pbi_filters_found:
                date_from, date_to = DataFrameService.extract_period_dates(pbi_filters_preview)

                if date_from and date_to:
                    period_date_from = date_from.strftime("%d.%m.%Y")
                    period_date_to = date_to.strftime("%d.%m.%Y")
                    detected_period_types = DataFrameService.detect_period_types(date_from, date_to)

                    if not detected_period_types:
                        period_warning = (
                            "Загруженные данные не похожи на диапазон за неделю или месяц, "
                            "поэтому периодо-зависимые колонки из этой таблицы не будут использованы. "
                            "Остальные подходящие колонки будут обработаны."
                        )

        return FileStructureInfo(
            rows_count=prepared_df.shape[0],
            columns_count=prepared_df.shape[1],
            columns=original_columns,
            normalized_columns=normalized_columns,
            detected_user_id_column=detected_user_id_column,
            standardized_user_id_column=DataFrameService.STANDARD_USER_ID_COLUMN,
            requires_period_detection=requires_period_detection,
            pbi_filters_found=pbi_filters_found,
            pbi_filters_preview=pbi_filters_preview,
            period_date_from=period_date_from,
            period_date_to=period_date_to,
            detected_period_types=detected_period_types,
            period_warning=period_warning,
            sheets=sheet_names,
        )

    @staticmethod
    def requires_period_detection(columns: list[str]):
        return any(metric in columns for metric in DataFrameService.PERIOD_DEPENDENT_METRICS)

    @staticmethod
    def extract_pbi_filters_preview(df: pd.DataFrame):
        if df.empty or df.shape[1] == 0:
            return None

        first_column = df.iloc[:, 0].dropna()

        if first_column.empty:
            return None

        last_value = str(first_column.iloc[-1]).strip()

        if "Примененные фильтры:" not in last_value:
            return None

        preview_lines = [line.strip() for line in last_value.splitlines() if line.strip()]

        return preview_lines[:10] if preview_lines else None

    @staticmethod
    def extract_period_dates(filters_preview: list[str]):
        date_pattern = r"(\d{2}\.\d{2}\.\d{4})"

        for line in filters_preview:
            if "Date начиная с" not in line:
                continue

            matches = re.findall(date_pattern, line)

            if len(matches) < 2:
                return None, None

            try:
                date_from = datetime.strptime(matches[0], "%d.%m.%Y").date()
                date_to = datetime.strptime(matches[1], "%d.%m.%Y").date()
                return date_from, date_to
            except ValueError:
                return None, None

        return None, None

    @staticmethod
    def detect_period_types(date_from: date, date_to: date):
        days_in_period = (date_to - date_from).days + 1
        period_types = []

        if days_in_period == 8:
            period_types.append("week")

        if date_from.day == 1:
            period_types.append("month")

        return period_types

    @staticmethod
    def _find_user_id_column(columns: list[str]):
        for column in columns:
            if DataFrameService.USER_ID_PATTERN in column:
                return column

        raise HTTPException(status_code=400,
                            detail=f"В файле отсутствует колонка с идентификатором ученика: в названии должно быть 'user_id'"
                            )

    @staticmethod
    def _standardize_user_id_column(df: pd.DataFrame, detected_user_id_column: str):
        if detected_user_id_column != DataFrameService.STANDARD_USER_ID_COLUMN:
            df = df.rename(columns={detected_user_id_column: DataFrameService.STANDARD_USER_ID_COLUMN})

        return df
