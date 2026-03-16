import pandas as pd
import re
from datetime import datetime, date
from fastapi import HTTPException
from app.core.feature_rules import PROGRESS_FEATURE_RULES
from app.utils.dataframe import normalize_column_name


class ProgressExtractor:
    PERIOD_DEPENDENT_METRICS = {
        "процент_сдачи_дз_с_решением_до_выбранной_даты",
        "средний_балл_по_всем_дз_с_решением_до_выбранной_даты",
    }

    @staticmethod
    def is_progress_special_format(columns: list[str]):
        return any(metric in columns for metric in ProgressExtractor.PERIOD_DEPENDENT_METRICS)

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
    def extract_period_dates_from_filters(filters_preview: list[str]):
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
    def add_progress_features(prepared_df: pd.DataFrame, result_df: pd.DataFrame,
                              found_features: list[str], missing_features: list[str]):
        filters_preview = ProgressExtractor.extract_pbi_filters_preview(prepared_df)

        if not filters_preview:
            for feature_name in PROGRESS_FEATURE_RULES.keys():
                missing_features.append(feature_name)
            return

        date_from, date_to = ProgressExtractor.extract_period_dates_from_filters(filters_preview)

        if not date_from or not date_to:
            for feature_name in PROGRESS_FEATURE_RULES.keys():
                missing_features.append(feature_name)
            return

        period_types = ProgressExtractor.detect_period_types(date_from, date_to)

        if not period_types:
            for feature_name in PROGRESS_FEATURE_RULES.keys():
                missing_features.append(feature_name)
            return

        for feature_name, rule in PROGRESS_FEATURE_RULES.items():
            normalized_column = normalize_column_name(rule["column"])

            if normalized_column not in prepared_df.columns:
                missing_features.append(feature_name)
                continue

            source_series = prepared_df[normalized_column]

            for period_type in period_types:
                final_feature_name = f"{feature_name}_{period_type}"
                result_df[final_feature_name] = source_series.copy()
                found_features.append(final_feature_name)


class ActivityExtractor:

    @staticmethod
    def is_activity_special_format(raw_df: pd.DataFrame):
        """
        Идентифицирует файл активности учеников по значению "День"
        в левой верхней ячейке
        """
        if raw_df.empty:
            return False

        top_left_value = raw_df.iloc[0, 0]

        if pd.isna(top_left_value):
            return False

        return str(top_left_value).strip() == "День"

    @staticmethod
    def build_activity_dataframe(raw_df: pd.DataFrame):
        """
        Собирает файл активности учеников, преобразуя две строки
        заголовков в одну. Заголовки берутся из второй строки
        пока не встретится "user_action_days_count_every_day",
        начиная с первого нахождения берутся заголовки первой
        """
        if raw_df.shape[0] < 3:
            raise HTTPException(
                status_code=400,
                detail="Файл активности слишком короткий"
            )

        header_row_1 = raw_df.iloc[0]
        header_row_2 = raw_df.iloc[1]
        data_df = raw_df.iloc[2:].copy()

        columns = []
        use_first_row_for_headers = False

        for value_row_1, value_row_2 in zip(header_row_1, header_row_2):
            first_value = str(value_row_1).strip()
            second_value = str(value_row_2).strip()

            if second_value.lower() == "user_action_days_count_every_day":
                use_first_row_for_headers = True

            if use_first_row_for_headers:
                column_name = first_value
            else:
                column_name = second_value

            columns.append(column_name)

        data_df.columns = columns
        return data_df.reset_index(drop=True)

    @staticmethod
    def add_activity_features(prepared_df: pd.DataFrame, result_df: pd.DataFrame, found_features: list[str],
                              missing_features: list[str]):
        total_column_index = ActivityExtractor._find_total_column_index(prepared_df.columns.tolist())

        if total_column_index is None:
            missing_features.append("active_days_pct")
            return

        if total_column_index == 0:
            missing_features.append("active_days_pct")
            return

        last_day_column_name = prepared_df.columns[total_column_index - 1]
        days_in_export = ActivityExtractor._extract_day_number_from_column_name(last_day_column_name)

        if not days_in_export or days_in_export <= 0:
            missing_features.append("active_days_pct")
            return

        total_series = pd.to_numeric(
            prepared_df.iloc[:, total_column_index],
            errors="coerce",
        )

        result_df["active_days_pct"] = (total_series / days_in_export).clip(upper=1)
        found_features.append("active_days_pct")

    @staticmethod
    def _find_total_column_index(columns: list):
        for index, column in enumerate(columns):
            if str(column).strip().lower() == "total":
                return index
        return None

    @staticmethod
    def _extract_day_number_from_column_name(column_name):
        column_text = str(column_name).strip()

        lines = [line.strip() for line in column_text.splitlines() if line.strip()]
        if not lines:
            return None

        first_line = lines[0]

        if first_line.isdigit():
            return int(first_line)

        return None
