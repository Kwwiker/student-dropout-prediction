import re
from datetime import datetime

import pandas as pd


class MLPreparationService:
    SURVEY_RATING_COLUMNS = [
        "presentation_rating",
        "load_difficulty_rating",
        "subject_choice_assurance",
    ]

    SURVEY_CATEGORICAL_COLUMNS = [
        "time_search_rating",
        "school_grade",
        "target_score",
        "time_spend_amount",
        "education_style",
    ]

    PERCENT_COLUMNS = [
        "completed_hw_pct_week",
        "completed_hw_pct_month",
        "earned_points_pct_week",
        "earned_points_pct_month",
        "active_days_pct",
    ]

    @staticmethod
    def prepare_for_model(df: pd.DataFrame):
        prepared_df = df.copy()

        prepared_df = MLPreparationService._normalize_empty_values(prepared_df)
        prepared_df = MLPreparationService._drop_rows_without_current_month(prepared_df)
        prepared_df = MLPreparationService._normalize_user_id(prepared_df)
        prepared_df = MLPreparationService._fill_percent_columns_with_zero(prepared_df)
        prepared_df = MLPreparationService._round_percent_columns(prepared_df)
        prepared_df = MLPreparationService._clean_survey_rating_columns(prepared_df)
        prepared_df = MLPreparationService._fill_survey_missing_values(prepared_df)
        prepared_df = MLPreparationService._extract_education_track(prepared_df)
        prepared_df = MLPreparationService._build_payment_offset_days(prepared_df)

        return prepared_df

    @staticmethod
    def _normalize_empty_values(df: pd.DataFrame):
        df = df.copy()

        empty_like_values = {"", "nan", "none", "nat", "null"}

        for column in df.columns:
            if df[column].dtype == "object":
                df[column] = df[column].apply(
                    lambda value: None
                    if pd.isna(value) or str(value).strip().lower() in empty_like_values
                    else str(value).strip()
                )

        return df

    @staticmethod
    def _drop_rows_without_current_month(df: pd.DataFrame):
        if "current_month" not in df.columns:
            return df

        return df.dropna(subset=["current_month"]).copy()

    @staticmethod
    def _normalize_user_id(df: pd.DataFrame):
        if "user_id" not in df.columns:
            return df

        def clean_user_id(value):
            if pd.isna(value):
                return None

            text = str(value).strip()

            if not text:
                return None

            if text.endswith(".0"):
                text = text[:-2]

            return text

        df["user_id"] = df["user_id"].apply(clean_user_id)
        return df

    @staticmethod
    def _fill_percent_columns_with_zero(df: pd.DataFrame):
        for column in MLPreparationService.PERCENT_COLUMNS:
            if column in df.columns:
                df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0)

        return df

    @staticmethod
    def _round_percent_columns(df: pd.DataFrame):
        for column in MLPreparationService.PERCENT_COLUMNS:
            if column in df.columns:
                df[column] = pd.to_numeric(df[column], errors="coerce").round(2)

        return df

    @staticmethod
    def _clean_survey_rating_columns(df: pd.DataFrame):
        for column in MLPreparationService.SURVEY_RATING_COLUMNS:
            if column not in df.columns:
                continue

            df[column] = df[column].apply(MLPreparationService._extract_rating_value)
            df[column] = df[column].apply(
                lambda value: "нет ответа" if pd.isna(value) else int(value)
            )

        return df

    @staticmethod
    def _fill_survey_missing_values(df: pd.DataFrame):
        all_survey_categorical_columns = (
            MLPreparationService.SURVEY_CATEGORICAL_COLUMNS + ["education_track"]
        )

        for column in all_survey_categorical_columns:
            if column in df.columns:
                df[column] = df[column].apply(
                    lambda value: "нет ответа"
                    if pd.isna(value) or not str(value).strip()
                    else str(value).strip()
                )

        return df

    @staticmethod
    def _extract_education_track(df: pd.DataFrame):
        if "education_track" not in df.columns:
            return df

        def extract_track(value):
            if pd.isna(value):
                return "нет ответа"

            text = str(value).strip()

            if not text:
                return "нет ответа"

            if ":" in text:
                track = text.split(":", 1)[1].strip()
                return track if track else "нет ответа"

            return text

        df["education_track"] = df["education_track"].apply(extract_track)
        return df

    @staticmethod
    def _build_payment_offset_days(df: pd.DataFrame):
        if "transaction_date" not in df.columns or "current_month" not in df.columns:
            return df

        transaction_dates = pd.to_datetime(df["transaction_date"], errors="coerce")
        month_start_dates = df["current_month"].apply(MLPreparationService._parse_current_month_start)

        df["payment_offset_days"] = (transaction_dates - month_start_dates).dt.days

        return df

    @staticmethod
    def _extract_rating_value(value):
        if pd.isna(value):
            return None

        text = str(value).strip()

        if not text:
            return None

        # Случай даты вида 01.01.2026 0:00 -> берём день месяца
        date_match = re.match(r"^\s*(\d{1,2})\.(\d{1,2})\.(\d{2,4})", text)
        if date_match:
            return int(date_match.group(1))

        # Случай повторённой оценки или просто числа в тексте -> берём первое число
        number_match = re.search(r"(\d{1,2})", text)
        if number_match:
            return int(number_match.group(1))

        return None

    @staticmethod
    def _parse_current_month_start(value):
        if pd.isna(value):
            return pd.NaT

        text = str(value).strip()

        if not text:
            return pd.NaT

        for fmt in ("%b.%y", "%b %y", "%m.%y", "%m/%y"):
            try:
                parsed = datetime.strptime(text, fmt)
                return pd.Timestamp(year=parsed.year, month=parsed.month, day=1)
            except ValueError:
                continue

        russian_months = {
            "янв": 1,
            "фев": 2,
            "мар": 3,
            "апр": 4,
            "май": 5,
            "июн": 6,
            "июл": 7,
            "авг": 8,
            "сен": 9,
            "сент": 9,
            "окт": 10,
            "ноя": 11,
            "дек": 12,
        }

        match = re.search(r"([А-Яа-яA-Za-z]+)[\.\s/-]*(\d{2,4})", text)
        if match:
            month_text = match.group(1).lower()
            year_text = match.group(2)

            for month_prefix, month_number in russian_months.items():
                if month_text.startswith(month_prefix):
                    year = int(year_text)
                    if year < 100:
                        year += 2000
                    return pd.Timestamp(year=year, month=month_number, day=1)

        return pd.NaT