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
    def is_progress_df(df: pd.DataFrame):
        """
        Идентифицирует файл прогресса учеников по наличию колонок из PERIOD_DEPENDENT_METRICS
        """
        return any(metric in df.columns for metric in ProgressExtractor.PERIOD_DEPENDENT_METRICS)

    @staticmethod
    def process_progress_period(df: pd.DataFrame):
        """
        Создаёт копии столбцов с данными об успеваемости учеников с пометками о периоде
        """
        pbi_filters = ProgressExtractor.extract_pbi_filters(df)
        if not pbi_filters:
            return None

        date_from, date_to = ProgressExtractor.extract_dates(pbi_filters)
        if not date_from or not date_to:
            return None

        period_types = ProgressExtractor.detect_period_types(date_from, date_to)
        if not period_types:
            return None

        for feature_name, rule in PROGRESS_FEATURE_RULES.items():
            column_name = normalize_column_name(rule["column"])
            if column_name not in df.columns:
                continue

            for period_type in period_types:
                column_name_with_period = f"{feature_name}_{period_type}"
                df[column_name_with_period] = df[column_name].copy()

        return df

    @staticmethod
    def extract_pbi_filters(df: pd.DataFrame):
        """
        Извлекает фильтры PBI, найденные в файле
        """
        first_column = df.iloc[:, 0].dropna()

        if first_column.empty:
            return None

        last_value = str(first_column.iloc[-1]).strip()

        if "Примененные фильтры:" not in last_value:
            return None

        filters = [line.strip() for line in last_value.splitlines() if line.strip()]

        return filters if filters else None

    @staticmethod
    def extract_dates(filters: list[str]):
        """
        Извлекает даты, найденные в фильтрах PBI
        """
        date_pattern = r"(\d{2}\.\d{2}\.\d{4})"

        for line in filters:
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
        """
        Определяет за какой период данные на основе дат из применённых фильтров.
        Данные в PBI приходят утром следующего дня, поэтому для выгрузки за неделю выбираются два дня с одинаковым
        днём недели. В фильтре дата отображается в формате "до какой-то даты". Чтобы определить за какой период
        данные, считается разница дат и вычитается 1 для компенсации предлога "до"
        """
        days_in_period = (date_to - date_from).days - 1
        period_types = []

        if days_in_period == 7:
            period_types.append("week")

        if date_from.day == 1:
            period_types.append("month")

        return period_types


class ActivityExtractor:

    @staticmethod
    def is_activity_df(raw_df: pd.DataFrame):
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
    def process_activity_headers(raw_df: pd.DataFrame):
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
        df = raw_df.iloc[2:].copy()

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

        df.columns = columns
        return df.reset_index(drop=True)

    @staticmethod
    def add_active_days_pct_column(df: pd.DataFrame):
        """
        Создаёт производный столбец доли активных дней за период, попавший выгрузку.
        Добавляется раньше остальных производных столбцов, чтобы не нести дальше много столбцов привязанных к дате
        """
        total_index = df.columns.get_loc("Total")
        last_date_column_name = df.columns[total_index - 1]

        lines = [line.strip() for line in last_date_column_name.splitlines()]
        number = int(lines[0])

        df["active_days_pct"] = (df["Total"] / number).clip(upper=1)

        return df
