import pandas as pd
from fastapi import HTTPException

from app.core.settings import settings
from app.schemas.file_info import FileStructureInfo, FileInfo
from app.services.extractors import ProgressExtractor, ActivityExtractor
from app.services.readers import ExcelReader
from app.utils.dataframe import normalize_column_names, normalize_column_name
from app.core.feature_rules import FEATURE_RULES


class DataFrameService:
    @staticmethod
    def build_working_df(file: FileInfo):
        df = pd.DataFrame()
        if file.extension == ".xlsx":
            df = ExcelReader.read(file.path)
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Неподдерживаемое расширение файла: {file.extension}"
            )

        # Файл активности имеет особое расположение заголовков и требует создания производного столбца на этом этапе
        if ActivityExtractor.is_activity_df(df):
            df = ActivityExtractor.process_activity_headers(df)
            df = ActivityExtractor.add_active_days_pct_column(df)
        else:
            df.columns = df.iloc[0]
            df = df[1:]
            df.reset_index(drop=True)

        normalized_columns = normalize_column_names(df.columns.tolist())
        df.columns = normalized_columns

        key_column = DataFrameService.find_key_column(df)
        df = df.rename(columns={key_column: settings.key_column_name})

        # Файл прогресса требует учёта временного периода в некоторых заголовках
        if ProgressExtractor.is_progress_df(df):
            df = ProgressExtractor.process_progress_period(df)

        working_df = pd.DataFrame()
        working_df[settings.key_column_name] = df[settings.key_column_name]

        found_features = [settings.key_column_name]
        missing_features = []

        for feature_name, rule in FEATURE_RULES.items():
            matched_column = None

            if rule["source"] == "exact":
                target_column = normalize_column_name(rule["column"])
                if target_column in df.columns:
                    matched_column = target_column

            elif rule["source"] == "contains":
                pattern = normalize_column_name(rule["pattern"])
                for column in df.columns:
                    if pattern in column:
                        matched_column = column
                        break

            if matched_column:
                working_df[feature_name] = df[matched_column]
                found_features.append(feature_name)
            else:
                missing_features.append(feature_name)

        return working_df, found_features, missing_features

    @staticmethod
    def find_key_column(df: pd.DataFrame):
        for column in df.columns:
            if settings.key_column_name in column:
                return column

        raise HTTPException(status_code=400,
                            detail=f"В файле отсутствует колонка с {settings.key_column_name}"
                            )
