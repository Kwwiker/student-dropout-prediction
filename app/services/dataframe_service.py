import pandas as pd
from fastapi import HTTPException

from app.schemas.file_info import FileStructureInfo
from app.services.extractors import ProgressExtractor, ActivityExtractor
from app.utils.dataframe import normalize_column_names, normalize_column_name
from app.core.feature_rules import FEATURE_RULES


class DataFrameService:
    USER_ID_PATTERN = "user_id"
    STANDARD_USER_ID_COLUMN = "user_id"

    @staticmethod
    def prepare_dataframe(df: pd.DataFrame):
        normalized_columns = normalize_column_names(df.columns.tolist())

        prepared_df = df.copy()
        prepared_df.columns = normalized_columns

        detected_user_id_column = DataFrameService._find_user_id_column(prepared_df.columns.tolist())
        prepared_df = DataFrameService._standardize_user_id_column(prepared_df, detected_user_id_column)

        return prepared_df, normalized_columns, detected_user_id_column

    @staticmethod
    def build_working_dataframe(df: pd.DataFrame):
        print("DATAFRAME_SERVICE: start")
        prepared_df, normalized_columns, detected_user_id_column = DataFrameService.prepare_dataframe(df)
        print("PREPARED DF COLUMNS:")
        print(prepared_df.columns.tolist())


        result_df = pd.DataFrame()
        result_df[DataFrameService.STANDARD_USER_ID_COLUMN] = prepared_df[DataFrameService.STANDARD_USER_ID_COLUMN]

        found_features = [DataFrameService.STANDARD_USER_ID_COLUMN]
        missing_features = []
        print("CALLING ProgressExtractor.add_progress_features")
        if ProgressExtractor.is_progress_special_format(normalized_columns):
            ProgressExtractor.add_progress_features(
                prepared_df=prepared_df,
                result_df=result_df,
                found_features=found_features,
                missing_features=missing_features,
            )

        ActivityExtractor.add_activity_features(prepared_df=prepared_df, result_df=result_df,
                                                found_features=found_features, missing_features=missing_features)

        for feature_name, rule in FEATURE_RULES.items():
            if feature_name == DataFrameService.STANDARD_USER_ID_COLUMN:
                continue

            matched_column = None

            if rule["source"] == "exact":
                target_column = normalize_column_name(rule["column"])

                if target_column in prepared_df.columns:
                    matched_column = target_column

            elif rule["source"] == "contains":
                pattern = normalize_column_name(rule["pattern"])

                for column in prepared_df.columns:
                    if pattern in column:
                        matched_column = column
                        break

            elif rule["source"] == "user_id":
                matched_column = DataFrameService.STANDARD_USER_ID_COLUMN

            if matched_column:
                result_df[feature_name] = prepared_df[matched_column]
                found_features.append(feature_name)
            else:
                missing_features.append(feature_name)

        return result_df, found_features, missing_features

    @staticmethod
    def extract_file_structure_info(df: pd.DataFrame, sheet_names: list[str] | None = None):
        original_columns = df.columns.tolist()

        prepared_df, normalized_columns, detected_user_id_column = DataFrameService.prepare_dataframe(df)

        progress_special_format = ProgressExtractor.is_progress_special_format(normalized_columns)

        pbi_filters_preview = None
        pbi_filters_found = False
        period_date_from = None
        period_date_to = None
        detected_period_types = None
        period_warning = None

        if progress_special_format:
            pbi_filters_preview = ProgressExtractor.extract_pbi_filters_preview(df)
            pbi_filters_found = pbi_filters_preview is not None

            if pbi_filters_found:
                date_from, date_to = ProgressExtractor.extract_period_dates_from_filters(pbi_filters_preview)

                if date_from and date_to:
                    period_date_from = date_from.strftime("%d.%m.%Y")
                    period_date_to = date_to.strftime("%d.%m.%Y")
                    detected_period_types = ProgressExtractor.detect_period_types(date_from, date_to)

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
            progress_special_format=progress_special_format,
            pbi_filters_found=pbi_filters_found,
            pbi_filters_preview=pbi_filters_preview,
            period_date_from=period_date_from,
            period_date_to=period_date_to,
            detected_period_types=detected_period_types,
            period_warning=period_warning,
            sheets=sheet_names,
        )

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
