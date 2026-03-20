from fastapi import HTTPException
import pandas as pd
from starlette.datastructures import UploadFile

from app.schemas.file_info import FileInfo
from app.services.dataframe_service import DataFrameService
from app.services.file_service import FileService
from app.services.ml_preparation_service import MLPreparationService
from app.services.readers import CSVReader, ExcelReader
from uuid import uuid4
from app.utils.files import ensure_directory
from app.core.settings import settings


class PipelineService:

    @staticmethod
    def start_pipeline(upload_files: list[UploadFile]):
        saved_files = []
        for file in upload_files:
            saved_file = FileService.save_upload_file(file)  # Сохраняем загруженные файлы
            saved_files.append(saved_file)

        dataframes = []
        all_found_features = set()
        all_missing_features = set()
        for file in saved_files:
            df, found_features, missing_features = DataFrameService.build_working_df(file)
            dataframes.append(df)
            all_found_features.update(found_features)
            all_missing_features.update(missing_features)
        all_missing_features -= all_found_features

        merged_df = PipelineService.merge_dataframes(dataframes)
        cleared_df = MLPreparationService.prepare_for_model(merged_df)

        PipelineService.export_df(cleared_df)

    @staticmethod
    def merge_dataframes(dataframes: list[pd.DataFrame]):
        if not dataframes:
            raise HTTPException(
                status_code=400,
                detail="Не передано ни одного датафрейма для объединения"
            )

        merged_df = dataframes[0].copy()

        for next_df in dataframes[1:]:
            overlapping_columns = [
                column for column in next_df.columns
                if column != "user_id" and column in merged_df.columns
            ]

            if overlapping_columns:
                print("WARNING: дублирующие столбцы были пропущены:", overlapping_columns)
                next_df = next_df.drop(columns=overlapping_columns)

            merged_df = merged_df.merge(
                next_df,
                on="user_id",
                how="outer",
            )

        return merged_df

    @staticmethod
    def export_df(dataframe: pd.DataFrame):
        ensure_directory(settings.export_dir)
        filename = f"merged_{uuid4()}.csv"
        file_path = settings.export_dir / filename

        dataframe.to_csv(file_path, index=False, encoding="utf-8-sig", sep=";")

        print("MERGED CSV SAVED:", file_path)
