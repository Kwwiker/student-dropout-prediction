from fastapi import HTTPException
import pandas as pd
from app.services.dataframe_service import DataFrameService
from app.services.readers import CSVReader, ExcelReader


class PipelineService:

    @staticmethod
    def analyze_uploaded_file(file_path: str, extension: str):
        if extension == ".csv":
            df = CSVReader.read(file_path)
            return DataFrameService.extract_file_structure_info(df)

        elif extension == ".xlsx":
            df, sheet_names = ExcelReader.read(file_path)
            return DataFrameService.extract_file_structure_info(df, sheet_names)

        raise HTTPException(
            status_code=400,
            detail=f"Неподдерживаемое расширение файла: {extension}"
        )

    @staticmethod
    def build_working_dataframe(file_path: str, extension: str):
        if extension == ".csv":
            df = CSVReader.read(file_path)

        elif extension == ".xlsx":
            df, _ = ExcelReader.read(file_path)

        else:
            raise HTTPException(
                status_code=400,
                detail=f"Неподдерживаемое расширение файла: {extension}"
            )

        return DataFrameService.build_working_dataframe(df)

    @staticmethod
    def build_merged_working_dataframe(saved_files: list[dict]):
        partial_dataframes = []
        all_found_features = []
        all_missing_features = []

        for saved_file in saved_files:
            print(f"PIPELINE: processing file {saved_file['original_filename']}")

            working_df, found_features, missing_features = PipelineService.build_working_dataframe(
                file_path=saved_file["file_path"],
                extension=saved_file["extension"],
            )

            print(f"PIPELINE: built working dataframe for {saved_file['original_filename']}")
            print("WORKING DF COLUMNS:", working_df.columns.tolist())
            print("WORKING DF SHAPE:", working_df.shape)

            partial_dataframes.append(working_df)
            all_found_features.extend(found_features)
            all_missing_features.extend(missing_features)

        merged_df = PipelineService._merge_working_dataframes(partial_dataframes)

        unique_found_features = list(dict.fromkeys(all_found_features))
        unique_missing_features = list(dict.fromkeys(all_missing_features))

        return merged_df, unique_found_features, unique_missing_features

    @staticmethod
    def _merge_working_dataframes(dataframes: list[pd.DataFrame]):
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
                print("WARNING: duplicate columns skipped:", overlapping_columns)
                next_df = next_df.drop(columns=overlapping_columns)

            merged_df = merged_df.merge(
                next_df,
                on="user_id",
                how="outer",
            )

        return merged_df
