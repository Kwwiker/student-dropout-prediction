import re
from datetime import datetime, date
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

        raw_df = PipelineService._read_excel_raw(file_path, first_sheet_name)

        if raw_df.empty:
            raise HTTPException(
                status_code=400,
                detail=f"Первый лист Excel-файла пустой: {first_sheet_name}"
            )

        if PipelineService._is_activity_special_format(raw_df):
            df = PipelineService._build_activity_dataframe(raw_df)
        else:
            try:
                df = pd.read_excel(file_path, sheet_name=first_sheet_name)
            except Exception:
                raise HTTPException(
                    status_code=400,
                    detail=f"Не удалось прочитать первый лист Excel-файла: {first_sheet_name}"
                )

        if df.empty:
            raise HTTPException(
                status_code=400,
                detail=f"Первый лист Excel-файла пустой: {first_sheet_name}"
            )

        return PipelineService._build_file_structure_info(df, sheet_names)

    @staticmethod
    def _read_excel_raw(file_path: str, sheet_name: str):
        try:
            return pd.read_excel(file_path, sheet_name=sheet_name, header=None)
        except Exception:
            raise HTTPException(
                status_code=400,
                detail=f"Не удалось прочитать лист Excel-файла: {sheet_name}"
            )

    @staticmethod
    def _build_file_structure_info(df: pd.DataFrame, sheet_names: list[str] | None = None):
        original_columns = df.columns.tolist()

        prepared_df, normalized_columns, detected_user_id_column = PipelineService._prepare_dataframe(df)

        requires_period_detection = PipelineService._requires_period_detection(normalized_columns)

        pbi_filters_preview = None
        pbi_filters_found = False
        period_date_from = None
        period_date_to = None
        detected_period_types = None
        period_warning = None

        if requires_period_detection:
            pbi_filters_preview = PipelineService._extract_pbi_filters_preview(df)
            pbi_filters_found = pbi_filters_preview is not None

            if pbi_filters_found:
                date_from, date_to = PipelineService._extract_period_dates(pbi_filters_preview)

                if date_from and date_to:
                    period_date_from = date_from.strftime("%d.%m.%Y")
                    period_date_to = date_to.strftime("%d.%m.%Y")
                    detected_period_types = PipelineService._detect_period_types(date_from, date_to)

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
            standardized_user_id_column=PipelineService.STANDARD_USER_ID_COLUMN,
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
    def _prepare_dataframe(df: pd.DataFrame):
        normalized_columns = normalize_column_names(df.columns.tolist())

        prepared_df = df.copy()
        prepared_df.columns = normalized_columns

        detected_user_id_column = PipelineService._find_user_id_column(prepared_df.columns.tolist())

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
    def _requires_period_detection(columns: list[str]):
        return any(metric in columns for metric in PipelineService.PERIOD_DEPENDENT_METRICS)

    @staticmethod
    def _extract_pbi_filters_preview(df: pd.DataFrame):
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
    def _extract_period_dates(filters_preview: list[str]):
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
    def _detect_period_types(date_from: date, date_to: date):
        days_in_period = (date_to - date_from).days + 1
        period_types = []

        if days_in_period == 8:
            period_types.append("week")

        if date_from.day == 1:
            period_types.append("month")

        return period_types

    @staticmethod
    def _is_activity_special_format(raw_df: pd.DataFrame):
        if raw_df.empty:
            return False

        top_left_value = raw_df.iloc[0, 0]

        if pd.isna(top_left_value):
            return False

        return str(top_left_value).strip() == "День"

    @staticmethod
    def _build_activity_dataframe(raw_df: pd.DataFrame):
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
            first_value = str(value_row_1).strip() if pd.notna(value_row_1) else ""
            second_value = str(value_row_2).strip() if pd.notna(value_row_2) else ""

            if second_value.lower() == "user_action_days_count_every_day":
                use_first_row_for_headers = True

            if use_first_row_for_headers:
                column_name = first_value if first_value else "unnamed_column"
            else:
                column_name = second_value if second_value else "unnamed_column"

            columns.append(column_name)

        data_df.columns = columns
        data_df = data_df.reset_index(drop=True)

        return data_df
