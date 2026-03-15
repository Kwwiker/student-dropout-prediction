import pandas as pd
from fastapi import HTTPException


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
