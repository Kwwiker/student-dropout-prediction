import re


def normalize_column_name(column_name: str):
    normalized = column_name.strip().lower()
    normalized = normalized.replace("%", "процент")
    normalized = normalized.replace(" ", "_")
    normalized = normalized.replace("(", "")
    normalized = normalized.replace(")", "")
    normalized = re.sub(r"_+", "_", normalized)

    return normalized.strip("_")


def normalize_column_names(columns: list[str]):
    return [normalize_column_name(column) for column in columns]
