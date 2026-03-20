FEATURE_RULES = {
    "transaction_date": {
        "source": "exact",
        "column": "Дата операции",
    },
    "current_month": {
        "source": "exact",
        "column": "Месяц обучения",
    },
    "course_name": {
        "source": "exact",
        "column": "Курс",
    },
    "subject_name": {
        "source": "exact",
        "column": "subject",
    },
    "exam_type": {
        "source": "exact",
        "column": "exam_type",
    },
    "teacher_name": {
        "source": "exact",
        "column": "teacher",
    },
    "presentation_rating": {
        "source": "contains",
        "pattern": "сложность подачи",
    },
    "load_difficulty_rating": {
        "source": "contains",
        "pattern": "насколько посильна",
    },
    "time_search_rating": {
        "source": "contains",
        "pattern": "легко найти время",
    },
    "subject_choice_assurance": {
        "source": "contains",
        "pattern": "будешь сдавать",
    },
    "school_grade": {
        "source": "contains",
        "pattern": "сентябр",
    },
    "target_score": {
        "source": "contains",
        "pattern": "цель в баллах",
    },
    "time_spend_amount": {
        "source": "contains",
        "pattern": "времени в неделю",
    },
    "education_style": {
        "source": "contains",
        "pattern": "проходить курс",
    },
    "completed_hw_pct_week": {
        "source": "exact",
        "column": "completed_hw_pct_week",
    },
    "earned_points_pct_week": {
        "source": "exact",
        "column": "earned_points_pct_week",
    },
    "completed_hw_pct_month": {
        "source": "exact",
        "column": "completed_hw_pct_month",
    },
    "earned_points_pct_month": {
        "source": "exact",
        "column": "earned_points_pct_month",
    },
    "education_track": {
        "source": "exact",
        "column": "Список целей",
    },
    "active_days_pct": {
        "source": "exact",
        "column": "active_days_pct",
    },
}

PROGRESS_FEATURE_RULES = {
    "completed_hw_pct": {
        "source": "exact",
        "column": "% сдачи ДЗ с решением (до выбранной даты)",
    },
    "earned_points_pct": {
        "source": "exact",
        "column": "средний балл по всем ДЗ с решением (до выбранной даты)",
    },
}
