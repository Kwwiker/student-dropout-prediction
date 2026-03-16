FEATURE_RULES = {
    "user_id": {
        "source": "user_id",
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
    "education_track": {
        "source": "exact",
        "column": "Цели в курсе",
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
