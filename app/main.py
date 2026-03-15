from fastapi import FastAPI

from app.api.upload import router as upload_router
from app.core.settings import settings

app = FastAPI(title=settings.app_name)

app.include_router(upload_router)

@app.get("/")
def root():
    return {"message": "Student Dropout Prediction System"}