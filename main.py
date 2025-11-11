from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import JSONResponse

app = FastAPI(title="MOS-GSM Duplicate Checker")

@app.get("/ping")
def ping():
    return {"status": "ok"}

@app.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    min_amount: float = Form(4500)
):
    """
    Пока упрощённый обработчик:
    - принимает Excel-файл
    - принимает порог min_amount
    - возвращает базовую инфу (заглушка)
    Дальше сюда добавим логику парсинга и записи в БД.
    """
    return {
        "message": "Файл получен",
        "filename": file.filename,
        "min_amount": min_amount
    }
