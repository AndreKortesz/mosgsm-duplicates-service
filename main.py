import os
import io
import re
from datetime import datetime

import pandas as pd
from fastapi import FastAPI, UploadFile, File as FastAPIFile, Form, Depends
from fastapi.responses import JSONResponse
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Float,
    Boolean,
    ForeignKey,
    DateTime,
    Text,
)
from sqlalchemy.orm import declarative_base, sessionmaker, relationship, Session

# ========== Настройки приложения ==========

app = FastAPI(title="MOS-GSM Duplicate Checker")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# ========== Модели БД ==========

class File(Base):
    __tablename__ = "files"

    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String, nullable=False)
    uploaded_at = Column(DateTime, default=datetime.utcnow)

    orders = relationship("OrderRow", back_populates="file")


class OrderRow(Base):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True, index=True)
    file_id = Column(Integer, ForeignKey("files.id"), nullable=False)

    raw_text = Column(Text)  # исходная ячейка заказа/адреса
    order_number = Column(String, index=True)
    order_date = Column(DateTime, nullable=True)
    address = Column(Text)

    payout = Column(Float)
    worker_name = Column(String)
    work_type = Column(String)  # diagnostic / inspection / installation / other
    comment = Column(Text)

    parsed_ok = Column(Boolean, default=False)
    is_problematic = Column(Boolean, default=False)

    created_at = Column(DateTime, default=datetime.utcnow)

    file = relationship("File", back_populates="orders")


# ========== Инициализация БД ==========

@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(bind=engine)


# ========== Зависимость для работы с БД ==========

def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ========== Вспомогательные функции парсинга ==========

ORDER_NUMBER_REGEX = re.compile(r"\b[А-ЯA-Z]{2,5}-\d{5,7}\b")


def extract_order_number(text: str) -> str | None:
    if not text:
        return None
    m = ORDER_NUMBER_REGEX.search(text)
    return m.group(0) if m else None


def extract_address(text: str) -> str | None:
    """
    Простой вариант:
    - Ищем 'от ДД.ММ.ГГГГ' и берём всё после первой запятой после даты.
    - Если не найдём — возвращаем None (уйдёт в проблемные).
    """
    if not text:
        return None

    m = re.search(r"от\s+\d{2}\.\d{2}\.\d{4}[^,]*,(.+)$", text)
    if m:
        return m.group(1).strip()
    return None


def detect_work_type(name: str | None, payout: float | None) -> str:
    """
    Логика из ТЗ:
    - 'Диагностика' => diagnostic
    - 'Выручка (выезд) специалиста' => inspection
    - payout > 5000 => installation (если нет явной диагностики/выезда)
    - иначе other
    """
    text = (name or "").lower()

    if "диагност" in text:
        return "diagnostic"
    if "выручка (выезд) специалиста" in text or "выезд специалиста" in text:
        return "inspection"

    if payout is not None and payout > 5000:
        return "installation"

    return "other"


def is_template_row(row: dict) -> bool:
    """
    Фильтр шаблонных строк:
    - Заголовки, пустые строки, строки без полезных данных.
    Здесь упрощенный вариант: если нет ни одной цифры и текста 'заказ', 'клиент', 'монтаж', 'диагност' и т.п.,
    можно считать такой ряд кандидатом на шаблонный.
    Этот фильтр потом можно донастроить под реальные файлы.
    """
    joined = " ".join([str(v) for v in row.values() if v is not None]).strip().lower()
    if not joined:
        return True

    # Явные признаки содержимого
    keywords = ["заказ", "клиент", "монтаж", "диагност", "выезд", "адрес", "сумма"]
    if any(k in joined for k in keywords):
        return False

    # Если вообще нет цифр и мало символов — вероятно заголовок/мусор
    if not any(ch.isdigit() for ch in joined) and len(joined) < 10:
        return True

    return False


# ========== Эндпоинты ==========

@app.get("/ping")
def ping():
    return {"status": "ok"}


@app.post("/upload")
async def upload_file(
    file: UploadFile = FastAPIFile(...),
    min_amount: float = Form(4500),
    db: Session = Depends(get_db),
):
    """
    1. Читаем Excel.
    2. Создаем запись о файле.
    3. Разбираем строки и кладем в таблицу orders.
    4. Возвращаем краткий отчет (позже добавим детальный анализ дублей).
    """
    content = await file.read()

    try:
        df = pd.read_excel(io.BytesIO(content))
    except Exception as e:
        return JSONResponse(
            status_code=400,
            content={"error": f"Не удалось прочитать Excel: {str(e)}"},
        )

    # Создаем запись о файле
    db_file = File(filename=file.filename)
    db.add(db_file)
    db.commit()
    db.refresh(db_file)

    total_rows = 0
    inserted_rows = 0
    problematic_rows = 0

    # Пытаемся угадать названия колонок
    # Эти имена подстрой под свой реальный файл при необходимости
    possible_order_cols = [c for c in df.columns if "заказ" in str(c).lower()]
    order_col = possible_order_cols[0] if possible_order_cols else None

    # Определяем колонку с выплатой
payout_col = None

# 1. Приоритет — колонка "Итого"
for c in df.columns:
    name = str(c).strip().lower()
    if "итого" in name:
        payout_col = c
        break

# 2. Если "Итого" не нашли — пробуем "Сумма оплаты от услуг"
if payout_col is None:
    for c in df.columns:
        name = str(c).strip().lower()
        if "сумма оплаты от услуг" in name:
            payout_col = c
            break

    worker_col = next(
        (c for c in df.columns if "фио" in str(c).lower() or "монтажник" in str(c).lower()),
        None,
    )
    name_col = next(
        (c for c in df.columns if "наименование" in str(c).lower() or "вид работ" in str(c).lower()),
        None,
    )
    comment_col = next(
        (c for c in df.columns if "коммент" in str(c).lower()),
        None,
    )

    for _, row in df.iterrows():
        total_rows += 1
        row_dict = row.to_dict()

        # Пропускаем шаблонные строки
        if is_template_row(row_dict):
            continue

        text_cell = ""
        if order_col and row.get(order_col) is not None:
            text_cell = str(row.get(order_col))
        else:
            # Если нет отдельной колонки "Заказ", попробуем слить все в одну строку
            text_cell = " ".join([str(v) for v in row_dict.values() if v is not None])

        order_number = extract_order_number(text_cell)
        address = extract_address(text_cell)

        # payout
        payout_val = None
        if payout_col and not pd.isna(row.get(payout_col)):
            try:
                payout_val = float(row.get(payout_col))
            except Exception:
                payout_val = None

        worker_name = str(row.get(worker_col)) if worker_col and not pd.isna(row.get(worker_col)) else None
        name_value = str(row.get(name_col)) if name_col and not pd.isna(row.get(name_col)) else ""
        comment_value = str(row.get(comment_col)) if comment_col and not pd.isna(row.get(comment_col)) else ""

        work_type = detect_work_type(name_value, payout_val)

        # Определяем проблемные строки:
        # нет order_number и нет address -> проблемная
        is_problematic = False
        parsed_ok = True

        if not order_number and not address:
            is_problematic = True
            parsed_ok = False

        # Если запись явно мусорная — можно пропускать, но ты просил видеть все непонятные, кроме шаблонных
        # поэтому сохраняем проблемные строки тоже.
        order_row = OrderRow(
            file_id=db_file.id,
            raw_text=text_cell,
            order_number=order_number,
            address=address,
            payout=payout_val,
            worker_name=worker_name,
            work_type=work_type,
            comment=comment_value,
            parsed_ok=parsed_ok,
            is_problematic=is_problematic,
        )
        db.add(order_row)
        inserted_rows += 1

        if is_problematic:
            problematic_rows += 1

    db.commit()

    return {
        "message": "Файл загружен и обработан",
        "file_id": db_file.id,
        "filename": db_file.filename,
        "total_rows_in_file": int(total_rows),
        "saved_rows": int(inserted_rows),
        "problematic_rows": int(problematic_rows),
        "hint": "Следующий шаг — добавить аналитику дублей и поиск по базе.",
    }
