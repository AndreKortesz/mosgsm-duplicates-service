import os
import io
import re
from datetime import datetime
from collections import defaultdict
import pandas as pd
from fastapi import FastAPI, UploadFile, File as FastAPIFile, Depends
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
    raw_text = Column(Text)
    order_number = Column(String, index=True)
    order_date = Column(DateTime, nullable=True)
    address = Column(Text)
    payout = Column(Float)
    worker_name = Column(String)
    work_type = Column(String)
    comment = Column(Text)
    parsed_ok = Column(Boolean, default=False)
    is_problematic = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    file = relationship("File", back_populates="orders")

# ========== Инициализация БД ==========
@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(bind=engine)

# ========== Зависимость для БД ==========
def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ========== Парсинг ==========
ORDER_NUMBER_REGEX = re.compile(r"\b[А-ЯA-Z]{2,5}-\d{5,7}\b")

def extract_order_number(text: str) -> str | None:
    if not text:
        return None
    m = ORDER_NUMBER_REGEX.search(text)
    return m.group(0) if m else None

def extract_address(text: str) -> str | None:
    """
    Извлекаем адрес после даты и первой запятой.
    Пример: "Заказ клиента КАУТ-001410 от 02.10.2025 17:13:20, 
             МО, Дмитровский муниципальный округ, дер.Рождественно, 153 СВН"
    Берём всё после ", " после даты.
    """
    if not text:
        return None
    # Ищем дату формата ДД.ММ.ГГГГ ЧЧ:ММ:СС или ДД.ММ.ГГГГ
    m = re.search(r"от\s+\d{2}\.\d{2}\.\d{4}[^,]*,\s*(.+)$", text)
    if m:
        return m.group(1).strip()
    return None

def is_template_row(row: dict) -> bool:
    """
    Фильтр шаблонных/служебных строк.
    """
    joined = " ".join([str(v) for v in row.values() if v is not None]).strip().lower()
    if not joined or len(joined) < 5:
        return True
    
    # Явные служебные строки
    template_markers = [
        "параметр периода",
        "процент оплаты",
        "процент выручки",
        "монтажник",  # это заголовок
    ]
    for marker in template_markers:
        if marker in joined:
            return True
    
    # Строки итогов
    if joined.startswith("итого") or "итого:" in joined:
        return True
    
    return False

# ========== Аналитика дублей ==========
def row_short(r: OrderRow) -> dict:
    return {
        "id": r.id,
        "file_id": r.file_id,
        "order_number": r.order_number,
        "address": r.address,
        "payout": r.payout,
        "worker_name": r.worker_name,
        "work_type": r.work_type,
    }

def analyze_duplicates_for_file(db: Session, file_id: int) -> dict:
    """
    Анализ дублей по всей базе.
    """
    all_orders: list[OrderRow] = (
        db.query(OrderRow)
        .filter(
            OrderRow.order_number.isnot(None),
            OrderRow.address.isnot(None),
        )
        .all()
    )

    clusters = defaultdict(list)
    for r in all_orders:
        key = (r.order_number.strip(), r.address.strip())
        clusters[key].append(r)

    hard_duplicates = []
    combo_clusters = []
    clusters_with_multiple = []

    for (order_number, address), rows in clusters.items():
        if len(rows) < 2:
            continue
        
        clusters_with_multiple.append((order_number, address, rows))
        
        by_type = defaultdict(list)
        has_diag_or_insp = False
        has_install = False
        
        for r in rows:
            by_type[r.work_type].append(r)
            if r.work_type in ("diagnostic", "inspection"):
                has_diag_or_insp = True
            if r.work_type == "installation":
                has_install = True

        # Жесткие дубли
        for wt, items in by_type.items():
            if len(items) >= 2:
                hard_duplicates.append({
                    "order_number": order_number,
                    "address": address,
                    "work_type": wt,
                    "rows": [row_short(r) for r in items],
                })

        # Комбо
        if has_diag_or_insp and has_install:
            combo_clusters.append({
                "order_number": order_number,
                "address": address,
                "rows": [row_short(r) for r in rows],
            })

    return {
        "clusters_with_multiple_count": len(clusters_with_multiple),
        "hard_duplicates_count": len(hard_duplicates),
        "combo_clusters_count": len(combo_clusters),
        "hard_duplicates_sample": hard_duplicates[:30],
        "combo_clusters_sample": combo_clusters[:30],
    }

# ========== Эндпоинты ==========
@app.get("/ping")
def ping():
    return {"status": "ok"}

@app.post("/upload")
async def upload_file(
    file: UploadFile = FastAPIFile(...),
    db: Session = Depends(get_db),
):
    """
    Загрузка и обработка Excel файла.
    """
    content = await file.read()
    
    try:
        # ИСПРАВЛЕНО: header=5 (строка 6 в Excel - там настоящие заголовки)
        df = pd.read_excel(io.BytesIO(content), header=5)
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

    # Поиск колонок (ищем точные совпадения или содержание ключевых слов)
    columns_lower = {str(c).strip().lower(): c for c in df.columns}
    
    # Колонка с заказом и комментарием (там номер заказа + адрес)
    order_col = None
    for key in ["заказ, комментарий", "заказ", "комментарий"]:
        if key in columns_lower:
            order_col = columns_lower[key]
            break
    
    # Колонка "Итого" (приоритет)
    payout_col = None
    if "итого" in columns_lower:
        payout_col = columns_lower["итого"]
    elif "сумма оплаты от услуг" in columns_lower:
        payout_col = columns_lower["сумма оплаты от услуг"]
    
    # Колонка с монтажником
    worker_col = None
    for key in ["монтажник", "фио"]:
        if key in columns_lower:
            worker_col = columns_lower[key]
            break
    
    # Колонка диагностики
    diagnostic_col = None
    if "диагностика" in columns_lower:
        diagnostic_col = columns_lower["диагностика"]
    
    # Колонка выручка (выезд) специалиста
    inspection_col = None
    for key in ["выручка (выезд) специалиста", "выезд специалиста"]:
        if key in columns_lower:
            inspection_col = columns_lower[key]
            break

    for _, row in df.iterrows():
        total_rows += 1
        row_dict = row.to_dict()

        # Фильтр шаблонных строк
        if is_template_row(row_dict):
            continue

        # Извлекаем текст из колонки "Заказ, Комментарий"
        text_cell = ""
        if order_col and pd.notna(row.get(order_col)):
            text_cell = str(row.get(order_col)).strip()
        
        if not text_cell:
            # Fallback на все ячейки
            text_cell = " ".join([str(v) for v in row_dict.values() if pd.notna(v)])

        # Извлекаем номер заказа и адрес
        order_number = extract_order_number(text_cell)
        address = extract_address(text_cell)

        # Выплата из колонки "Итого"
        payout_val = None
        if payout_col is not None and pd.notna(row.get(payout_col)):
            raw = row.get(payout_col)
            try:
                if isinstance(raw, str):
                    cleaned = raw.replace(" ", "").replace(",", ".")
                    payout_val = float(cleaned)
                else:
                    payout_val = float(raw)
            except Exception:
                payout_val = None

        # Суммы по диагностике и выезду
        diag_sum = 0.0
        if diagnostic_col and pd.notna(row.get(diagnostic_col)):
            try:
                val = str(row.get(diagnostic_col)).replace(" ", "").replace(",", ".")
                diag_sum = float(val)
            except Exception:
                diag_sum = 0.0

        insp_sum = 0.0
        if inspection_col and pd.notna(row.get(inspection_col)):
            try:
                val = str(row.get(inspection_col)).replace(" ", "").replace(",", ".")
                insp_sum = float(val)
            except Exception:
                insp_sum = 0.0

        # Определяем тип работы
        if diag_sum > 0:
            work_type = "diagnostic"
        elif insp_sum > 0:
            work_type = "inspection"
        elif payout_val is not None and payout_val > 5000:
            work_type = "installation"
        else:
            work_type = "other"

        # ФИО монтажника
        worker_name = None
        if worker_col and pd.notna(row.get(worker_col)):
            worker_name = str(row.get(worker_col)).strip()

        # Проблемная строка
        is_problematic = False
        parsed_ok = True
        if not order_number and not address:
            is_problematic = True
            parsed_ok = False

        order_row = OrderRow(
            file_id=db_file.id,
            raw_text=text_cell,
            order_number=order_number,
            address=address,
            payout=payout_val,
            worker_name=worker_name,
            work_type=work_type,
            comment="",
            parsed_ok=parsed_ok,
            is_problematic=is_problematic,
        )
        db.add(order_row)
        inserted_rows += 1
        
        if is_problematic:
            problematic_rows += 1

    db.commit()

    # Анализ дублей
    analysis = analyze_duplicates_for_file(db, db_file.id)

    return {
        "message": "Файл загружен и обработан",
        "file_id": db_file.id,
        "filename": db_file.filename,
        "total_rows_in_file": int(total_rows),
        "saved_rows": int(inserted_rows),
        "problematic_rows": int(problematic_rows),
        "clusters_with_multiple_count": analysis["clusters_with_multiple_count"],
        "hard_duplicates_count": analysis["hard_duplicates_count"],
        "combo_clusters_count": analysis["combo_clusters_count"],
        "hard_duplicates_sample": analysis["hard_duplicates_sample"],
        "combo_clusters_sample": analysis["combo_clusters_sample"],
    }
    
