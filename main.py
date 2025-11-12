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
    Улучшенный парсинг адреса:
    - Ищем паттерн "от [дата] [время], [адрес]"
    - Если не нашли - пробуем просто после даты
    """
    if not text:
        return None
    
    # Паттерн с временем: "от 02.10.2025 15:13:20, адрес..."
    match = re.search(r"от\s+\d{2}\.\d{2}\.\d{4}\s+[\d:]+,\s*(.+)$", text)
    if match:
        return match.group(1).strip()
    
    # Альтернативный паттерн: просто после даты
    match = re.search(r"от\s+\d{2}\.\d{2}\.\d{4}[^,]*,\s*(.+)$", text)
    if match:
        return match.group(1).strip()
    
    return None

def is_template_row(row: dict) -> bool:
    """
    Фильтр шаблонных/служебных строк
    """
    joined = " ".join([str(v) for v in row.values() if v is not None]).strip().lower()
    if not joined:
        return True
    
    # Явные рабочие признаки
    keywords = ["заказ", "клиент", "монтаж", "диагност", "выезд", "адрес", "сумма"]
    if any(k in joined for k in keywords):
        return False
    
    # Строки "итого ..." - служебные
    if joined.startswith("итого"):
        return True
    
    # Если нет цифр и мало символов - мусор
    if not any(ch.isdigit() for ch in joined) and len(joined) < 10:
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
    Анализ по всей базе для поиска дублей между файлами
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
        
        # Жесткие дубли: 2+ записи с одним work_type
        for wt, items in by_type.items():
            if len(items) >= 2:
                hard_duplicates.append({
                    "order_number": order_number,
                    "address": address,
                    "work_type": wt,
                    "rows": [row_short(r) for r in items],
                })
        
        # Комбо: диагностика/осмотр + монтаж
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
    Загрузка и обработка Excel файла
    """
    content = await file.read()
    
    try:
        # Читаем Excel, заголовки в 7-й строке (индекс 6)
        df = pd.read_excel(io.BytesIO(content), header=6)
        
        # Очищаем названия колонок
        df.columns = [str(col).strip() if col is not None else "" for col in df.columns]
        
    except Exception as e:
        return JSONResponse(
            status_code=400,
            content={"error": f"Не удалось прочитать Excel: {str(e)}"},
        )
    
    # Создаём запись о файле
    db_file = File(filename=file.filename)
    db.add(db_file)
    db.commit()
    db.refresh(db_file)
    
    total_rows = 0
    inserted_rows = 0
    problematic_rows = 0
    
    # Находим колонку заказа
    order_col = None
    for c in df.columns:
        if "заказ" in str(c).lower() and "комментар" in str(c).lower():
            order_col = c
            break
    if order_col is None:
        possible_order_cols = [c for c in df.columns if "заказ" in str(c).lower()]
        order_col = possible_order_cols[0] if possible_order_cols else None
    
    # Находим колонку "Итого" (ТОЛЬКО из этой колонки берем основную сумму)
    payout_col = None
    for c in df.columns:
        name = str(c).strip()
        if name == "Итого" or "итого" in name.lower():
            payout_col = c
            break
    
    # Находим колонку монтажника
    worker_col = None
    for c in df.columns:
        name = str(c).lower()
        if "монтажник" in name or "фио" in name or "исполнитель" in name:
            worker_col = c
            break
    if worker_col is None and len(df.columns) > 0:
        worker_col = df.columns[0]
    
    # Колонки для типа работы
    
    # Диагностика: ищем "Диагностика" или "Оплата диагностики"
    diagnostic_col = None
    for c in df.columns:
        name = str(c).lower()
        if "диагност" in name:
            diagnostic_col = c
            break
    
    # Осмотр: ищем "Выручка (выезд) специалиста"
    inspection_col = None
    for c in df.columns:
        name = str(c).lower()
        if ("выручка" in name and "выезд" in name and "специалист" in name) or \
           (name == "выручка (выезд) специалиста"):
            inspection_col = c
            break
    
    # Колонка комментариев
    comment_col = None
    for c in df.columns:
        if "коммент" in str(c).lower():
            comment_col = c
            break
    
    # Обрабатываем строки
    for idx, row in df.iterrows():
        total_rows += 1
        row_dict = row.to_dict()
        
        # Пропускаем служебные строки
        if is_template_row(row_dict):
            continue
        
        # Извлекаем текст из колонки заказа
        text_cell = ""
        if order_col and pd.notna(row.get(order_col)):
            text_cell = str(row.get(order_col)).strip()
        
        if not text_cell:
            text_cell = " ".join([str(v) for v in row_dict.values() if pd.notna(v)])
        
        # Парсим номер заказа и адрес
        order_number = extract_order_number(text_cell)
        address = extract_address(text_cell)
        
        # Извлекаем сумму из колонки "Итого"
        payout_val = None
        if payout_col is not None:
            raw = row.get(payout_col)
            if pd.notna(raw):
                try:
                    if isinstance(raw, str):
                        cleaned = raw.replace(" ", "").replace(",", ".")
                        payout_val = float(cleaned)
                    else:
                        payout_val = float(raw)
                except Exception:
                    payout_val = None
        
        # Суммы для определения типа работы
        diag_sum = 0.0
        # Ищем колонку "Диагностика" или "Оплата диагностики"
        if diagnostic_col and pd.notna(row.get(diagnostic_col)):
            try:
                val = str(row.get(diagnostic_col)).replace(" ", "").replace(",", ".")
                diag_sum = float(val)
            except Exception:
                diag_sum = 0.0
        
        insp_sum = 0.0
        # Ищем колонку "Выручка (выезд) специалиста"
        if inspection_col and pd.notna(row.get(inspection_col)):
            try:
                val = str(row.get(inspection_col)).replace(" ", "").replace(",", ".")
                insp_sum = float(val)
            except Exception:
                insp_sum = 0.0
        
        # Определяем тип работы (ВАЖНО: порядок проверки имеет значение)
        work_type = "other"  # по умолчанию
        
        # 1. Если есть диагностика > 0 → diagnostic
        if diag_sum > 0:
            work_type = "diagnostic"
        # 2. Если есть выезд специалиста > 0 → inspection
        elif insp_sum > 0:
            work_type = "inspection"
        # 3. Если "Итого" > 5000 → installation
        elif payout_val is not None and payout_val > 5000:
            work_type = "installation"
        # 4. Иначе → other
        
        # Извлекаем имя монтажника
        worker_name = None
        if worker_col and pd.notna(row.get(worker_col)):
            worker_name = str(row.get(worker_col)).strip()
            # Фильтруем заголовки
            if worker_name.lower() in ["монтажник", "исполнитель", "фио", ""]:
                worker_name = None
        
        # Комментарий
        comment_value = ""
        if comment_col and pd.notna(row.get(comment_col)):
            comment_value = str(row.get(comment_col)).strip()
        
        # Проверка на проблемную строку
        is_problematic = False
        parsed_ok = True
        if not order_number and not address:
            is_problematic = True
            parsed_ok = False
        
        # Сохраняем в БД
        order_row = OrderRow(
            file_id=db_file.id,
            raw_text=text_cell[:1000] if text_cell else "",
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

@app.on_event("startup")
def on_startup():
    # Проверяем переменную окружения для сброса БД
    if os.getenv("RESET_DB") == "true":
        print("⚠️  RESET_DB=true - Удаление всех таблиц...")
        Base.metadata.drop_all(bind=engine)
        print("✅ Таблицы удалены")
    
    # Создаём таблицы заново
    Base.metadata.create_all(bind=engine)
    print("✅ Таблицы созданы")
