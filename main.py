import os
import io
import re
from datetime import datetime
from collections import defaultdict
from typing import Optional
import pandas as pd
from fastapi import FastAPI, UploadFile, File as FastAPIFile, Depends, Request, Query
from fastapi.responses import JSONResponse, HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
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
    desc,
)
from sqlalchemy.orm import declarative_base, sessionmaker, relationship, Session

# ========== Настройки приложения ==========
app = FastAPI(title="Mos-GSM Duplicate Checker")

# Создаем директории
os.makedirs("templates", exist_ok=True)
os.makedirs("static", exist_ok=True)

# Подключаем статические файлы
app.mount("/static", StaticFiles(directory="static"), name="static")

# Инициализация шаблонов
templates = Jinja2Templates(directory="templates")

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
    parse_errors = Column(Text)  # <-- НОВОЕ ПОЛЕ для ошибок парсинга
    created_at = Column(DateTime, default=datetime.utcnow)
    file = relationship("File", back_populates="orders")

class FileParseLog(Base):
    __tablename__ = "file_parse_logs"
    id = Column(Integer, primary_key=True, index=True)
    file_id = Column(Integer, ForeignKey("files.id"), nullable=False)
    log_type = Column(String)  # warning, error, info
    message = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)

# ========== Инициализация БД ==========
@app.on_event("startup")
def on_startup():
    if os.getenv("RESET_DB") == "true":
        print("⚠️  RESET_DB=true - Удаление всех таблиц...")
        Base.metadata.drop_all(bind=engine)
        print("✅ Таблицы удалены")
    
    Base.metadata.create_all(bind=engine)
    print("✅ Таблицы созданы")

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
    Улучшенный парсинг адреса из разных форматов:
    1. "от ДД.ММ.ГГГГ ЧЧ:ММ:СС, адрес..."
    2. "Заказ клиента КАУТ-ХХХХХХ от ДД.ММ.ГГГГ ЧЧ:ММ:СС, адрес..."
    3. Просто адрес после запятой
    """
    if not text:
        return None
    
    # Паттерн 1: "от [дата] [время], [адрес]"
    match = re.search(r"от\s+\d{2}\.\d{2}\.\d{4}\s+[\d:]+,\s*(.+)$", text)
    if match:
        address = match.group(1).strip()
        if len(address) > 5:
            return address
    
    # Паттерн 2: "от [дата] без времени, [адрес]"
    match = re.search(r"от\s+\d{2}\.\d{2}\.\d{4}[^,]*,\s*(.+)$", text)
    if match:
        address = match.group(1).strip()
        if len(address) > 5:
            return address
    
    # Паттерн 3: Если есть номер заказа, берём всё после запятой
    if ORDER_NUMBER_REGEX.search(text):
        parts = text.split(',')
        if len(parts) >= 2:
            address = ','.join(parts[1:]).strip()
            if len(address) > 5 and not address.replace(' ', '').replace(':', '').replace('.', '').isdigit():
                return address
    
    return None

def is_template_row(row: dict) -> bool:
    """Фильтр шаблонных строк"""
    joined = " ".join([str(v) for v in row.values() if v is not None]).strip().lower()
    if not joined:
        return True
    
    keywords = ["заказ", "клиент", "монтаж", "диагност", "выезд", "адрес", "сумма"]
    if any(k in joined for k in keywords):
        return False
    
    if joined.startswith("итого"):
        return True
    
    if not any(ch.isdigit() for ch in joined) and len(joined) < 10:
        return True
    
    return False

def is_worker_header(text: str) -> bool:
    """
    Проверяет, является ли строка заголовком монтажника (ФИО).
    Примеры: "Ветренко Дмитрий", "Викулин Андрей", "Гуляев Олег"
    """
    if not text:
        return False
    
    text = text.strip()
    
    # Если в строке есть номер заказа - это не заголовок
    if ORDER_NUMBER_REGEX.search(text):
        return False
    
    # Если есть дата - это не заголовок
    if re.search(r'\d{2}\.\d{2}\.\d{4}', text):
        return False
    
    # Убираем пояснения в скобках типа "(оплата клиентом)"
    text_clean = re.sub(r'\([^)]*\)', '', text).strip()
    
    # Разбиваем на слова
    words = text_clean.split()
    
    # Если 2-3 слова, все начинаются с заглавной буквы, и нет цифр
    if 2 <= len(words) <= 3:
        all_capitalized = all(word[0].isupper() for word in words if word)
        has_no_digits = not any(char.isdigit() for char in text_clean)
        has_no_special = not any(char in text_clean for char in ['№', '/', '\\'])
        
        if all_capitalized and has_no_digits and has_no_special:
            return True
    
    return False

def normalize_text(text: str) -> str:
    """Нормализация текста для сравнения"""
    if not text:
        return ""
    return " ".join(text.lower().strip().split())

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
        "raw_text": r.raw_text[:100] if r.raw_text else "",
    }

def analyze_duplicates_for_file(db: Session, file_id: int = None) -> dict:
    """
    Анализ дублей и комбо по ВСЕЙ базе или по конкретному файлу.
    
    НОВАЯ ЛОГИКА:
    1. Схожие адреса (даже без номера заказа) = КОМБО
    2. Одинаковые заказы с разными адресами = ТРЕБУЕТ ПРОВЕРКИ
    3. Одинаковый заказ + адрес + тип работы (2+) = ЖЁСТКИЙ ДУБЛЬ
    """
    
    # Получаем все заказы (или только из конкретного файла)
    query = db.query(OrderRow)
    if file_id:
        query = query.filter(OrderRow.file_id == file_id)
    
    all_orders: list[OrderRow] = query.all()
    
    # === 1. ЖЁСТКИЕ ДУБЛИ: одинаковый заказ + адрес + тип работы ===
    hard_duplicates = []
    clusters_by_order_address = defaultdict(list)
    
    for r in all_orders:
        if r.order_number and r.address:
            key = (
                r.order_number.strip().upper(),
                normalize_text(r.address)
            )
            clusters_by_order_address[key].append(r)
    
    for (order_number, normalized_address), rows in clusters_by_order_address.items():
        if len(rows) < 2:
            continue
        
        by_type = defaultdict(list)
        for r in rows:
            by_type[r.work_type].append(r)
        
        # Если 2+ записи с одним типом работы - жёсткий дубль
        for wt, items in by_type.items():
            if len(items) >= 2:
                hard_duplicates.append({
                    "order_number": order_number,
                    "address": rows[0].address,
                    "work_type": wt,
                    "rows": [row_short(r) for r in items],
                })
    
    # === 2. КОМБО: схожие адреса (разные заказы или без заказов) ===
    combo_clusters = []
    clusters_by_address = defaultdict(list)
    
    # Группируем ВСЕ строки по нормализованному адресу
    for r in all_orders:
        if r.address:
            norm_addr = normalize_text(r.address)
            if len(norm_addr) > 5:  # Игнорируем слишком короткие адреса
                clusters_by_address[norm_addr].append(r)
    
    for norm_addr, rows in clusters_by_address.items():
        if len(rows) < 2:
            continue
        
        # Проверяем типы работ в кластере
        work_types = set(r.work_type for r in rows)
        order_numbers = set(r.order_number for r in rows if r.order_number)
        
        # Если это не жёсткий дубль (разные заказы ИЛИ разные типы работ)
        # ИЛИ вообще нет номера в одной из строк
        is_hard_duplicate = (
            len(order_numbers) == 1 and 
            len(work_types) == 1 and 
            len(rows) >= 2
        )
        
        if not is_hard_duplicate:
            combo_clusters.append({
                "address": rows[0].address,
                "order_numbers": list(order_numbers),
                "work_types": list(work_types),
                "rows": [row_short(r) for r in rows],
            })
    
    # === 3. ТРЕБУЕТ ПРОВЕРКИ: одинаковые номера заказов с РАЗНЫМИ адресами ===
    needs_review = []
    clusters_by_order = defaultdict(list)
    
    for r in all_orders:
        if r.order_number:
            clusters_by_order[r.order_number.strip().upper()].append(r)
    
    for order_num, rows in clusters_by_order.items():
        if len(rows) < 2:
            continue
        
        # Получаем уникальные адреса
        addresses = set(normalize_text(r.address) for r in rows if r.address)
        
        # Если адресов больше одного - требует проверки
        if len(addresses) > 1:
            needs_review.append({
                "order_number": order_num,
                "addresses": [r.address for r in rows if r.address],
                "rows": [row_short(r) for r in rows],
            })
    
    # === 4. ПРОБЛЕМНЫЕ СТРОКИ ===
    problematic_query = db.query(OrderRow).filter(OrderRow.is_problematic == True)
    if file_id:
        problematic_query = problematic_query.filter(OrderRow.file_id == file_id)
    
    problematic_orders = problematic_query.all()
    
    return {
        "hard_duplicates_count": len(hard_duplicates),
        "combo_clusters_count": len(combo_clusters),
        "needs_review_count": len(needs_review),
        "problematic_count": len(problematic_orders),
        "hard_duplicates_sample": hard_duplicates[:50],
        "combo_clusters_sample": combo_clusters[:50],
        "needs_review_sample": needs_review[:50],
        "problematic_sample": [row_short(r) for r in problematic_orders[:50]],
    }

# ========== API Эндпоинты ==========

@app.get("/api/files")
async def api_get_files(db: Session = Depends(get_db)):
    """Список всех файлов с расширенной статистикой"""
    files = db.query(File).order_by(desc(File.uploaded_at)).all()
    
    result = []
    for f in files:
        total_rows = db.query(OrderRow).filter(OrderRow.file_id == f.id).count()
        problematic = db.query(OrderRow).filter(
            OrderRow.file_id == f.id, 
            OrderRow.is_problematic == True
        ).count()
        
        # Получаем статистику дублей для этого файла
        analysis = analyze_duplicates_for_file(db, f.id)
        
        # Проверяем есть ли ошибки парсинга
        has_parse_errors = db.query(FileParseLog).filter(
            FileParseLog.file_id == f.id,
            FileParseLog.log_type == "error"
        ).count() > 0
        
        result.append({
            "id": f.id,
            "filename": f.filename,
            "uploaded_at": f.uploaded_at.isoformat(),
            "total_rows": total_rows,
            "problematic_rows": problematic,
            "hard_duplicates": analysis["hard_duplicates_count"],
            "combo_clusters": analysis["combo_clusters_count"],
            "needs_review": analysis["needs_review_count"],
            "has_parse_errors": has_parse_errors,
        })
    
    return {"files": result}

@app.get("/api/files/{file_id}")
async def api_get_file(file_id: int, db: Session = Depends(get_db)):
    """Детали файла"""
    file = db.query(File).filter(File.id == file_id).first()
    if not file:
        return JSONResponse(status_code=404, content={"error": "File not found"})
    
    analysis = analyze_duplicates_for_file(db, file_id)
    total_rows = db.query(OrderRow).filter(OrderRow.file_id == file_id).count()
    
    return {
        "id": file.id,
        "filename": file.filename,
        "uploaded_at": file.uploaded_at.isoformat(),
        "total_rows": total_rows,
        "analysis": analysis,
    }

@app.get("/api/search")
async def api_search(
    query: str = Query(..., min_length=1),
    db: Session = Depends(get_db)
):
    """
    Глобальный поиск по всем файлам.
    Ищет по: номеру заказа, адресу, сумме, монтажнику, исходному тексту.
    """
    if not query or len(query.strip()) < 1:
        return {"results": [], "total": 0}
    
    q = query.strip()
    
    # Ищем по всем полям
    search_query = db.query(OrderRow).filter(
        (OrderRow.order_number.ilike(f"%{q}%")) |
        (OrderRow.address.ilike(f"%{q}%")) |
        (OrderRow.worker_name.ilike(f"%{q}%")) |
        (OrderRow.raw_text.ilike(f"%{q}%")) |
        (OrderRow.comment.ilike(f"%{q}%"))
    )
    
    # Если это число, ищем по сумме
    try:
        amount = float(q.replace(",", ".").replace(" ", ""))
        search_query = search_query.union(
            db.query(OrderRow).filter(OrderRow.payout == amount)
        )
    except ValueError:
        pass
    
    results = search_query.limit(100).all()
    
    # Группируем результаты по файлам
    by_file = defaultdict(list)
    for r in results:
        by_file[r.file_id].append(row_short(r))
    
    # Получаем информацию о файлах
    files_info = {}
    for file_id in by_file.keys():
        f = db.query(File).filter(File.id == file_id).first()
        if f:
            files_info[file_id] = {
                "id": f.id,
                "filename": f.filename,
                "uploaded_at": f.uploaded_at.strftime("%d.%m.%Y %H:%M"),
            }
    
    return {
        "query": q,
        "total": len(results),
        "by_file": [
            {
                "file": files_info.get(fid, {}),
                "results": rows
            }
            for fid, rows in by_file.items()
        ]
    }

@app.get("/api/dashboard/all")
async def api_dashboard_all(db: Session = Depends(get_db)):
    """
    Общий дашборд: статистика по ВСЕМ файлам вместе
    """
    total_files = db.query(File).count()
    total_orders = db.query(OrderRow).count()
    
    # Анализ по всем файлам
    analysis = analyze_duplicates_for_file(db, file_id=None)
    
    # Статистика по типам работ
    work_types_count = {}
    for wt in ["diagnostic", "inspection", "installation", "other"]:
        count = db.query(OrderRow).filter(OrderRow.work_type == wt).count()
        work_types_count[wt] = count
    
    # Топ монтажников по количеству заказов
    from sqlalchemy import func
    top_workers = (
        db.query(OrderRow.worker_name, func.count(OrderRow.id).label('count'))
        .filter(OrderRow.worker_name.isnot(None))
        .group_by(OrderRow.worker_name)
        .order_by(func.count(OrderRow.id).desc())
        .limit(10)
        .all()
    )
    
    return {
        "total_files": total_files,
        "total_orders": total_orders,
        "hard_duplicates_count": analysis["hard_duplicates_count"],
        "combo_clusters_count": analysis["combo_clusters_count"],
        "needs_review_count": analysis["needs_review_count"],
        "problematic_count": analysis["problematic_count"],
        "work_types": work_types_count,
        "top_workers": [{"name": w[0], "count": w[1]} for w in top_workers],
        "analysis": analysis,
    }

@app.get("/api/files/{file_id}/parse-logs")
async def api_get_parse_logs(file_id: int, db: Session = Depends(get_db)):
    """Получить логи парсинга файла"""
    logs = (
        db.query(FileParseLog)
        .filter(FileParseLog.file_id == file_id)
        .order_by(FileParseLog.created_at.desc())
        .all()
    )
    
    return {
        "file_id": file_id,
        "logs": [
            {
                "id": log.id,
                "type": log.log_type,
                "message": log.message,
                "created_at": log.created_at.strftime("%d.%m.%Y %H:%M:%S"),
            }
            for log in logs
        ]
    }

@app.get("/api/files/{file_id}/rows")
async def api_get_rows(
    file_id: int,
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=500),
    order_number: Optional[str] = None,
    address: Optional[str] = None,
    worker_name: Optional[str] = None,
    work_type: Optional[str] = None,
    problematic_only: bool = False,
    db: Session = Depends(get_db)
):
    """Строки файла с фильтрацией и пагинацией"""
    query = db.query(OrderRow).filter(OrderRow.file_id == file_id)
    
    if order_number:
        query = query.filter(OrderRow.order_number.ilike(f"%{order_number}%"))
    if address:
        query = query.filter(OrderRow.address.ilike(f"%{address}%"))
    if worker_name:
        query = query.filter(OrderRow.worker_name.ilike(f"%{worker_name}%"))
    if work_type:
        query = query.filter(OrderRow.work_type == work_type)
    if problematic_only:
        query = query.filter(OrderRow.is_problematic == True)
    
    total = query.count()
    offset = (page - 1) * limit
    rows = query.offset(offset).limit(limit).all()
    
    return {
        "total": total,
        "page": page,
        "limit": limit,
        "rows": [row_short(r) for r in rows],
    }

@app.delete("/api/files/{file_id}")
async def api_delete_file(file_id: int, db: Session = Depends(get_db)):
    """Удалить файл и его записи"""
    db.query(OrderRow).filter(OrderRow.file_id == file_id).delete()
    db.query(File).filter(File.id == file_id).delete()
    db.commit()
    return {"message": "File deleted successfully"}

@app.post("/api/files/{file_id}/recalc")
async def api_recalc_file(file_id: int, db: Session = Depends(get_db)):
    """Пересчитать анализ файла"""
    analysis = analyze_duplicates_for_file(db, file_id)
    return {"message": "Analysis recalculated", "analysis": analysis}

@app.get("/api/files/{file_id}/export/{what}")
async def api_export(
    file_id: int,
    what: str,
    db: Session = Depends(get_db)
):
    """Экспорт в CSV"""
    file = db.query(File).filter(File.id == file_id).first()
    if not file:
        return JSONResponse(status_code=404, content={"error": "File not found"})
    
    if what == "rows":
        rows = db.query(OrderRow).filter(OrderRow.file_id == file_id).all()
        data = [row_short(r) for r in rows]
    elif what == "problematic":
        rows = db.query(OrderRow).filter(
            OrderRow.file_id == file_id,
            OrderRow.is_problematic == True
        ).all()
        data = [row_short(r) for r in rows]
    elif what in ["hard", "combo", "clusters", "review"]:
        analysis = analyze_duplicates_for_file(db, file_id)
        if what == "hard":
            data = analysis["hard_duplicates_sample"]
        elif what == "combo":
            data = analysis["combo_clusters_sample"]
        elif what == "review":
            data = analysis["needs_review_sample"]
        else:
            data = []
    else:
        return JSONResponse(status_code=400, content={"error": "Invalid export type"})
    
    df = pd.DataFrame(data)
    output = io.StringIO()
    df.to_csv(output, index=False, encoding='utf-8-sig')
    output.seek(0)
    
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={file.filename}_{what}.csv"}
    )

@app.get("/debug/row/{row_id}")
async def debug_row(row_id: int, db: Session = Depends(get_db)):
    """Посмотреть детали одной строки"""
    row = db.query(OrderRow).filter(OrderRow.id == row_id).first()
    if not row:
        return {"error": "Row not found"}
    
    return {
        "id": row.id,
        "file_id": row.file_id,
        "raw_text": row.raw_text,
        "order_number": row.order_number,
        "address": row.address,
        "payout": row.payout,
        "worker_name": row.worker_name,
        "work_type": row.work_type,
        "comment": row.comment,
        "parsed_ok": row.parsed_ok,
        "is_problematic": row.is_problematic,
    }

@app.post("/upload")
async def upload_file(
    file: UploadFile = FastAPIFile(...),
    db: Session = Depends(get_db),
):
    """Загрузка и обработка Excel файла"""
    content = await file.read()
    
    try:
        # Читаем Excel, пропуская первые 5 строк (параметры)
        df = pd.read_excel(io.BytesIO(content), header=5)
        
        # Очищаем названия колонок
        df.columns = [str(col).strip() if col is not None else "" for col in df.columns]
        
        print(f"🔍 DEBUG: Всего колонок: {len(df.columns)}")
        print(f"🔍 DEBUG: Первые 5 колонок: {list(df.columns[:5])}")
        print(f"🔍 DEBUG: Последние 5 колонок: {list(df.columns[-5:])}")
        
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
    
    # ========== ПОИСК КОЛОНОК ==========
    
    print(f"\n{'='*60}")
    print(f"🔍 АНАЛИЗ СТРУКТУРЫ ФАЙЛА")
    print(f"{'='*60}")
    print(f"Всего колонок: {len(df.columns)}")
    
    # Выводим все колонки для отладки
    for idx, col in enumerate(df.columns):
        print(f"  [{idx:2d}] {col}")
    print(f"{'='*60}\n")
    
    # 1. Колонка заказа (первая колонка)
    order_col = df.columns[0] if len(df.columns) > 0 else None
    print(f"✓ Колонка заказа: [{0}] {order_col}")
    
    # 2. Колонка монтажника (та же, что и заказ)
    worker_col = df.columns[0] if len(df.columns) > 0 else None
    print(f"✓ Колонка монтажника: [{0}] {worker_col}")
    
    # 3. КОЛОНКА "ИТОГО" - основная сумма для анализа
    payout_col = None
    payout_col_idx = None
    
    # Сначала ищем по названию (исключая "Выручка итого")
    for idx, c in enumerate(df.columns):
        name = str(c).strip().lower()
        if "итого" in name and "выручка" not in name:
            payout_col = c
            payout_col_idx = idx
            print(f"✓ Колонка 'Итого' найдена по имени: [{idx}] {c}")
            break
    
    # Если не нашли по имени, ищем по индексам (пробуем 16-20)
    if payout_col is None:
        for idx in [18, 17, 19, 16, 20, 15]:
            if idx < len(df.columns):
                col_name = str(df.columns[idx]).strip()
                print(f"  Проверяем [{idx}]: {col_name}")
                if "итого" in col_name.lower() and "выручка" not in col_name.lower():
                    payout_col = df.columns[idx]
                    payout_col_idx = idx
                    print(f"✓ Колонка 'Итого' найдена по индексу: [{idx}] {df.columns[idx]}")
                    break
    
    if payout_col is None:
        print("⚠️ ВНИМАНИЕ: Колонка 'Итого' не найдена!")

    if payout_col is None:
        msg = "⚠️ ВНИМАНИЕ: Колонка 'Итого' не найдена!"
        print(msg)
        log_entry = FileParseLog(file_id=db_file.id, log_type="warning", message=msg)
        db.add(log_entry)
    
    if diagnostic_col is None:
        msg = "⚠️ ВНИМАНИЕ: Колонка 'Диагностика' не найдена!"
        print(msg)
        log_entry = FileParseLog(file_id=db_file.id, log_type="warning", message=msg)
        db.add(log_entry)
    
    if inspection_col is None:
        msg = "⚠️ ВНИМАНИЕ: Колонка 'Выручка (выезд) специалиста' не найдена!"
        print(msg)
        log_entry = FileParseLog(file_id=db_file.id, log_type="warning", message=msg)
        db.add(log_entry)
    
    # 4. КОЛОНКА "ДИАГНОСТИКА" или "ОПЛАТА ДИАГНОСТИКИ"
    diagnostic_col = None
    diagnostic_col_idx = None
    
    # Ищем по названию
    for idx, c in enumerate(df.columns):
        name = str(c).lower()
        if "диагност" in name:
            diagnostic_col = c
            diagnostic_col_idx = idx
            print(f"✓ Колонка диагностики: [{idx}] {c}")
            break
    
    # Если не нашли, пробуем по индексам (обычно 4 или 5)
    if diagnostic_col is None:
        for idx in [4, 5, 3, 6]:
            if idx < len(df.columns):
                col_name = str(df.columns[idx]).lower()
                if "диагност" in col_name:
                    diagnostic_col = df.columns[idx]
                    diagnostic_col_idx = idx
                    print(f"✓ Колонка диагностики найдена по индексу: [{idx}] {df.columns[idx]}")
                    break
    
    if diagnostic_col is None:
        print("⚠️ ВНИМАНИЕ: Колонка 'Диагностика' не найдена!")
    
    # 5. КОЛОНКА "ВЫРУЧКА (ВЫЕЗД) СПЕЦИАЛИСТА"
    inspection_col = None
    inspection_col_idx = None
    
    # Ищем по названию
    for idx, c in enumerate(df.columns):
        name = str(c).lower()
        # Ищем точное совпадение с "выезд" + "специалист"
        if "выезд" in name and "специалист" in name:
            inspection_col = c
            inspection_col_idx = idx
            print(f"✓ Колонка осмотра (выезд специалиста): [{idx}] {c}")
            break
    
    # Если не нашли, пробуем по индексам (обычно 6 или 7)
    if inspection_col is None:
        for idx in [6, 7, 5, 8]:
            if idx < len(df.columns):
                col_name = str(df.columns[idx]).lower()
                if "выезд" in col_name and "специалист" in col_name:
                    inspection_col = df.columns[idx]
                    inspection_col_idx = idx
                    print(f"✓ Колонка осмотра найдена по индексу: [{idx}] {df.columns[idx]}")
                    break
    
    if inspection_col is None:
        print("⚠️ ВНИМАНИЕ: Колонка 'Выручка (выезд) специалиста' не найдена!")
    
    # 6. Колонка комментариев
    comment_col = None
    for idx, c in enumerate(df.columns):
        if "коммент" in str(c).lower():
            comment_col = c
            print(f"✓ Колонка комментариев: [{idx}] {c}")
            break
    
    print(f"\n{'='*60}")
    print(f"ИТОГО: Найдено колонок для анализа:")
    print(f"  - Заказ: {'✓' if order_col else '✗'}")
    print(f"  - Итого: {'✓' if payout_col else '✗'}")
    print(f"  - Диагностика: {'✓' if diagnostic_col else '✗'}")
    print(f"  - Осмотр (выезд): {'✓' if inspection_col else '✗'}")
    print(f"{'='*60}\n")
    
    # Обрабатываем строки
    for idx, row in df.iterrows():
        total_rows += 1
        row_dict = row.to_dict()
        
        if is_template_row(row_dict):
            continue
        
        text_cell = ""
        if order_col and pd.notna(row.get(order_col)):
            text_cell = str(row.get(order_col)).strip()
        
        if not text_cell:
            text_cell = " ".join([str(v) for v in row_dict.values() if pd.notna(v)])
        
        # Если это заголовок монтажника - пропускаем
        if is_worker_header(text_cell):
            print(f"⏭️  Пропущен заголовок монтажника: {text_cell}")
            continue
        
        order_number = extract_order_number(text_cell)
        address = extract_address(text_cell)
        
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
        if diagnostic_col is not None and pd.notna(row.get(diagnostic_col)):
            try:
                raw_val = row.get(diagnostic_col)
                if isinstance(raw_val, str):
                    val = raw_val.replace(" ", "").replace(",", ".")
                else:
                    val = str(raw_val)
                diag_sum = float(val)
                if diag_sum > 0:
                    print(f"  💰 Диагностика: {diag_sum} ₽ (заказ: {order_number})")
            except Exception as e:
                print(f"  ⚠️ Ошибка парсинга диагностики: {e}")
                diag_sum = 0.0
        
        insp_sum = 0.0
        if inspection_col is not None and pd.notna(row.get(inspection_col)):
            try:
                raw_val = row.get(inspection_col)
                if isinstance(raw_val, str):
                    val = raw_val.replace(" ", "").replace(",", ".")
                else:
                    val = str(raw_val)
                insp_sum = float(val)
                if insp_sum > 0:
                    print(f"  👁️  Осмотр (выезд): {insp_sum} ₽ (заказ: {order_number})")
            except Exception as e:
                print(f"  ⚠️ Ошибка парсинга осмотра: {e}")
                insp_sum = 0.0
        
        # DEBUG: Показываем извлечённую сумму из "Итого"
        if payout_val and payout_val > 0:
            print(f"  💵 Итого: {payout_val} ₽ (заказ: {order_number})")
        
        # Определяем тип работы (ВАЖНО: порядок имеет значение!)
        work_type = "other"
        
        if diag_sum > 0:
            work_type = "diagnostic"
            print(f"  ➜ Тип работы: ДИАГНОСТИКА")
        elif insp_sum > 0:
            work_type = "inspection"
            print(f"  ➜ Тип работы: ОСМОТР")
        elif payout_val is not None and payout_val > 5000:
            work_type = "installation"
            print(f"  ➜ Тип работы: МОНТАЖ (Итого > 5000)")
        else:
            print(f"  ➜ Тип работы: ДРУГОЕ")
        
        worker_name = None
        if worker_col and pd.notna(row.get(worker_col)):
            worker_name = str(row.get(worker_col)).strip()
            if worker_name.lower() in ["монтажник", "исполнитель", "фио", ""]:
                worker_name = None
        
        comment_value = ""
        if comment_col and pd.notna(row.get(comment_col)):
            comment_value = str(row.get(comment_col)).strip()
        
        # Проверка на проблемную строку
        is_problematic = False
        parsed_ok = True
        
        if not order_number or not address:
            is_problematic = True
            parsed_ok = False
        
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
        "problematic_count": analysis["problematic_count"],
        "hard_duplicates_sample": analysis["hard_duplicates_sample"],
        "combo_clusters_sample": analysis["combo_clusters_sample"],
        "problematic_sample": analysis["problematic_sample"],
    }

# ========== UI Эндпоинты ==========

@app.get("/", response_class=HTMLResponse)
async def ui_home(request: Request):
    """Главная страница - загрузка файла"""
    return templates.TemplateResponse("upload.html", {"request": request})

@app.get("/ui/dashboard", response_class=HTMLResponse)
async def ui_dashboard(request: Request):
    """Общий дашборд по всем файлам"""
    return templates.TemplateResponse("dashboard.html", {"request": request})

@app.get("/ui/files", response_class=HTMLResponse)
async def ui_files_list(request: Request, db: Session = Depends(get_db)):
    """Список всех файлов"""
    files = db.query(File).order_by(desc(File.uploaded_at)).all()
    
    files_data = []
    for f in files:
        total_rows = db.query(OrderRow).filter(OrderRow.file_id == f.id).count()
        problematic = db.query(OrderRow).filter(
            OrderRow.file_id == f.id, 
            OrderRow.is_problematic == True
        ).count()
        
        files_data.append({
            "id": f.id,
            "filename": f.filename,
            "uploaded_at": f.uploaded_at.strftime("%d.%m.%Y %H:%M"),
            "total_rows": total_rows,
            "problematic_rows": problematic,
        })
    
    return templates.TemplateResponse("files_list.html", {
        "request": request,
        "files": files_data
    })

@app.get("/ui/files/{file_id}", response_class=HTMLResponse)
async def ui_file_detail(request: Request, file_id: int, db: Session = Depends(get_db)):
    """Детали файла с табами"""
    file = db.query(File).filter(File.id == file_id).first()
    if not file:
        return HTMLResponse(content="<h1>Файл не найден</h1>", status_code=404)
    
    analysis = analyze_duplicates_for_file(db, file_id)
    total_rows = db.query(OrderRow).filter(OrderRow.file_id == file_id).count()
    
    return templates.TemplateResponse("file_detail.html", {
        "request": request,
        "file": file,
        "file_id": file_id,
        "total_rows": total_rows,
        "analysis": analysis,
    })

@app.get("/admin/reset", response_class=HTMLResponse)
async def ui_admin(request: Request):
    """Страница сервисных функций"""
    return templates.TemplateResponse("admin.html", {"request": request})

@app.post("/admin/reset/soft")
async def admin_reset_soft(db: Session = Depends(get_db)):
    """Мягкий сброс - удаляет данные"""
    db.query(OrderRow).delete()
    db.query(File).delete()
    db.commit()
    return {"message": "Все данные удалены"}

@app.post("/admin/reset/hard")
async def admin_reset_hard(db: Session = Depends(get_db)):
    """Жёсткий сброс - удаляет таблицы"""
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    return {"message": "База данных пересоздана"}
