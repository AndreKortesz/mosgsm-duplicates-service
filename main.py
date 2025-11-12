import os
import io
import re
from datetime import datetime
from collections import defaultdict
from typing import Optional
import pandas as pd
from fastapi import FastAPI, UploadFile, File as FastAPIFile, Depends, Request, Query
from fastapi.responses import JSONResponse, HTMLResponse, StreamingResponse
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
app = FastAPI(title="MOS-GSM Duplicate Checker")

# Создаем директорию для шаблонов
os.makedirs("templates", exist_ok=True)

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
    created_at = Column(DateTime, default=datetime.utcnow)
    file = relationship("File", back_populates="orders")

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
    if not text:
        return None
    
    match = re.search(r"от\s+\d{2}\.\d{2}\.\d{4}\s+[\d:]+,\s*(.+)$", text)
    if match:
        return match.group(1).strip()
    
    match = re.search(r"от\s+\d{2}\.\d{2}\.\d{4}[^,]*,\s*(.+)$", text)
    if match:
        return match.group(1).strip()
    
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

def analyze_duplicates_for_file(db: Session, file_id: int) -> dict:
    """Анализ с учётом проблемных строк"""
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
        key = (
            r.order_number.strip().upper(),
            normalize_text(r.address)
        )
        clusters[key].append(r)
    
    hard_duplicates = []
    combo_clusters = []
    clusters_with_multiple = []
    
    for (order_number, normalized_address), rows in clusters.items():
        if len(rows) < 2:
            continue
        
        original_address = rows[0].address
        clusters_with_multiple.append((order_number, original_address, rows))
        
        by_type = defaultdict(list)
        has_diag_or_insp = False
        has_install = False
        
        for r in rows:
            by_type[r.work_type].append(r)
            if r.work_type in ("diagnostic", "inspection"):
                has_diag_or_insp = True
            if r.work_type == "installation":
                has_install = True
        
        for wt, items in by_type.items():
            if len(items) >= 2:
                hard_duplicates.append({
                    "order_number": order_number,
                    "address": original_address,
                    "work_type": wt,
                    "rows": [row_short(r) for r in items],
                })
        
        if has_diag_or_insp and has_install:
            combo_clusters.append({
                "order_number": order_number,
                "address": original_address,
                "rows": [row_short(r) for r in rows],
            })
    
    # ВАЖНО: Добавляем проблемные строки
    problematic_orders = (
        db.query(OrderRow)
        .filter(OrderRow.file_id == file_id, OrderRow.is_problematic == True)
        .all()
    )
    
    return {
        "clusters_with_multiple_count": len(clusters_with_multiple),
        "hard_duplicates_count": len(hard_duplicates),
        "combo_clusters_count": len(combo_clusters),
        "problematic_count": len(problematic_orders),
        "hard_duplicates_sample": hard_duplicates[:30],
        "combo_clusters_sample": combo_clusters[:30],
        "problematic_sample": [row_short(r) for r in problematic_orders[:30]],
    }

# ========== API Эндпоинты ==========

@app.get("/api/files")
async def api_get_files(db: Session = Depends(get_db)):
    """Список всех файлов"""
    files = db.query(File).order_by(desc(File.uploaded_at)).all()
    
    result = []
    for f in files:
        total_rows = db.query(OrderRow).filter(OrderRow.file_id == f.id).count()
        problematic = db.query(OrderRow).filter(
            OrderRow.file_id == f.id, 
            OrderRow.is_problematic == True
        ).count()
        
        result.append({
            "id": f.id,
            "filename": f.filename,
            "uploaded_at": f.uploaded_at.isoformat(),
            "total_rows": total_rows,
            "problematic_rows": problematic,
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
    elif what in ["hard", "combo", "clusters"]:
        analysis = analyze_duplicates_for_file(db, file_id)
        if what == "hard":
            data = analysis["hard_duplicates_sample"]
        elif what == "combo":
            data = analysis["combo_clusters_sample"]
        else:
            data = []
    else:
        return JSONResponse(status_code=400, content={"error": "Invalid export type"})
    
    # Создаём CSV
    df = pd.DataFrame(data)
    output = io.StringIO()
    df.to_csv(output, index=False, encoding='utf-8-sig')
    output.seek(0)
    
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={file.filename}_{what}.csv"}
    )

@app.post("/upload")
async def upload_file(
    file: UploadFile = FastAPIFile(...),
    db: Session = Depends(get_db),
):
    """Загрузка и обработка Excel файла"""
    content = await file.read()
    
    try:
        df = pd.read_excel(io.BytesIO(content), header=6)
        df.columns = [str(col).strip() if col is not None else "" for col in df.columns]
    except Exception as e:
        return JSONResponse(
            status_code=400,
            content={"error": f"Не удалось прочитать Excel: {str(e)}"},
        )
    
    db_file = File(filename=file.filename)
    db.add(db_file)
    db.commit()
    db.refresh(db_file)
    
    total_rows = 0
    inserted_rows = 0
    problematic_rows = 0
    
    # Находим колонки
    order_col = None
    for c in df.columns:
        if "заказ" in str(c).lower() and "комментар" in str(c).lower():
            order_col = c
            break
    if order_col is None:
        possible_order_cols = [c for c in df.columns if "заказ" in str(c).lower()]
        order_col = possible_order_cols[0] if possible_order_cols else None
    
    payout_col = None
    for c in df.columns:
        name = str(c).strip()
        if name == "Итого" or "итого" in name.lower():
            payout_col = c
            break
    
    worker_col = None
    for c in df.columns:
        name = str(c).lower()
        if "монтажник" in name or "фио" in name or "исполнитель" in name:
            worker_col = c
            break
    if worker_col is None and len(df.columns) > 0:
        worker_col = df.columns[0]
    
    diagnostic_col = None
    for c in df.columns:
        name = str(c).lower()
        if "диагност" in name:
            diagnostic_col = c
            break
    
    inspection_col = None
    for c in df.columns:
        name = str(c).lower()
        if ("выручка" in name and "выезд" in name and "специалист" in name) or \
           (name == "выручка (выезд) специалиста"):
            inspection_col = c
            break
    
    comment_col = None
    for c in df.columns:
        if "коммент" in str(c).lower():
            comment_col = c
            break
    
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
        
        work_type = "other"
        if diag_sum > 0:
            work_type = "diagnostic"
        elif insp_sum > 0:
            work_type = "inspection"
        elif payout_val is not None and payout_val > 5000:
            work_type = "installation"
        
        worker_name = None
        if worker_col and pd.notna(row.get(worker_col)):
            worker_name = str(row.get(worker_col)).strip()
            if worker_name.lower() in ["монтажник", "исполнитель", "фио", ""]:
                worker_name = None
        
        comment_value = ""
        if comment_col and pd.notna(row.get(comment_col)):
            comment_value = str(row.get(comment_col)).strip()
        
        # ВАЖНО: Проблемная строка если нет номера ИЛИ нет адреса
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

