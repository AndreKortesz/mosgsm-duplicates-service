import os
import io
import re
from datetime import datetime
from collections import defaultdict
import pandas as pd
from fastapi import FastAPI, UploadFile, File as FastAPIFile, Depends, Request
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
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

# Создаем директорию для статических файлов если её нет
os.makedirs("static", exist_ok=True)

# HTML шаблон главной страницы
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MOS-GSM Duplicate Checker</title>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-gradient-to-br from-gray-900 via-gray-800 to-gray-900 min-h-screen">
    <div class="container mx-auto px-4 py-8 max-w-6xl">
        <!-- Шапка -->
        <div class="bg-gray-800 rounded-lg shadow-2xl p-6 mb-6 border border-gray-700">
            <h1 class="text-3xl font-bold text-white mb-2">📊 MOS-GSM Duplicate Checker</h1>
            <p class="text-gray-400">Система проверки дублирующих выплат монтажникам</p>
        </div>

        <!-- Блок загрузки -->
        <div class="bg-gray-800 rounded-lg shadow-2xl p-6 mb-6 border border-gray-700">
            <h2 class="text-xl font-semibold text-white mb-4">📁 Загрузить Excel файл</h2>
            
            <div class="border-2 border-dashed border-gray-600 rounded-lg p-8 text-center hover:border-blue-500 transition-colors">
                <input type="file" id="fileInput" accept=".xlsx,.xls" class="hidden">
                <label for="fileInput" class="cursor-pointer">
                    <div class="text-gray-400 mb-2">
                        <svg class="w-16 h-16 mx-auto mb-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12"></path>
                        </svg>
                    </div>
                    <span class="text-blue-400 font-semibold">Нажмите для выбора файла</span>
                    <p class="text-gray-500 text-sm mt-2">или перетащите файл сюда</p>
                </label>
            </div>

            <button id="uploadBtn" class="mt-4 w-full bg-blue-600 hover:bg-blue-700 text-white font-semibold py-3 px-6 rounded-lg transition-colors disabled:bg-gray-600 disabled:cursor-not-allowed">
                Загрузить и проверить
            </button>

            <div id="progress" class="hidden mt-4">
                <div class="bg-gray-700 rounded-full h-2 overflow-hidden">
                    <div class="bg-blue-500 h-full animate-pulse" style="width: 100%"></div>
                </div>
                <p class="text-center text-gray-400 mt-2">Обработка файла...</p>
            </div>
        </div>

        <!-- Результаты -->
        <div id="results" class="hidden">
            <!-- Общая статистика -->
            <div class="bg-gray-800 rounded-lg shadow-2xl p-6 mb-6 border border-gray-700">
                <h2 class="text-xl font-semibold text-white mb-4">📈 Статистика</h2>
                <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
                    <div class="bg-gray-700 rounded-lg p-4">
                        <div class="text-gray-400 text-sm">Всего строк</div>
                        <div class="text-2xl font-bold text-white" id="totalRows">-</div>
                    </div>
                    <div class="bg-gray-700 rounded-lg p-4">
                        <div class="text-gray-400 text-sm">Сохранено</div>
                        <div class="text-2xl font-bold text-green-400" id="savedRows">-</div>
                    </div>
                    <div class="bg-gray-700 rounded-lg p-4">
                        <div class="text-gray-400 text-sm">Проблемных</div>
                        <div class="text-2xl font-bold text-yellow-400" id="problematicRows">-</div>
                    </div>
                    <div class="bg-gray-700 rounded-lg p-4">
                        <div class="text-gray-400 text-sm">Дублей</div>
                        <div class="text-2xl font-bold text-red-400" id="duplicatesCount">-</div>
                    </div>
                </div>
            </div>

            <!-- Жесткие дубли -->
            <div id="hardDuplicatesBlock" class="bg-gray-800 rounded-lg shadow-2xl p-6 mb-6 border border-red-500">
                <h2 class="text-xl font-semibold text-red-400 mb-4">🔴 Жесткие дубли (риск переплаты)</h2>
                <div id="hardDuplicatesList"></div>
            </div>

            <!-- Комбо -->
            <div id="comboBlock" class="bg-gray-800 rounded-lg shadow-2xl p-6 mb-6 border border-yellow-500">
                <h2 class="text-xl font-semibold text-yellow-400 mb-4">🟡 Комбо (осмотр + монтаж)</h2>
                <div id="comboList"></div>
            </div>

            <!-- Проблемные строки -->
            <div id="problematicBlock" class="bg-gray-800 rounded-lg shadow-2xl p-6 border border-gray-600">
                <h2 class="text-xl font-semibold text-gray-400 mb-4">⚠️ Проблемные строки</h2>
                <p class="text-gray-500 text-sm">Строки без номера заказа или адреса</p>
            </div>
        </div>
    </div>

    <script>
        const fileInput = document.getElementById('fileInput');
        const uploadBtn = document.getElementById('uploadBtn');
        const progress = document.getElementById('progress');
        const results = document.getElementById('results');

        let selectedFile = null;

        fileInput.addEventListener('change', (e) => {
            selectedFile = e.target.files[0];
            if (selectedFile) {
                uploadBtn.disabled = false;
                uploadBtn.textContent = `Загрузить: ${selectedFile.name}`;
            }
        });

        uploadBtn.addEventListener('click', async () => {
            if (!selectedFile) return;

            const formData = new FormData();
            formData.append('file', selectedFile);

            uploadBtn.disabled = true;
            progress.classList.remove('hidden');
            results.classList.add('hidden');

            try {
                const response = await fetch('/upload', {
                    method: 'POST',
                    body: formData
                });

                const data = await response.json();
                
                progress.classList.add('hidden');
                uploadBtn.disabled = false;
                uploadBtn.textContent = 'Загрузить и проверить';
                
                displayResults(data);
            } catch (error) {
                alert('Ошибка при загрузке: ' + error.message);
                progress.classList.add('hidden');
                uploadBtn.disabled = false;
            }
        });

        function displayResults(data) {
            results.classList.remove('hidden');

            // Статистика
            document.getElementById('totalRows').textContent = data.total_rows_in_file;
            document.getElementById('savedRows').textContent = data.saved_rows;
            document.getElementById('problematicRows').textContent = data.problematic_rows;
            document.getElementById('duplicatesCount').textContent = data.hard_duplicates_count;

            // Жесткие дубли
            const hardDuplicatesList = document.getElementById('hardDuplicatesList');
            if (data.hard_duplicates_sample && data.hard_duplicates_sample.length > 0) {
                hardDuplicatesList.innerHTML = data.hard_duplicates_sample.map(dup => `
                    <div class="bg-gray-700 rounded-lg p-4 mb-3">
                        <div class="text-white font-semibold mb-2">
                            ${dup.order_number} - ${dup.address}
                        </div>
                        <div class="text-sm text-gray-400 mb-2">Тип: ${translateWorkType(dup.work_type)}</div>
                        <div class="space-y-1">
                            ${dup.rows.map(row => `
                                <div class="text-sm text-gray-300 bg-gray-600 rounded p-2">
                                    💰 ${row.payout ? row.payout.toFixed(2) + ' ₽' : 'Нет суммы'} | 
                                    👤 ${row.worker_name || 'Нет имени'}
                                </div>
                            `).join('')}
                        </div>
                    </div>
                `).join('');
            } else {
                hardDuplicatesList.innerHTML = '<p class="text-gray-500">Жестких дублей не найдено ✅</p>';
            }

            // Комбо
            const comboList = document.getElementById('comboList');
            if (data.combo_clusters_sample && data.combo_clusters_sample.length > 0) {
                comboList.innerHTML = data.combo_clusters_sample.map(combo => `
                    <div class="bg-gray-700 rounded-lg p-4 mb-3">
                        <div class="text-white font-semibold mb-2">
                            ${combo.order_number} - ${combo.address}
                        </div>
                        <div class="space-y-1">
                            ${combo.rows.map(row => `
                                <div class="text-sm text-gray-300 bg-gray-600 rounded p-2">
                                    ${translateWorkType(row.work_type)} | 
                                    💰 ${row.payout ? row.payout.toFixed(2) + ' ₽' : '-'} | 
                                    👤 ${row.worker_name || '-'}
                                </div>
                            `).join('')}
                        </div>
                    </div>
                `).join('');
            } else {
                comboList.innerHTML = '<p class="text-gray-500">Комбо не найдено</p>';
            }
        }

        function translateWorkType(type) {
            const types = {
                'diagnostic': '🔍 Диагностика',
                'inspection': '👁️ Осмотр',
                'installation': '🔧 Монтаж',
                'other': '❓ Другое'
            };
            return types[type] || type;
        }
    </script>
</body>
</html>
"""

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
    # Проверяем переменную окружения для сброса БД
    if os.getenv("RESET_DB") == "true":
        print("⚠️  RESET_DB=true - Удаление всех таблиц...")
        Base.metadata.drop_all(bind=engine)
        print("✅ Таблицы удалены")
    
    # Создаём таблицы заново
    Base.metadata.create_all(bind=engine)
    print("✅ Таблицы созданы")

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

def normalize_text(text: str) -> str:
    """Нормализация текста для сравнения"""
    if not text:
        return ""
    # Убираем лишние пробелы, приводим к нижнему регистру
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
        # ВАЖНО: нормализуем адрес для правильного сравнения
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
        
        # Берём оригинальный адрес из первой строки для отображения
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
        
        # Жесткие дубли: 2+ записи с одним work_type
        for wt, items in by_type.items():
            if len(items) >= 2:
                hard_duplicates.append({
                    "order_number": order_number,
                    "address": original_address,
                    "work_type": wt,
                    "rows": [row_short(r) for r in items],
                })
        
        # Комбо: диагностика/осмотр + монтаж
        if has_diag_or_insp and has_install:
            combo_clusters.append({
                "order_number": order_number,
                "address": original_address,
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
@app.get("/", response_class=HTMLResponse)
async def root():
    """Главная страница с интерфейсом"""
    return HTML_TEMPLATE

@app.get("/ping")
def ping():
    return {"status": "ok"}

@app.post("/reset-database")
async def reset_database(db: Session = Depends(get_db)):
    """
    ВНИМАНИЕ: Удаляет ВСЕ данные из базы!
    Используйте с осторожностью.
    """
    try:
        # Удаляем все записи
        db.query(OrderRow).delete()
        db.query(File).delete()
        db.commit()
        
        return {
            "message": "База данных успешно очищена",
            "status": "success"
        }
    except Exception as e:
        db.rollback()
        return JSONResponse(
            status_code=500,
            content={"error": f"Ошибка при очистке БД: {str(e)}"}
        )

@app.get("/debug/orders/{file_id}")
async def debug_orders(file_id: int, db: Session = Depends(get_db)):
    """Посмотреть, как распарсились строки"""
    orders = db.query(OrderRow).filter(OrderRow.file_id == file_id).limit(20).all()
    
    return {
        "orders": [
            {
                "id": o.id,
                "order_number": o.order_number,
                "address": o.address[:50] if o.address else None,
                "payout": o.payout,
                "work_type": o.work_type,
                "worker_name": o.worker_name,
            }
            for o in orders
        ]
    }

@app.post("/debug/columns")
async def debug_columns(file: UploadFile = FastAPIFile(...)):
    """Посмотреть названия колонок в Excel"""
    content = await file.read()
    
    try:
        df = pd.read_excel(io.BytesIO(content), header=6)
        df.columns = [str(col).strip() if col is not None else "" for col in df.columns]
        
        return {
            "columns": list(df.columns),
            "first_row_sample": df.iloc[0].to_dict() if len(df) > 0 else {}
        }
    except Exception as e:
        return {"error": str(e)}

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
