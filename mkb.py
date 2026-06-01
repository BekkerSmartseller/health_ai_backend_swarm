import pandas as pd
import asyncio
import logging
from services.hindsight_client import HindsightClient

logger = logging.getLogger(__name__)

# https://data.apicrafter.ru/packages/nsimz-4689
# 1. Чтение данных
df = pd.read_excel("/home/bekker/projects/health_ai_backend_swarm/load_data/mkb10.xlsx", header=2, dtype=str)
df.columns = ["code", "name", "parent_code"]
df = df.dropna(subset=['code']).copy()
df['code'] = df['code'].astype(str).str.strip()
df['name'] = df['name'].astype(str).str.strip()
df['parent_code'] = df['parent_code'].fillna('').astype(str).str.strip()

parent_map = dict(zip(df['code'], df['parent_code']))

# 2. Определяем рубрики (коды без точки, без дефиса, не пустые и не являются блоками/классами)
# Простой способ: рубрика — код из 3 символов (буква + 2 цифры)
def is_rubric(code):
    return len(code) == 3 and code[0].isalpha() and code[1:].isdigit()

rubric_codes = [c for c in df['code'].unique() if is_rubric(c)]

# 3. Построение документа для одной рубрики
def build_rubric_doc(rubric_code):
    rubric_row = df[df['code'] == rubric_code].iloc[0]
    name = rubric_row['name']
    
    # Строим полный путь вверх
    path = []
    current = rubric_code
    while current in parent_map and parent_map[current] != '':
        parent = parent_map[current]
        parent_name = df[df['code'] == parent]['name'].values[0] if parent in df['code'].values else ''
        path.append(f"{parent} ({parent_name})")
        current = parent
    # путь от рубрики до класса (включительно)
    hierarchy = " → ".join(path) if path else "корень"
    
    lines = [f"Код МКБ-10: {rubric_code} — {name}."]
    lines.append(f"Иерархия: {rubric_code} → {hierarchy}.")
    
    # Подрубрики (дети)
    children = df[df['parent_code'] == rubric_code]
    if not children.empty:
        lines.append("Подрубрики:")
        for _, ch in children.iterrows():
            lines.append(f"- {ch['code']} ({ch['name']}) — потомок {rubric_code}.")
    else:
        lines.append("Нет подрубрик (конечный код).")
    
    return "\n".join(lines)

# 4. Загрузка
async def load_mkb10():
    client = HindsightClient()
    bank_id = "mkb-10"
    
    logger.info(f"Найдено рубрик: {len(rubric_codes)}")
    
    for rubric in rubric_codes:
        doc = build_rubric_doc(rubric)
        if len(doc) > 50000:
            logger.warning(f"Рубрика {rubric} слишком большая, пропуск")
            continue
        
        logger.info(f"Загрузка рубрики {rubric} (длина {len(doc)} символов)")
        res = await client.retain(bank_id, doc, metadata={"type": "mkb10_rubric", "code": rubric})
        if res:
            logger.info(f"Рубрика {rubric} сохранена (id={res})")
        else:
            logger.error(f"Ошибка сохранения рубрики {rubric}")
        await asyncio.sleep(0.3)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(load_mkb10())