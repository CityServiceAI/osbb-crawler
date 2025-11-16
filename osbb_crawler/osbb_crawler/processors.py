# osbb_crawler/processors.py
import io
import csv
import json
from .items import OsbbRecordItem
# Якщо потрібен Excel, тут знадобиться 'pandas' або 'openpyxl'

# osbb_crawler/processors.py (або osbb_crawler/constants.py)

# osbb_crawler/processors.py (додайте цю функцію)

def find_value_by_priority(record: dict, field_name: str, mappings: dict) -> str | None:
    """
    Знаходить значення в об'єкті 'record', перебираючи пріоритетні ключі 
    для заданого 'field_name' зі словника 'mappings'.
    """
    # 1. Отримуємо список можливих ключів для цього поля
    possible_keys = mappings.get(field_name, [])
    
    # 2. Перебираємо всі можливі ключі
    for key in possible_keys:
        value = record.get(key.strip().lower()) 
        
        # 3. Якщо знайшли непорожнє значення, повертаємо його
        if value:
            # Можна додати тут очищення/нормалізацію значення (strip())
            return str(value).strip()
            
    # Якщо нічого не знайдено, повертаємо None
    return None

FIELD_MAPPINGS = {
    'name': ['Назва', 'Назва ОСББ', 'Повна назва', 'OSBB_NAME'],
    'edrpou': ['ЄДРПОУ', 'Код ЄДРПОУ', 'ЕДРПОУ', 'Code'],
    'address': ['Адреса', 'Місцезнаходження', 'Юридична адреса', 'Address', 'ADDR'],
    'phone': ['Телефон', 'Phone', 'osbb_phone', 'Phone_number', "Номер телефону"]
    # Додайте інші поля, які ви плануєте збирати
}

def process_file_content(raw_content: bytes, data_format: str, source_url: str):
    """
    Головна функція-диспетчер, яка викликає відповідний парсер.
    Повертає генератор OsbbRecordItem.
    """
    data_format = data_format.upper()
    
    if data_format == 'CSV':
        yield from parse_csv(raw_content, source_url)
    elif data_format == 'JSON':
        yield from parse_json(raw_content, source_url)
    elif data_format in ['XLS', 'XLSX']:
        # Потрібна зовнішня бібліотека. Логіку додасте пізніше.
        print(f"Потрібна логіка для парсингу Excel ({data_format})")
        pass
    else:
        print(f"Непідтримуваний формат: {data_format}")

# --- Конкретні функції парсингу ---

def parse_csv(raw_content: bytes, source_url: str):
    """
    Парсить вміст CSV-файлу і генерує OsbbRecordItem.
    """
    # Декодуємо байти в текст, використовуючи io.StringIO
    # (часто потрібне правильне кодування, наприклад 'utf-8' або 'cp1251')
    try:
        text_content = raw_content.decode('utf-8')
    except UnicodeDecodeError:
        # Спроба іншого кодування, якщо UTF-8 не спрацювало
        text_content = raw_content.decode('cp1251', errors='ignore')
    
    # Використовуємо DictReader для легкого доступу до полів
    csv_file = io.StringIO(text_content)
    reader = csv.DictReader(csv_file)
    
    for row in reader:
        # Тут вам треба знати імена колонок у файлі (наприклад, 'Назва', 'ЄДРПОУ')
        # І адаптувати їх до OsbbRecordItem
        
        osbb = OsbbRecordItem()
        # ПРИКЛАД: Адаптуйте імена ключів під реальні назви колонок у файлі!
        osbb['name'] = find_value_by_priority(row, 'name', FIELD_MAPPINGS)
        osbb['edrpou'] = find_value_by_priority(row, 'edrpou', FIELD_MAPPINGS)
        osbb['address'] = find_value_by_priority(row, 'address', FIELD_MAPPINGS)
        osbb['phone'] = find_value_by_priority(row, 'phone', FIELD_MAPPINGS)
    
        osbb['source_dataset_url'] = source_url  
        yield osbb

def parse_json(raw_content: bytes, source_url: str):
    """
    Парсить вміст JSON-файлу і генерує OsbbRecordItem.
    Підтримує прямий список, а також вкладення у ключах 'records' або 'data'.
    """
    try:
        data = json.loads(raw_content)
    except json.JSONDecodeError:
        print("!!! ПОМИЛКА: Неправильний JSON або кодування.")
        return

    records = []
    
    # 1. Якщо корінь JSON – це одразу список, використовуємо його
    if isinstance(data, list):
        records = data
        
    # 2. Якщо корінь – словник, шукаємо в ньому список ОСББ:
    elif isinstance(data, dict):
        # Шукаємо у ключі 'data' (як ви виявили)
        if 'data' in data and isinstance(data['data'], list):
            records = data['data']
        # Або шукаємо у ключі 'records' (загальна конвенція)
        elif 'records' in data and isinstance(data['records'], list):
            records = data['records']
            
    
    if not records:
        print(f"!!! ПОПЕРЕДЖЕННЯ: Список об'єктів ОСББ не знайдено в корені, 'data' або 'records'. Перевірте структуру: {source_url}")
        return # Якщо список порожній або не знайдено, завершуємо роботу.

    # 3. Обробка знайденого списку
    for record in records:
        osbb = OsbbRecordItem()
        
        # Використовуємо універсальний пошуковик, який ви раніше створили
        osbb['name'] = find_value_by_priority(record, 'name', FIELD_MAPPINGS)
        osbb['edrpou'] = find_value_by_priority(record, 'edrpou', FIELD_MAPPINGS)
        osbb['address'] = find_value_by_priority(record, 'address', FIELD_MAPPINGS)
        osbb['phone'] = find_value_by_priority(record, 'phone', FIELD_MAPPINGS)
        
        osbb['source_dataset_url'] = source_url
        
        yield osbb

