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
    'name': ['Назва', 'Назва ОСББ', 'Повна назва', 'OSBB_NAME', 'TheNameOfTheACMB', 'entityName'],
    'edrpou': ['ЄДРПОУ', 'Код ЄДРПОУ', 'ЕДРПОУ', 'Code', 'EDRPOU'],
    'address': ['Адреса', 'Місцезнаходження', 'Юридична адреса', 'Address', 'ADDR', 'LegalAddress', 'address_post_name'],
    'phone': ['Телефон', 'Phone', 'osbb_phone', 'Phone_number', "Номер телефону", 'ContactTel'],
    'email': ['Email', 'E-mail', 'Електронна пошта'],

    # Географічні одиниці (не використовуємо для 'address', оскільки воно об'єднується)
    'city': ['Місто', 'city', 'addressPostName', 'adminunitl4'],
    'region': ['Область', 'region', 'addressAdminUnitL2', 'address_admin_unit_l2'],

    # Компоненти адреси (використовуються для спеціальної логіки в parse_csv)
    'address_street': ['addressThoroughfare', 'Street', 'Вулиця', 'address_thoroughfare'],
    'address_house': ['addressLocatorDesignator', 'address_locator_designator', 'House', 'Номер будинку', 'будинок'],
    
    # Резервний варіант для 'address', якщо об'єднання неможливе
    'address_full': ['Адреса', 'Місцезнаходження', 'Юридична адреса', 'Address', 'ADDR', 'Місцезнаходження юридичної особи'],
}

def process_file_content(raw_content: bytes, data_format: str, source_url: str):
    """
    Головна функція-диспетчер, яка викликає відповідний парсер.
    Повертає генератор OsbbRecordItem.
    """
    data_format = data_format.upper()
    
    if data_format == 'CSV':
        yield from parse_csv(raw_content, source_url)
    elif data_format in ['JSON', 'API']:
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
    Парсить вміст CSV-файлу і генерує OsbbRecordItem,
    з логікою об'єднання вулиці та номера будинку.
    """
    try:
        text_content = raw_content.decode('utf-8')
    except UnicodeDecodeError:
        text_content = raw_content.decode('cp1251', errors='ignore')
    
    csv_file = io.StringIO(text_content)
    
    # Важливо: нормалізуємо заголовки CSV до нижнього регістру без пробілів 
    # перед тим, як передавати DictReader, щоб find_value_by_priority працював коректно.
    # Для простоти поки що сподіваємося, що DictReader робить це або заголовки чисті.
    # Якщо будуть проблеми, цей етап треба додати.

    reader = csv.DictReader(io.StringIO(text_content))
    
    for row in reader:
        # Для DictReader ключі row — це заголовки файлу. 
        # Приводимо ключі рядка до нижнього регістру для коректної роботи find_value_by_priority
        # Захист від None у ключах та нормалізація
        normalized_row = {
            (k.strip().lower() if k is not None else ''): (str(v).strip() if v is not None else '')
            for k, v in row.items() 
            if not (k is None and v is None) 
        }
        
        osbb = OsbbRecordItem()
        # --- 1. Збір даних ---
        osbb['name'] = find_value_by_priority(normalized_row, 'name', FIELD_MAPPINGS)
        osbb['edrpou'] = find_value_by_priority(normalized_row, 'edrpou', FIELD_MAPPINGS)
        osbb['phone'] = find_value_by_priority(normalized_row, 'phone', FIELD_MAPPINGS)
        osbb['email'] = find_value_by_priority(normalized_row, 'email', FIELD_MAPPINGS)
        
        osbb['region'] = find_value_by_priority(normalized_row, 'region', FIELD_MAPPINGS)
        osbb['city'] = find_value_by_priority(normalized_row, 'city', FIELD_MAPPINGS)

        # --- 2. Об'єднання адреси ---
        street = find_value_by_priority(normalized_row, 'address_street', FIELD_MAPPINGS)
        house = find_value_by_priority(normalized_row, 'address_house', FIELD_MAPPINGS)
        final_address = ', '.join([p for p in [street, house] if p])
        
        if not final_address:
            final_address = find_value_by_priority(normalized_row, 'address_full', FIELD_MAPPINGS)
        
        osbb['address'] = final_address
        osbb['source_dataset_url'] = source_url
        
        # --- 3. Фінальне очищення та фільтрація ---
        
        # Гарантуємо, що всі поля є рядками, щоб уникнути помилок експорту CSV
        for key, value in osbb.items():
            if value is None:
                osbb[key] = "" 

        # НОВА ЛОГІКА ФІЛЬТРАЦІЇ: ЄДРПОУ АБО Адреса
        if osbb.get('edrpou') or osbb.get('address'):
            yield osbb
        else:
            print(f"!!! ПОПЕРЕДЖЕННЯ: Пропущено рядок (немає ЄДРПОУ/Адреси) з файлу: {source_url}")

# Функція parse_json залишається без змін (просто додайте нові поля, 
# якщо JSON-файли містять їх у простих ключах)

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
        
        # Якщо це GeoJSON, дані можуть бути вкладені у 'properties'
        normalized_record = record.get('properties') if record.get('type') == 'Feature' and isinstance(record.get('properties'), dict) else record
        
        # --- 1. Збір даних ---
        osbb['name'] = find_value_by_priority(normalized_record, 'name', FIELD_MAPPINGS)
        osbb['edrpou'] = find_value_by_priority(normalized_record, 'edrpou', FIELD_MAPPINGS)
        osbb['phone'] = find_value_by_priority(normalized_record, 'phone', FIELD_MAPPINGS)
        osbb['email'] = find_value_by_priority(normalized_record, 'email', FIELD_MAPPINGS)
        
        osbb['region'] = find_value_by_priority(normalized_record, 'region', FIELD_MAPPINGS)
        osbb['city'] = find_value_by_priority(normalized_record, 'city', FIELD_MAPPINGS)
        
        # --- 2. Об'єднання адреси (Логіка, перенесена з CSV) ---
        street = find_value_by_priority(normalized_record, 'address_street', FIELD_MAPPINGS)
        house = find_value_by_priority(normalized_record, 'address_house', FIELD_MAPPINGS)
        final_address = ', '.join([p for p in [street, house] if p])
        
        if not final_address:
            final_address = find_value_by_priority(normalized_record, 'address_full', FIELD_MAPPINGS)
        
        osbb['address'] = final_address
        osbb['source_dataset_url'] = source_url
        
        # --- 3. Фінальне очищення та фільтрація ---
        
        # Гарантуємо, що всі поля є рядками, щоб уникнути помилок експорту CSV
        for key in osbb.fields:
            if osbb.get(key) is None:
                osbb[key] = "" 

        # НОВА ЛОГІКА ФІЛЬТРАЦІЇ: ЄДРПОУ АБО Адреса
        if osbb.get('edrpou') or osbb.get('address'):
            yield osbb
        else:
            print(f"!!! ПОПЕРЕДЖЕННЯ: Пропущено рядок (немає ЄДРПОУ/Адреси) з JSON/API: {source_url}")

def parse_excel(raw_content: bytes, source_url: str):
    """
    Парсить вміст Excel-файлу (XLS/XLSX) і генерує OsbbRecordItem.
    """
    # Це приклад, як би виглядала логіка, якщо Pandas доступний. 
    # TODO Додати парсинг для excel.
    try:
        df = pd.read_excel(io.BytesIO(raw_content), engine='openpyxl')
        for row_dict in df.to_dict('records'):
            normalized_row = {
                (k.strip().lower() if k is not None else ''): (str(v).strip() if v is not None else '')
                for k, v in row_dict.items() 
            }
            # ... (Вся логіка збору, об'єднання адреси та фільтрації, як у parse_csv)
            # ... (для прикладу: pass)
            pass
            
    except Exception as e:
        print(f"!!! ПОМИЛКА: Не вдалося прочитати Excel-файл {source_url}: {e}")

