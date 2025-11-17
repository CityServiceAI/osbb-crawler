# osbb_crawler/processors.py
import io
import csv
import json
import re
import pandas as pd
from .items import OsbbRecordItem
# Якщо потрібен Excel, тут знадобиться 'pandas' або 'openpyxl'

# osbb_crawler/processors.py (або osbb_crawler/constants.py)

# osbb_crawler/processors.py (додайте цю функцію)

def find_value_by_priority(record: dict, field_name: str, mappings: dict) -> str | None:
    """
    Знаходить значення в об'єкті 'record', перебираючи пріоритетні ключі 
    для заданого 'field_name' зі словника 'mappings'.
    Використовує толерантний пошук (fuzzy matching), ігноруючи пунктуацію 
    та пробіли, щоб обробити 'ЄДРПОУ.' або 'Адреса, юридична'.
    """
    possible_keys = mappings.get(field_name, [])
    
    # 1. Створюємо словник для швидкого пошуку, де ключі датасету очищені від пунктуації.
    # Регулярний вираз r'[\W_]+' видаляє всі символи, що не є літерами чи цифрами, 
    # включаючи пробіли та нижнє підкреслення.
    cleaned_record_map = {}
    for k, v in record.items():
        if k is None:
            continue
        # Нормалізуємо та очищуємо ключ датасету
        cleaned_key = re.sub(r'[\W_]+', '', str(k).strip().lower())
        cleaned_record_map[cleaned_key] = v

    # 2. Перебираємо всі можливі ключі з нашого FIELD_MAPPINGS
    for mapping_key in possible_keys:
        # Нормалізуємо ключ з наших мапінгів (теж очищуємо)
        key_to_find = re.sub(r'[\W_]+', '', mapping_key).strip().lower()

        # 3. Шукаємо по очищеному ключу в очищеній мапі
        if key_to_find in cleaned_record_map:
            value = cleaned_record_map[key_to_find]
            
            # Якщо знайшли непорожнє значення, повертаємо його
            if value:
                return str(value).strip()
            
    # Якщо нічого не знайдено, повертаємо None
    return None

FIELD_MAPPINGS = {
    'name': ['Назва', 'Назва ОСББ', 'Повна назва', 'OSBB_NAME', 'TheNameOfTheACMB', 'entityName', 'condominiumName', 'name_osbb', 'name'],
    'edrpou': ['ЄДРПОУ', 'Код ЄДРПОУ', 'ЕДРПОУ','ЄДРПОУ', 'Code', 'EDRPOU', 'osbb_edrpoy', 'ЄДРПОУ / ПН'],
    'address': ['Адреса', 'Місцезнаходження', 'Юридична адреса', 'Юридична Адреса', 'Address', 'ADDR', 'LegalAddress', 'address_post_name', 'address', 'adressa_osbb', "Назва суб'єкта"],
    'phone': ['Телефон', 'Phone', 'osbb_phone', 'Phone_number', "Номер телефону", 'ContactTel', 'Контактний тел', 'контактний тел.'],
    'email': ['Email', 'E-mail', 'Електронна пошта'],

    # Географічні одиниці (не використовуємо для 'address', оскільки воно об'єднується)
    'city': ['Місто', 'city', 'addressPostName', 'adminunitl4'],
    'region': ['Область', 'region', 'addressAdminUnitL2', 'address_admin_unit_l2'],

    # Компоненти адреси (використовуються для спеціальної логіки в parse_csv)
    'address_street': ['addressThoroughfare', 'Street', 'Вулиця', 'address_thoroughfare', 'actual_adress_street'],
    'address_house': ['addressLocatorDesignator', 'address_locator_designator', 'House', 'Номер будинку', 'будинок', 'actual_adress_building'],
    
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
        yield from parse_excel(raw_content, source_url)
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

    try:
        # Пробуємо вивести діалект/роздільник
        dialect = csv.Sniffer().sniff(text_content[:1024])
        reader = csv.DictReader(csv_file, dialect=dialect)
    except Exception:
        # Якщо Sniffer не впорався, використовуємо кому за замовчуванням
        csv_file.seek(0) # Переводимо курсор на початок
        reader = csv.DictReader(csv_file)
    
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
        final_address = find_value_by_priority(normalized_row, 'address', FIELD_MAPPINGS)
        street = find_value_by_priority(normalized_row, 'address_street', FIELD_MAPPINGS)
        house = find_value_by_priority(normalized_row, 'address_house', FIELD_MAPPINGS)
       
        if not final_address:
             final_address = ', '.join([p for p in [street, house] if p])
        
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
    Підтримує прямий список, а також вкладення у ключах 'records', 'data' або 'features' (GeoJSON).
    """
    try:
        data = json.loads(raw_content)
    except json.JSONDecodeError:
        print("!!! ПОМИЛКА: Неправильний JSON або кодування.")
        return

    records = []
    
    # 1. Визначення, де знаходиться масив записів
    
    # Якщо корінь JSON – це одразу список, використовуємо його
    if isinstance(data, list):
        records = data
        
    # Якщо корінь – словник, шукаємо в ньому список ОСББ:
    elif isinstance(data, dict):
        # Перевіряємо поширені ключі, включаючи GeoJSON-подібний 'features'
        for key in ['data', 'records', 'features']:
            if key in data and isinstance(data[key], list):
                records = data[key]
                break
            
    
    if not records:
        print(f"!!! ПОПЕРЕДЖЕННЯ: Список об'єктів ОСББ не знайдено в корені, 'data', 'records' або 'features'. Перевірте структуру: {source_url}")
        return

    # 2. Обробка знайденого списку та нормалізація джерела
    for record in records:
        
        # Визначаємо джерело даних. Якщо це GeoJSON Feature, 
        # використовуємо вкладений словник 'properties'.
        if isinstance(record, dict) and record.get('type') == 'Feature' and isinstance(record.get('properties'), dict):
            source_record = record['properties']
        else:
            # Інакше використовуємо сам запис
            source_record = record
            
        # Якщо з якоїсь причини запис не є словником, пропускаємо його
        if not isinstance(source_record, dict):
             print(f"!!! ПОПЕРЕДЖЕННЯ: Запис не є словником і буде пропущений: {source_url}")
             continue
            
        osbb = OsbbRecordItem()
        
        # --- 1. Збір даних (використовуємо source_record) ---
        osbb['name'] = find_value_by_priority(source_record, 'name', FIELD_MAPPINGS)
        osbb['edrpou'] = find_value_by_priority(source_record, 'edrpou', FIELD_MAPPINGS)
        osbb['phone'] = find_value_by_priority(source_record, 'phone', FIELD_MAPPINGS)
        osbb['email'] = find_value_by_priority(source_record, 'email', FIELD_MAPPINGS)
        
        osbb['region'] = find_value_by_priority(source_record, 'region', FIELD_MAPPINGS)
        osbb['city'] = find_value_by_priority(source_record, 'city', FIELD_MAPPINGS)
        
        # --- 2. Об'єднання адреси ---
        # Спочатку шукаємо повну адресу
        final_address = find_value_by_priority(source_record, 'address', FIELD_MAPPINGS)
        
        # Якщо повну адресу не знайдено, пробуємо зібрати з компонентів
        street = find_value_by_priority(source_record, 'address_street', FIELD_MAPPINGS)
        house = find_value_by_priority(source_record, 'address_house', FIELD_MAPPINGS)
        
        if not final_address:
             final_address = ', '.join([p for p in [street, house] if p])
        
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

# osbb_crawler/processors.py

# ... (інші функції)
# ... (parse_csv)
# ... (parse_json)

def parse_excel(raw_content: bytes, source_url: str):
    """
    Парсить вміст Excel-файлу (XLS/XLSX) і генерує OsbbRecordItem.
    Вимагає pandas та openpyxl.
    """
    try:
        # 1. Читаємо Excel-файл з бінарного вмісту в DataFrame
        # io.BytesIO(raw_content) дозволяє pandas читати дані з пам'яті
        df = pd.read_excel(io.BytesIO(raw_content), engine='openpyxl')
    except Exception as e:
        print(f"!!! ПОМИЛКА: Не вдалося прочитати Excel-файл {source_url}: {e}")
        return

    # 2. Перетворюємо DataFrame на список словників і обробляємо кожен рядок
    for row_dict in df.to_dict('records'):
        # Normalize keys (pandas columns) to lowercase for find_value_by_priority
        source_record = {
            (k.strip().lower() if k is not None else ''): (str(v).strip() if v is not None else '')
            for k, v in row_dict.items() 
        }
        
        osbb = OsbbRecordItem()
        
        # --- 1. Збір даних (Аналогічно CSV/JSON) ---
        osbb['name'] = find_value_by_priority(source_record, 'name', FIELD_MAPPINGS)
        osbb['edrpou'] = find_value_by_priority(source_record, 'edrpou', FIELD_MAPPINGS)
        osbb['phone'] = find_value_by_priority(source_record, 'phone', FIELD_MAPPINGS)
        osbb['email'] = find_value_by_priority(source_record, 'email', FIELD_MAPPINGS)
        
        osbb['region'] = find_value_by_priority(source_record, 'region', FIELD_MAPPINGS)
        osbb['city'] = find_value_by_priority(source_record, 'city', FIELD_MAPPINGS)

        # --- 2. Об'єднання адреси ---
        final_address = find_value_by_priority(source_record, 'address_full', FIELD_MAPPINGS)
        street = find_value_by_priority(source_record, 'address_street', FIELD_MAPPINGS)
        house = find_value_by_priority(source_record, 'address_house', FIELD_MAPPINGS)
       
        if not final_address: 
             final_address = ', '.join([p for p in [street, house] if p])
        
        osbb['address'] = final_address
        osbb['source_dataset_url'] = source_url
        
        # --- 3. Фінальне очищення та фільтрація ---
        for key in osbb.fields:
            if osbb.get(key) is None:
                osbb[key] = "" 

        if osbb.get('edrpou') or osbb.get('address'):
            yield osbb
        else:
            print(f"!!! ПОПЕРЕДЖЕННЯ: Пропущено рядок (немає ЄДРПОУ/Адреси) з Excel: {source_url}")

