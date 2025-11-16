# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

# osbb_crawler/items.py
import scrapy

class DatasetItem(scrapy.Item):
    # Назва набору даних
    title = scrapy.Field()
    # Опис набору
    description = scrapy.Field()
    # URL детальної сторінки на data.gov.ua
    page_url = scrapy.Field()
    # Пряме посилання на завантаження файлу (CSV, XLSX, JSON)
    download_link = scrapy.Field()
    # Формат файлу (CSV, XLSX, JSON)
    data_format = scrapy.Field()
    
    pass

class OsbbRecordItem(scrapy.Item):
    # Назва організації (ОСББ)
    name = scrapy.Field()
    
    # Реєстраційний код (ЄДРПОУ)
    edrpou = scrapy.Field()
    
    # Фізична адреса реєстрації
    address = scrapy.Field()
    
    # Контактний телефон
    phone = scrapy.Field()
    
    # Електронна пошта
    email = scrapy.Field()
    
    # URL сторінки, з якої витягнуто дані
    source_dataset_url = scrapy.Field()
    
    pass
