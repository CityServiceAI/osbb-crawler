# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

# Константа, яка зіставляє унікальні URL датасетів з назвою міста.
# Ви можете розширити цей список, коли знайдете нові джерела.
URL_CITY_MAPPING = {
    "https://data.gov.ua/dataset/0003": "Хмельницький",
    "https://data.gov.ua/dataset/__324": "Дрогобич",
    "https://data.gov.ua/dataset/5c0171c0-2851-4a72-9646-5509d58b11ef": "Чортків",
    "https://data.gov.ua/dataset/ttepejiik-ocbb-m-bihhnur": "Вінниця",
    "https://data.gov.ua/dataset/b89a2378-cf8b-47d7-9d87-40025094d3b3": "Трускавець",
    "https://data.gov.ua/dataset/perelik-osbb": "Львів",
    "https://data.gov.ua/dataset/39e1fdeb-d151-4ab0-914d-1733f3177dba": "Луцьк",
    "https://data.gov.ua/dataset/6b8c98bc-50b2-4ff8-a027-18bf234f7edf": "Бровари",
    "https://data.gov.ua/dataset/aed6faf1-e11d-4284-8b08-f1009340ec23": "Ужгород",
    "https://data.gov.ua/dataset/3-13-perelik-obednan-spivvlasnykiv-bagatokvartyrnyh-budynkiv-osbb-vmtg": "Вінниця",
    "https://data.gov.ua/dataset/perelik-osbb-mista-vinnytsiia": "Вінниця",
    "https://data.gov.ua/dataset/perelik-stvorenikh-ob-iednan-spivvlasnikiv-baratokvartirnikh-budinkiv": "Дубно",
    "https://data.gov.ua/dataset/zytlovi-budynky-lvova": "Львів"

}


class CityEnrichmentPipeline:
    """
    Додає назву міста до OsbbRecordItem на основі його джерела (source_dataset_url),
    якщо поле 'city' відсутнє або порожнє.
    """
    
    def process_item(self, item, spider):
        # 1. Перевірка, чи вже є місто
        current_city = item.get('city')
        if current_city and current_city.strip():
            # Якщо місто вже є, нічого не робимо
            return item

        source_url = item.get('source_dataset_url')
        if not source_url:
            return item
            
        # 2. Пошук міста у мапінгу
        
        # Ми перебираємо всі URL у мапінгу, щоб знайти відповідність.
        # Це дозволяє працювати з частковими збігами (наприклад, base_url містить download_link)
        
        found_city = None
        for base_url, city_name in URL_CITY_MAPPING.items():
            if base_url in source_url:
                found_city = city_name
                break
        
        # 3. Збагачення
        if found_city:
            item['city'] = found_city
            # Необов'язково: лог, щоб бачити, які записи були виправлені
            spider.logger.debug(f"Збагачено місто '{found_city}' для {source_url}")
            
        return item
