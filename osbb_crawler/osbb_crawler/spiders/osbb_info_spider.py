# osbb_crawler/spiders/osbb_registry_spider.py
import scrapy
from ..items import OsbbRecordItem
from ..items import DatasetItem
from ..processors import process_file_content

class OsbbRegistrySpider(scrapy.Spider):
    name = 'osbb_registry'
    allowed_domains = ['data.gov.ua', 'admin-gis.khm.gov.ua'] 
    
    # початковий URL сторінки пошуку 'осбб'
    start_urls = ['https://data.gov.ua/dataset?q=%D0%BE%D1%81%D0%B1%D0%B1&sort=score+desc%2C+metadata_modified+desc&_organization_limit=0']

    # Формати, які ми хочемо завантажити (файли даних)
    TARGET_FORMATS = ['csv', 'json', 'api', 'xlsx', 'xls'] 


    # У вашому Spider (osbb_crawler/spiders/osbb_registry.py)

    # У вашому Spider (osbb_crawler/spiders/osbb_registry.py)

    def parse(self, response):
        # Селектор li.info-list__item підтверджено
        dataset_items = response.css('li.info-list__item')

        for item in dataset_items:
            # Збір даних для передачі: Назва, Опис, URL
            dataset_title = item.css('h3.info-list__item-content-heading a.truncate::text').get(default='').strip()
            
            # Опис знаходиться в div одразу після h3, або можна спробувати більш специфічний селектор:
            description_lines = item.css('div.info-list__item-content div::text').getall()
            description = ' '.join(line.strip() for line in description_lines if line.strip())
            
            relative_url = item.css('h3.info-list__item-content-heading a.truncate::attr(href)').get()
            
            if relative_url:
                dataset_url = response.urljoin(relative_url)
                
                # Передаємо назву та опис через 'meta'
                yield scrapy.Request(
                    dataset_url, 
                    callback=self.parse_dataset_details,
                    meta={
                        'dataset_title': dataset_title,
                        'dataset_description': description
                    }
                )

        # Логіка пагінації
        next_page = response.css('.pagination a[rel="next"]::attr(href)').get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)
        


    # У вашому Spider (osbb_crawler/spiders/osbb_registry.py)
    # Припускаємо, що self.TARGET_FORMATS визначено, наприклад:
    # TARGET_FORMATS = ['csv', 'xlsx', 'xls', 'json', 'xml'] 

    def parse_dataset_details(self, response):
        """
        Обробляє детальні сторінки, знаходить ПРІОРИТЕТНЕ посилання на файл 
        і створює DatasetItem/запит, використовуючи вашу логіку пріоритетів.
        """
        name = response.meta.get('dataset_title', 'N/A')
        description = response.meta.get('dataset_description', 'N/A')

        available_resources = {}

        # *** ОНОВЛЕНІ СЕЛЕКТОРИ КОНТЕЙНЕРІВ РЕСУРСІВ ***
        # Використовуємо клас, що містить і посилання, і формат
        resource_containers = response.css('div.resource-list__item-download')
        
        # Якщо цей контейнер знаходиться всередині li.resource-list__item, 
        # можливо, краще шукати li, що містить цей div, але почнемо з прямого div.
        # Спробуємо також захопити li, оскільки можуть бути інші формати, які виглядають інакше
        if not resource_containers:
            resource_containers = response.css('li.resource-list__item, li.list-group-item')

        # Етап 1: Збираємо всі доступні ресурси
        for item in resource_containers:
            
            # 1. Пошук URL (На основі вашого HTML)
            # Посилання знаходиться всередині елемента-контейнера
            download_url = item.css('a.resource-url-analytics::attr(href)').get() 
            
            # Резервна копія для старих шаблонів
            if not download_url:
                download_url = item.css('a.btn::attr(href)').get()
                
            # 2. Пошук Формату (На основі вашого HTML: <p class="label">CSV</p>)
            format_label = (
                item.css('p.label::text').get() or # <--- НОВИЙ СЕЛЕКТОР (НАЙТОЧНІШИЙ)
                item.css('span.label-info::text').get() or 
                item.css('a.format-label::text').get() 
            )
            
            if download_url and format_label:
                norm_format = format_label.strip().lower()
                
                # Перевірка, чи це один із цільових форматів
                if norm_format in self.TARGET_FORMATS:
                    available_resources[norm_format] = response.urljoin(download_url)
            
        # Етап 2: Знаходимо найбільш пріоритетний ресурс
        best_format = None
        best_url = None
        
        for target_format in self.TARGET_FORMATS:
            if target_format in available_resources:
                best_format = target_format
                best_url = available_resources[target_format]
                break
        
        # Етап 3: Створення запиту, якщо ресурс знайдено
        if best_url:
            # Тут треба використати вашу модель Item, наприклад, OsbbCrawlerItem
            # Я використовую фіктивний DatasetItem для прикладу
            dataset_item = {
                'title': name,
                'description': description,
                'page_url': response.url,
                'download_link': best_url,
                'data_format': best_format.upper(), 
            }
            
            self.logger.info(f"Набір '{name}' обрано: {best_format.upper()}")
            
            # Генеруємо запит на ПРІОРИТЕТНИЙ файл
            yield scrapy.Request(
                url=best_url,
                callback=self.parse_file_content,
                meta={'dataset_metadata': dataset_item}
            )
        else:
            self.logger.info(f"Набір '{name}' ігнорується: не знайдено жодного цільового формату.")

    def parse_file_content(self, response):
        """
        Завантажує вміст файлу і передає його зовнішнім обробникам.
        Генерує OsbbRecordItem для Pipeline.
        """
        dataset_item = response.meta.get('dataset_metadata')
        
        # Отримуємо сирі байти або текст
        raw_content = response.body 
        data_format = dataset_item['data_format']
        source_url = dataset_item['page_url']

        self.logger.info(f"Обробка файлу: {dataset_item['download_link']} ({data_format})")

        # Передаємо роботу зовнішньому модулю process_file_content
        # Цей генератор поверне нам готові об'єкти OsbbRecordItem
        for osbb_record in process_file_content(raw_content, data_format, source_url):
            yield osbb_record