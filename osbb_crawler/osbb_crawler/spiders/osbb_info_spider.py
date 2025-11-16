# osbb_crawler/spiders/osbb_registry_spider.py
import scrapy
# Нам не потрібен Item, оскільки ми збираємо URL для іншого павука.
# Але для прикладу залишимо його структуру.
from ..items import OsbbRecordItem
from ..items import DatasetItem
from ..processors import process_file_content

class OsbbRegistrySpider(scrapy.Spider):
    name = 'osbb_registry'
    allowed_domains = ['data.gov.ua', 'admin-gis.khm.gov.ua'] 
    
    # початковий URL сторінки пошуку 'осбб'
    start_urls = ['https://data.gov.ua/dataset?q=%D0%BE%D1%81%D0%B1%D0%B1&sort=score+desc%2C+metadata_modified+desc&_organization_limit=0']

    # Формати, які ми хочемо завантажити (файли даних)
    TARGET_FORMATS = ['csv', 'json', 'xlsx', 'xls', 'api'] 

    # =================================================================
    # !!! ТИМЧАСОВА ЛОГІКА ДЛЯ ТЕСТУВАННЯ ОДНОГО ФАЙЛУ !!!
    # Після тестування цей метод потрібно ЗАКОМЕНТУВАТИ або ВИДАЛИТИ.
    # =================================================================
    def start_requests(self):
        # 1. Замініть цей URL на реальне ПРЯМЕ посилання на файл (з Кроку 1)
        TEST_FILE_URL = 'https://admin-gis.khm.gov.ua/public-api/get/data_infrastructure.osbb?key=4236665236&limit=100'
        
        # 2. Вкажіть формат файлу, який ви тестуєте
        FILE_FORMAT = 'JSON' # Або 'JSON', 'XLSX'

        # Створюємо фіктивний DatasetItem для передачі метаданих в parse_file_content
        test_item = DatasetItem(
            title='Test Single File', 
            page_url='http://test-page.local',
            download_link=TEST_FILE_URL,
            data_format=FILE_FORMAT
        )
        
        self.logger.info(f"Запуск тестування парсингу файлу: {TEST_FILE_URL}")
        
        # Генеруємо запит, який одразу викликає parse_file_content
        yield scrapy.Request(
            url=TEST_FILE_URL,
            callback=self.parse_file_content,
            meta={'dataset_metadata': test_item}
        )

    def parse(self, response):
        """
        Обробляє сторінку зі списком наборів даних (Перший рівень).
        """
        dataset_items = response.css('li.info-list__item')
        
        for item_block in dataset_items:
            # Витягуємо посилання на деталі
            detail_url_partial = item_block.css('h3.info-list__item-content-heading a::attr(href)').get()
            
            # Перевіряємо, чи є потрібний формат у списку міток формату
            format_labels = item_block.css('div.info-list__item-content ul.formats li::text').getall()
            has_target_format = any(
                fmt.lower().strip() in self.TARGET_FORMATS for fmt in format_labels
            )
            
            if has_target_format and detail_url_partial:
                full_detail_url = response.urljoin(detail_url_partial)
                
                # Генеруємо запит на деталі
                yield scrapy.Request(
                    url=full_detail_url, 
                    callback=self.parse_dataset_details,
                    cb_kwargs={
                        'name': item_block.css('h3.info-list__item-content-heading a::text').get().strip(),
                        'description': item_block.css('div.info-list__item-content div:nth-child(4)::text').get().strip()
                    }
                )
        
        # 3. Обробка Пагінації (Перехід на наступну сторінку)
        # Цей CKAN використовує пагінацію в кінці сторінки. 
        # Потрібно знайти посилання на наступну сторінку.
        # Точний селектор пагінації потрібно знайти на повній сторінці, але типовий вигляд:
        next_page = response.css('li.next a::attr(href)').get() 
        
        if next_page is not None:
            yield response.follow(next_page, callback=self.parse)


    def parse_dataset_details(self, response, name, description):
        """
        Обробляє детальні сторінки, знаходить прямі посилання на файли і 
        створює DatasetItem.
        """
        # Типовий селектор для елементів ресурсів на деталях CKAN-сторінки
        resource_links = response.css('section.additional-info div.resource-item')
        
        for link_item in resource_links:
            download_url = link_item.css('a.btn::attr(href)').get()
            format_label = link_item.css('a.format-label::text').get() 
            
            if download_url and format_label and format_label.strip().lower() in self.TARGET_FORMATS:
                
                # Створюємо DatasetItem
                dataset_item = DatasetItem()
                dataset_item['title'] = name
                dataset_item['description'] = description
                dataset_item['page_url'] = response.url
                dataset_item['download_link'] = response.urljoin(download_url)
                dataset_item['data_format'] = format_label.strip().upper()
                
                # !!! КЛЮЧОВИЙ МОМЕНТ: Генеруємо запит на сам файл !!!
                # Ми використовуємо тут метод `parse_file_content`, який буде визначено 
                # як наступний крок.
                
                yield scrapy.Request(
                    url=dataset_item['download_link'],
                    callback=self.parse_file_content,
                    # Передаємо метадані набору даних, щоб знати джерело
                    meta={'dataset_metadata': dataset_item}
                )

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