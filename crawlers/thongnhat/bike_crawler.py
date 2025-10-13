"""
Crawler để lấy dữ liệu xe đạp từ thongnhat.com.vn
FIXED: Chạy như subprocess để tránh xung đột với Airflow logging
"""
import scrapy
from datetime import datetime
import json
import sys


class BikeSpider(scrapy.Spider):
    """Spider crawl thông tin xe đạp"""
    
    name = 'bike_crawler'
    start_urls = ["https://thongnhat.com.vn/muc/san-pham"]
    
    custom_settings = {
        'ROBOTSTXT_OBEY': True,
        'DOWNLOAD_DELAY': 0.3,
        'CONCURRENT_REQUESTS': 16,
        'RETRY_TIMES': 3,
        'DOWNLOAD_TIMEOUT': 30,
        'USER_AGENT': 'Mozilla/5.0 (compatible; BikeDataBot/1.0)',
        'LOG_LEVEL': 'INFO',
    }

    def __init__(self, output_file=None, *args, **kwargs):
        super(BikeSpider, self).__init__(*args, **kwargs)
        self.output_file = output_file or '/tmp/bikes.json'
        self.results = []

    def parse(self, response):
        """Parse trang danh sách sản phẩm"""
        self.logger.info(f'Crawling: {response.url}')

        links = response.xpath(
            '//a[@class="woocommerce-LoopProduct-link woocommerce-loop-product__link"]/@href'
        ).getall()
        
        for link in links:
            if link:
                yield scrapy.Request(url=link, callback=self.parse_detail)
        
        # Pagination
        next_page = response.xpath('//a[@class="next page-numbers"]/@href').get()
        if next_page:
            self.logger.info(f'Next page: {next_page}')
            yield scrapy.Request(url=next_page, callback=self.parse)
    
    def parse_detail(self, response):
        """Parse chi tiết sản phẩm"""
        product_name = response.xpath('//h1[@class="product_title entry-title"]/text()').get()
        price = response.xpath('//p[@class="price"]/span[@class="woocommerce-Price-amount amount"]/bdi/text()').get()
        
        bike_info = {
            'crawled_at': datetime.now().isoformat(),
            'url': response.url,
            'tenxe': product_name.strip() if product_name else None,
            'price': price.strip() if price else None,
            'doituongsudung': None,
            'cokhung': None,
            'khungsuon': None,
            'phuoc': None,
            'taylac': None,
            'detruoc': None,
            'desau': None,
            'diaxich': None,
            'phanh': None,
            'sizebanh': None,
            'lop': None,
            'vanh': None,
            'taylai': None,
            'trucmayo': None
        }
        
        # Parse specs
        specs = response.xpath('//div[@id="product-spec"]//ul[@class="ttsp"]/li')
        for spec in specs:
            label = spec.xpath('.//span[@class="lable-col"]/text()').get()
            content = spec.xpath('.//span[@class="content-col"]/text()').get()
            
            if label and content:
                label = label.strip()
                content = content.strip()
                
                mapping = {
                    'Đối tượng sử dụng': 'doituongsudung',
                    'Chiều cao phù hợp': 'cokhung',
                    'Size': 'cokhung',
                    'Khung xe': 'khungsuon',
                    'Phuộc': 'phuoc',
                    'Tay bấm đề': 'taylac',
                    'Gạt đĩa': 'detruoc',
                    'Gạt líp': 'desau',
                    'Đùi đĩa': 'diaxich',
                    'Phanh': 'phanh',
                    'Size bánh': 'sizebanh',
                    'Lốp': 'lop',
                    'Vành': 'vanh',
                    'Ghi đông': 'taylai',
                    'Moay ơ': 'trucmayo'
                }
                
                for key, field in mapping.items():
                    if key in label:
                        bike_info[field] = content
                        break
        
        yield bike_info


# Script chạy độc lập
if __name__ == "__main__":
    from scrapy.crawler import CrawlerProcess
    import os
    
    # Lấy output path từ argument
    output_path = sys.argv[1] if len(sys.argv) > 1 else '/tmp/bikes.json'
    
    # Đảm bảo thư mục tồn tại
    os.makedirs(os.path.dirname(output_path) or '/tmp', exist_ok=True)
    
    process = CrawlerProcess(settings={
        'FEEDS': {
            output_path: {
                'format': 'json',
                'encoding': 'utf-8',
                'indent': 2
            },
        },
        'LOG_LEVEL': 'INFO',
    })
    
    process.crawl(BikeSpider, output_file=output_path)
    process.start()
    
    print(f"CRAWL_SUCCESS:{output_path}")