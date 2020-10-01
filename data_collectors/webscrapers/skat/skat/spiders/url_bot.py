import os
import json
from datetime import datetime
import scrapy
this_file_path = os.path.dirname(os.path.abspath(__file__))


class SkatUrlSpider(scrapy.Spider):
    name = 'SkatUrlSpider'

    def __init__(self, **kwargs):
        self.url_info: dict = {}

        self.url_file_folder: str = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../data')
        if not os.path.exists(self.url_file_folder):
            self.logger.info(f'creating data folder at "{self.url_file_folder}"')
            os.mkdir(self.url_file_folder)

    def closed(self, reason):
        
        if reason == 'finished':
            self.logger.info(f'found {len(self.url_info)} urls in total')

            # write url file to local file
            with open(f'{self.url_file_folder}/urls.json', 'w') as file:
                json.dump(self.url_info, file)

    def start_requests(self):
        start_url = "http://skat.dk/skat.aspx?oid=9464"  # year 2018 view url

        yield scrapy.Request(url=start_url, callback=self.go_to_tab_for_most_recent_year)

    def go_to_tab_for_most_recent_year(self, response):

        relative_url_to_most_recent_tab: str = response.xpath('//div[@class="reportTab reportTab-n1"]/a/@href').extract_first()
        absolute_url_to_most_recent_tab: str = response.urljoin(relative_url_to_most_recent_tab)

        yield scrapy.Request(url=absolute_url_to_most_recent_tab, callback=self.parse)

    def parse(self, response):
        # Logging URL
        self.logger.info('%s URL: %s', 'SkatUrlSpider.parse', response.url)

        # get results
        self.parse_result(response)
        # Get next tab after active tab
        next_page = response.xpath(
            '//div[contains(@class, "reportTabActive")]/following-sibling::div[contains(@class, "reportTab")]//a/@href').extract_first()

        # crawl next result page
        if next_page is not None:
            next_page = response.urljoin(next_page)
            yield scrapy.Request(next_page, callback=self.parse)

    def parse_result(self, response):

        # there are many urls on each page. Each of which are collected in this dict of dicts
        rows = response.xpath('//tr[contains(@class, "TableRow TableRowligningsrådet")]')

        for row in rows:
            case_number: str = row.xpath('.//td[@class="bleg report report-r3 text-nowrap"]/text()').extract_first()
            # if no case number skip case
            if case_number is None:
                return None

            url_relative = row.xpath('.//a[contains(@class, "normal")]/@href').extract_first()
            url_abs = response.urljoin(url_relative)

            # add to dict holding all urls
            self.url_info[case_number] = {'url': url_abs}

    def get_date(self, strDate):
        if strDate is not None:
            # Convert str to datetime (ie. 23-12-03)
            try:
                return datetime.strptime(strDate, '%d-%m-%y')
            except ValueError:
                pass

        return None
