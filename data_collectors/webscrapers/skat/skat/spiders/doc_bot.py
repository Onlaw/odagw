from typing import Set
import os
import json
import lxml.html
import scrapy
this_file_path = os.path.dirname(os.path.abspath(__file__))


class SkatDocSpider(scrapy.Spider):
    name = 'SkatDocSpider'
    custom_settings = {
        'ITEM_PIPELINES': {
            'skat.pipelines.SkatPipeline': 100
        }
    }

    def __init__(self):
        self.collected_tokens: int = 0
        self.data_folder: str = os.path.abspath(os.path.join(this_file_path, '../../data'))
        self.url_file_path: str = os.path.join(self.data_folder, 'urls.json')
        self.urls_already_scraped_file_path: str = os.path.join(self.data_folder, 'data.jsonl')
        self.urls_to_scrape: Set[str]
        self.urls_already_scraped: Set[str]
        breakpoint()
        if os.path.exists(self.urls_already_scraped_file_path):
            with open(self.urls_already_scraped_file_path, 'r') as fp:
                urls_already_scraped_info = fp.read()

            self.urls_already_scraped = {json.loads(line)['uri'] for line in urls_already_scraped_info.splitlines()}

            # TODO check tha data files are actually there!!!
        else:
            self.urls_already_scraped = set(())
            self.logger.info(f'No already scraped urls file found at {self.urls_already_scraped_file_path}. Scraping all urls in {self.url_file_path}')

        self.urls_to_scrape: Set[str] = self.load_urls()
        # TODO load urls already scraped-> set difference

    def load_urls(self):

        self.logger.info(f'urls are read from: "{self.url_file_path}"')
        # read download file from local disk
        with open(self.url_file_path, 'r') as file_fp:
            url_dict = json.load(file_fp)

        self.urls_in_url_file: Set[str] = {info['url']for uid, info in url_dict.items()}
        self.logger.info(f'loaded #{len(self.urls_in_url_file)} to scrape')

        urls_to_scrape: Set[str] = self.urls_in_url_file - self.urls_already_scraped

        return urls_to_scrape

    def start_requests(self, headers: dict = None):

        self.logger.info(f'Starting to scrape #{len(self.urls_to_scrape)} pages')
        for url in self.urls_to_scrape:
            request_args: dict = {'url': url, 'callback': self.parse}

            self.logger.info(f'starting to scrape this document\n{50*"-"}\n{url}\n{50*"-"}\n')
            yield scrapy.Request(**request_args)

    def parse(self, response):
        content: list = response.xpath('//div[@class="MPtext"]').extract()[0].split('<hr class="LineDelimiter">')

        data = self.extract_elements(response, content)

        body: str = ''
        if 'Redaktionelle noter' in data:
            body = '<h2>Redaktionelle noter</h2>' + \
                data['Redaktionelle noter'] + '<hr class="LineDelimiter">'

        resume: str = ''
        if 'Resumé' in data:
            resume += data['Resumé'].replace('\n', '<br>')

        if len(content) > 1:
            # Added missing <div>
            body += '<div id="MPtext">' + content[1]

        data['body'] = f'{resume}.\n{body}'

        data['url'] = response.url

        return data

    def extract_elements(self, response, content):
        meta_data_html = content[0] + '</div>'
        meta_data_doc = lxml.html.fromstring(meta_data_html)

        meta_data = {}
        for tbl in meta_data_doc.xpath('//table'):
            elements = tbl.xpath('.//tr/td//text()')
            if len(elements) >= 3:
                meta_data[elements[1]] = ' '.join(elements[2:])

        meta_data['name'] = response.xpath(
            "//meta[@name='name']/@content")[0].extract()

        return meta_data
    # TODO pipeline saving to disk
