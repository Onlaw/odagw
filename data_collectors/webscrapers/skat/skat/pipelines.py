from datetime import datetime, timezone
import json
import lxml.html
import lxml.etree


class SkatPipeline:
    def process_item(self, item, spider):
        content: str = self.strip_html(item['body'])

        filename: str = f'skat_{item["SKM-nummer"]}'
        with open(f'{spider.data_folder}/{filename}', 'w') as fp:
            fp.write(content)

        metadata: dict = {
            'doc_id': item['SKM-nummer'],
            'uri': item['url'],
            'date_built': datetime.now(timezone.utc).isoformat()
        }

        with open(spider.urls_already_scraped_file_path, 'a') as fp:
            fp.write(f'{json.dumps(metadata)}\n')

        collected_tokens: int = len(content.split())
        spider.collected_tokens += collected_tokens
        spider.logger.info(f'collected { item["SKM-nummer"]} which has #{collected_tokens} tokens.')
        spider.logger.info(f'Collected #{spider.collected_tokens} tokens in total')
        return item

    def strip_html(self, html: str) -> str:
        parsed_html = lxml.html.fromstring(html)

        for table in parsed_html.xpath('//table'):
            table.getparent().remove(table)

        all_content_no_html = ' '.join(lxml.etree.XPath("//text()")(parsed_html))

        content_no_extra_whitespaces = ' '.join(all_content_no_html.split())

        return content_no_extra_whitespaces
