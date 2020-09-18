from typing import List, Optional
import logging
from datetime import datetime
import json
import asyncio
import lxml.html
import sys
import os
this_file_path = os.path.dirname(os.path.abspath(__file__))  # get directory of this file
sys.path.append(this_file_path + '/..')
from data_collectors import prisma_collector
from data_collectors import file_collector_gcloud_storage

logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s : %(message)s')
streamHandler = logging.StreamHandler()
streamHandler.setFormatter(formatter)
logger.addHandler(streamHandler)


async def main():
    await build_skat()


async def build_skat():
    query_filters: List[str] = ['url_contains: "skat"']
    await build(query_filters, 'verdict')


async def build(query_filters: List[str], doc_type: str, limit: Optional[int] = None):
    prisma_endpoint = 'http://localhost:4467'
    file_collector = file_collector_gcloud_storage.GcloudStorageFileCollector()
    prisma_collector_ = prisma_collector.PrismaDocumentCollector(prisma_endpoint, file_collector)

    query: str = """
        contentFilesOriginal { id, url, name }
        documentType
        permalink
        resume
        resumeTitle
        uid
        url
        uniqueIdentifiers { uidType, value }
        """

    await get_verdict(prisma_collector_, query, query_filters, 'skat')


async def get_verdict(prisma_collector_: prisma_collector,
                      query: str,
                      query_filters: List[str],
                      base_save_name: str,
                      batch_size=1000, limit: Optional[int] = None):

    data_path: str = os.path.abspath(os.path.join(this_file_path + '/..', 'data'))
    logger.info(f'saving data to: {data_path}')
    if not os.path.exists(data_path):
        os.makedirs(data_path)
    limit = 3
    count_verdicts: int = await prisma_collector_.count_verdicts()
    if limit:
        max_documents = limit
    else:
        max_documents = count_verdicts
    logger.info('starting to get')
    batch_size = 1
    for i in range(0, max_documents, batch_size):
        metadatas: List[dict] = []
        async for document in prisma_collector_.documents(query=query, document_type='verdict',
                                                          offset=i, query_filters=query_filters, limit=min(batch_size, max_documents)):

            doc_id: str = f'{base_save_name}_{i}'
            metadata: dict = {'doc_id': doc_id,
                              'uri': document['url'],
                              'date_built': get_iso_formated_datetime_string_utc()
                              }
            metadatas.append(metadata)

            content: str = strip_html(document['content'])
            with open(f'{data_path}/{doc_id}', 'w') as fp:
                fp.write(content)
            # TODO add metadatas

        with open(f'{data_path}/data.jsonl', 'a') as fp:
            for metadata in metadatas:
                fp.write(f'{json.dumps(metadata)}\n')


def strip_html(html: str) -> str:
    parsed_html = lxml.html.fromstring(html)

    for table in parsed_html.xpath('//table'):
        table.getparent().remove(table)

    # for paragraph in parsed_html.xpath('//p'):
    #     paragraph_text = f'{paragraph.text_content()}{paragraph.tail}'.strip()
    #     breakpoint()
    #     if not paragraph_text.endswith('.'):
    #         paragraph.tail = paragraph.tail + '. '

    all_content_no_html = ' '.join(lxml.etree.XPath("//text()")(parsed_html))

    content_no_extra_whitespaces = ' '.join(all_content_no_html.split())

    return content_no_extra_whitespaces


def document2sentences(self, document: str) -> List[str]:
    return [paragraph_dict['token'] for paragraph_dict in self.sentence_tokenizer.get_sentence_spans(document)]


def get_iso_formated_datetime_string_utc() -> str:
    now = datetime.utcnow()
    return now.isoformat()


if __name__ == '__main__':
    asyncio.run(main())
