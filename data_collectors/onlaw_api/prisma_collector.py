from typing import AsyncGenerator, List, Optional
from datetime import datetime
from dateutil import parser
import asyncio
import aiohttp
import jwt
from prisma_helpers import prisma_helpers

import sys
import os
this_file_path = os.path.dirname(os.path.abspath(__file__))  # get directory of this file
sys.path.append(this_file_path + '/..')
from data_collectors import graphql_connection

import logging


class PrismaDocumentCollector:

    def __init__(self, endpoint: str, file_collector,
                 token: str = None, tcp_connections=110, concurrent_files_collected=300):
        """query_filter: determines which laws are collected. e.g. only non-historic.."""
        self.logger = logging.getLogger(__name__)
        self.logger.debug('starting PrismaDocumentCollector constructor')
        self.endpoint: str = endpoint
        self.file_collector = file_collector
        self.tcp_connections = tcp_connections
        self.concurrent_files_collected = concurrent_files_collected
        if token is None:
            self.logger.info('prisma token not set. aquiring token...')
            self.token = self.get_prisma_token()
            self.logger.info('Token aquired.')
        else:
            self.token = token

        self.session: aiohttp.ClientSession = None
        self.logger.debug('Done instantiating PrismaDocumentCollector')

    async def count_laws(self) -> int:
        return await self._count_record_for_type('law')

    async def count_verdicts(self, query_filter: str = '') -> int:
        return await self._count_record_for_type('verdict')

    async def _count_record_for_type(self, document_type: str, query_filter: str = '') -> int:
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=self.tcp_connections)) as session:
            graphql_connection_ = graphql_connection.GraphQLConnection(session, self.endpoint, self.token)

            return await prisma_helpers.count_of_type(graphql_connection_,
                                                      gql_type=document_type,
                                                      query_filter=query_filter)

    async def get_date_of_latest_document(self) -> datetime:
        date_of_latest_law_update_str = await self._get_date_of_latest_document_from_prisma()
        date_of_latest_law_update_utc = parser.isoparse(date_of_latest_law_update_str)  # noqa T484

        return date_of_latest_law_update_utc

    async def documents(self, *, query: str,
                        document_type: str,
                        offset: int,
                        limit: int = None,
                        metadata_only: bool = False,
                        query_filters: Optional[List[str]] = None,
                        uids: List[str] = None) -> AsyncGenerator:
        """async generator. see e.g. https://www.python.org/dev/peps/pep-0525/"""
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=self.tcp_connections)) as session:
            document_metadata: List[dict] = await self._collect_document_metadata(session, query, document_type,
                                                                                  offset=offset, limit=limit,
                                                                                  query_filters=query_filters, uids=uids)
            if metadata_only:
                for document in document_metadata:
                    yield document
            else:
                for count_files in range(0, len(document_metadata), self.concurrent_files_collected):
                    coros = [self._get_content_files(metadata, session) for metadata in document_metadata[count_files:count_files + self.concurrent_files_collected]]
                    for document_awaitable in asyncio.as_completed(coros):
                        yield await document_awaitable
        pass

    async def _get_content_files(self, metadata, session) -> dict:
        contentFile_metadata = metadata['contentFilesOriginal'][0]
        content = await self.file_collector.collect_file(contentFile_metadata['name'], contentFile_metadata['id'], session)

        metadata['content'] = content[1]

        return metadata

    # TODO return typed dict or dataclass!!!
    async def _collect_document_metadata(self, session, query: str,
                                         document_type: str,
                                         limit: int = None,
                                         offset: int = None,
                                         query_filters: Optional[List[str]] = None,
                                         uids: List[str] = None) -> List[dict]:

        query_filter: str = self._add_uids_to_query_filter(uids)

        if query_filters:
            query_filter += self.query_filters2query_filter_string(query_filters)

        graphql_connection_ = graphql_connection.GraphQLConnection(session, self.endpoint, self.token)
        document_metadata = await prisma_helpers.all_of_type(graphql_connection_,
                                                             gql_type=document_type,
                                                             limit=limit,
                                                             offset=offset,
                                                             query_filter=query_filter,
                                                             query_str=query)

        self.logger.info(f'collected metadata for #{len(document_metadata)} {document_type}, offset is {offset}')

        return document_metadata

    def _add_uids_to_query_filter(self, uids: List[str] = None) -> str:
        query_filter: str = ''
        if uids:
            query_filter += ', uid_in : [ '
            for uid in uids:
                query_filter += f'"{uid}",'
            query_filter += '], '

        return query_filter

    @classmethod
    def query_filters2query_filter_string(cls, query_filters: Optional[List[str]]) -> str:
        if query_filters is None:
            return ''
        return ', '.join(query_filters)

    async def _get_date_of_latest_document_from_prisma(self, query_filters: List[str] = None) -> str:
        query_str = 'updatedAt'

        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=self.tcp_connections)) as session:
            graphql_connection_ = graphql_connection.GraphQLConnection(session, self.endpoint, self.token)
            date_of_latest_law_update_response = await prisma_helpers.all_of_type(graphql_connection_,
                                                                                  gql_type='law', limit=1,
                                                                                  query_filter=self.query_filters2query_filter_string(query_filters),
                                                                                  query_str=query_str)

        try:
            date_of_latest_law_update_str = date_of_latest_law_update_response[0]['updatedAt']
        except IndexError:
            self.logger.error('no laws found:\n{}'.format(date_of_latest_law_update_response))
            raise

        return date_of_latest_law_update_str

    @staticmethod
    def get_prisma_token(PRISMA_SECRET: str = None):
        if PRISMA_SECRET is None:
            PRISMA_SECRET = os.environ.get('PRISMA_SECRET', '')

        if not PRISMA_SECRET:
            err_str = 'Environment variable "PRISMA_SECRET" not found. To set do e.g.: export PRISMA_SECRET=<your prisma secret>\n....exiting\n'
            raise KeyError(err_str)

        prisma_token = jwt.encode({}, PRISMA_SECRET, algorithm='HS256').decode('utf-8')

        return prisma_token
