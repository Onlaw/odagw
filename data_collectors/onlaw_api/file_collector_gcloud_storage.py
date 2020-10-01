import os
import asyncio
import aiohttp
from gcloud.aio.storage import Storage
from typing import List, Dict
import logging
ListOfFiles = List[Dict]


class GcloudStorageFileCollector:

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.debug('instantiating FileCollector')
        os_env_vars = os.environ
        try:
            self.bucket = os_env_vars['GCLOUD_STORAGE_BUCKET_PRIVATE_NAME']
            self.project = os_env_vars['GCLOUD_PROJECT_ID']
            self.credentials_file = os_env_vars['GCLOUD_STORAGE_CREDENTIALS']
        except KeyError as e:
            raise KeyError('required env var: "{}" not set'.format(e.args[0]))
        self.storage_object = None
        self.logger.debug('Done instantiating FileCollector')

    def _instantiate_storage_object(self):
        self.storage_object = Storage(service_file=self.credentials_file)

    async def collect_file(self, file_name: str, file_id: str,
                           session: aiohttp.ClientSession, timeout=1000) -> tuple:
        if not self.storage_object:
            self._instantiate_storage_object()

        metadata_task = \
            asyncio.create_task(self.storage_object.download_metadata(self.bucket, file_name,
                                                                      session=session,
                                                                      timeout=timeout))
        data_task = asyncio.create_task(self.storage_object.download(self.bucket, file_name, session=session, timeout=timeout))

        metadata = await metadata_task
        data_bytes = await data_task

        content_type = metadata['contentType']

        if content_type in ('text/html', 'text/html; charset=utf-8', 'text/plain; charset=utf-8', 'text/plain'):
            content = data_bytes.decode('utf-8')
        elif content_type in ('application/octet-stream', 'application/pdf'):
            content = data_bytes
        else:
            err_str = 'Do not know how to handle contentType: "{}"'.format(content_type)
            raise TypeError(err_str)

        return (file_id, content)
