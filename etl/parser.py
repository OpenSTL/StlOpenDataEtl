from etl.utils import decompress, get_file_ext
import zipfile
import logging
from etl.constants import *


class Parser:
    # Initializer / Instance Attributes
    def __init__(self, pbar_manager):
        self.pbar_manager = pbar_manager
        self.logger = logging.getLogger(__name__)

    def parse_all(self, responses):
        # Setup Extract stage progress bar
        self.job_count = len(responses)
        self.pbar = self.pbar_manager.counter(total=self.job_count, desc=__name__, unit='files')
        for response in responses:
            try:
                self.logger.debug('Parsing file: %s...', response.name)
                response.payload = self.flatten(response, SUPPORTED_FILE_EXT)
            except Exception as err:
                self.logger.error(err)
            # update progress bar
            self.pbar.update()
        # close progress bar
        self.pbar.close()
        return responses

    def flatten(self, response, extensions=None):
        for payload in response.payload:
            if zipfile.is_zipfile(payload.data):
                response.payload = decompress(
                    payload.data, extensions)
                self.flatten(response)
        return response.payload
