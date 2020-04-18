from etl.utils import decompress, get_file_ext
import zipfile
import logging
from etl.constants import *
from etl.progress_bar_manager import ProgressBarManager


class Parser:
    # Initializer / Instance Attributes
    def __init__(self):
        self.pbar_manager = ProgressBarManager()
        self.logger = logging.getLogger(__name__)

    def parse_all(self, responses):
        # Setup progress bar
        self.job_count = len(responses)
        self.pbar = self.pbar_manager.add_pbar(self.job_count, __name__, 'files')
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
