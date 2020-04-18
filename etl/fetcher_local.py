from etl.fetcher_response import FetcherResponse
from etl.payload_data import PayloadData
from io import BytesIO
import os
from posixpath import basename
import logging
from etl.progress_bar_manager import ProgressBarManager

# local testing replacement for Fetcher
# load a subset of data from local files instead
# of downloading
class FetcherLocal:

    # Initializer / Instance Attributes
    def __init__(self):
        self.pbar_manager = ProgressBarManager()
        self.logger = logging.getLogger(__name__)

    def fetch_all(self, filenames):
        # Setup progress bar
        self.job_count = len(filenames)
        self.pbar = self.pbar_manager.add_pbar(self.job_count, __name__, 'files')

        fetchedFiles = []

        for filename in filenames:
            self.logger.debug("Fetching local file: %s...", filename)
            file = self.fetch(filename)
            fetchedFiles.append(file)
            # update progress bar
            self.pbar.update()
        # Close progress bar
        self.pbar.close()
        return fetchedFiles

    def fetch(self, filename):
        file = open(filename, 'rb')
        baseFilename = basename(filename)
        fetchResponse = FetcherResponse(
            baseFilename,
            [
                PayloadData(baseFilename, BytesIO(file.read()))
            ],
            filename,
            None
        )
        file.close()
        return fetchResponse
