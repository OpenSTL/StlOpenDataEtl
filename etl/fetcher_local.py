from etl.fetcher_response import FetcherResponse
from etl.payload_data import PayloadData
from io import BytesIO
import os
from posixpath import basename
import constants
import logging

# local testing replacement for Fetcher
# load a subset of data from local files instead
# of downloading
class FetcherLocal:

    # Initializer / Instance Attributes
    def __init__(self, pbar,logger):
        self.pbar = pbar
        self.logger = logger
        # self.logger = logging.getLogger(__name__)

    def fetch_all(self, filenames):
        # set progress bar increment by passing in # of files to be fetched
        self.pbar.set_increment(len(filenames))
        fetchedFiles = []

        for filename in filenames:
            self.logger.debug("Fetching local file: %s", filename)
            file = self.fetch(filename)
            fetchedFiles.append(file)
            # update progress bar
            self.pbar.update()
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
