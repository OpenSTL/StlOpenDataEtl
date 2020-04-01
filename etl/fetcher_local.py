from etl.fetcher_response import FetcherResponse
from etl.payload_data import PayloadData
from io import BytesIO
import os
from posixpath import basename

# local testing replacement for Fetcher
# load a subset of data from local files instead
# of downloading
class FetcherLocal:

    def fetch_all(self, filenames):
        fetchedFiles = []

        for filename in filenames:
            file = self.fetch(filename)
            fetchedFiles.append(file)

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

