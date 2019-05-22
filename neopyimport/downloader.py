import os
import tempfile
from dataclasses import dataclass
from enum import Enum
from tarfile import TarFile
from typing import List, Union
from zipfile import ZipFile

import requests
from pandas import DataFrame, read_csv


class DATATYPES(Enum):
    CSV = 0
    ZIP = 1
    TAR = 2


class DATASOURCE(Enum):
    FS = 0
    ONLINE = 1


@dataclass
class downloader:
    """A helper that ease the dataset retrival from the web
    
        Arguments:
            path {str} -- The dataset URI, or location in a file system
            dtype: {DATATYPE} -- The type of data to retrieve (default: {DATATYPES.ZIP})
            source: {DATASOURCE} -- The source type of data (default: {DATASOURCE.ONLINE})
    """

    path: str
    dtype: DATATYPES = DATATYPES.ZIP
    source: DATASOURCE = DATASOURCE.ONLINE

    def _download(self) -> str:
        """Downloads the data from the online source
        
        Returns:
            str -- The name of the downloaded file
        """
        downloaded_file = requests.get(self.path)
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(downloaded_file.content)
            return f.name

    def load_dataframes(self, **pandas_args) -> Union[DataFrame, List[DataFrame]]:
        """Depending on the datasource and type, retrieve the data from source and return the loaded pd.DataFrame or list or pd.DataFrame.
        
        Returns:
            Union[str, List[str]] -- pd.DataFrame or list or pd.DataFrame depending of the input data type.
        """
        if self.source == DATASOURCE.ONLINE:
            self.path = self._download()
        tmpdir = tempfile.mkdtemp()

        if self.dtype == DATATYPES.CSV:
            return read_csv(self.path, **pandas_args)
        elif self.dtype == DATATYPES.ZIP:
            ZipFile(self.path).extractall(tmpdir)
        elif self.dtype == DATATYPES.TAR:
            TarFile.open(self.path).extractall(tmpdir)

        data_files = (
            os.path.join(root, filename)
            for root, directories, filenames in os.walk(tmpdir)
            for filename in filenames
        )

        return [read_csv(data_file, **pandas_args) for data_file in data_files]
