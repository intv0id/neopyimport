import tempfile, os, requests
from zipfile import ZipFile 
from tarfile import TarFile
from enum import Enum
from pandas import read_csv
from dataclasses import dataclass

class DATATYPES(Enum):
    CSV=0
    ZIP=1
    TAR=2

class DATASOURCE(Enum):
    FS=0
    ONLINE=1

@dataclass
class downloader():
    path: str
    dtype: DATATYPES = DATATYPES.ZIP
    source: DATASOURCE = DATASOURCE.ONLINE 
    
    def _download(self):
        downloaded_file = requests.get(self.path)
        with tempfile.NamedTemporaryFile() as f:  
            f.write(downloaded_file.content)
        return tempfile.name

    def load_dataframes(self, **pandas_args):
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
            os.path.join(root,filename) 
            for root, directories, filenames in os.walk(tmpdir)
            for filename in filenames 
        ) 

        return [read_csv(data_file, **pandas_args) for data_file in data_files]


