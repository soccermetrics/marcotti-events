import os
import glob


class BaseCSV(object):

    def __init__(self, settings):
        self.directory = settings.CSV_DATA_DIR
        self.data_files = settings.CSV_FILES

    def handles(self):
        for data_file in self.data_files:
            for filename in glob.glob(os.path.join(self.directory, data_file)):
                with open(filename, 'r') as fh:
                    yield fh
