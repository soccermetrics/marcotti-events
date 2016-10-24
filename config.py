import os
import json
import logging


def setup_logging(default_path="logging.json", default_level=logging.INFO):
    """Setup logging configuration"""
    path = default_path
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = json.load(f)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


class Config(object):
    """
    Base configuration class.  Contains one method that defines the database URI.

    This class is to be subclassed and its attributes defined therein.
    """

    def __init__(self):
        self.DATABASE_URI = self.database_uri()

    def database_uri(self):
        if getattr(self, 'DIALECT') == 'sqlite':
            uri = r'sqlite://{p.DBNAME}'.format(p=self)
        else:
            uri = r'{p.DIALECT}://{p.DBUSER}:{p.DBPASSWD}@{p.HOSTNAME}:{p.PORT}/{p.DBNAME}'.format(p=self)
        return uri
