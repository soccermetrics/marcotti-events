import re
import sys
import logging
from contextlib import contextmanager

from sqlalchemy.engine import create_engine
from sqlalchemy.orm.session import Session

from .version import __version__
from etl.ecsv import CSVExtractor
from etl import ETL, MarcottiTransform, MarcottiLoad


logger = logging.getLogger(__name__)


class Marcotti(object):

    def __init__(self, config):
        logger.info("Marcotti-MLS v{0}: Python {1} on {2}".format(
            '.'.join(__version__), sys.version, sys.platform))
        logger.info("Opened connection to {0}".format(self._public_db_uri(config.database_uri)))
        self.settings = config
        self.engine = create_engine(config.database_uri)
        self.connection = self.engine.connect()

    @staticmethod
    def _public_db_uri(uri):
        """
        Strip out database username/password from database URI.

        :param uri: Database URI string.
        :return: Database URI with username/password removed.
        """
        return re.sub(r"//.*@", "//", uri)

    def create_db(self, base):
        logger.info("Creating data models")
        base.metadata.create_all(self.connection)

    def initial_load(self, lang=None):
        """
        Load validation data into database.

        :param lang: Language of country names to be loaded ('es' for Spanish or None for English)
        """
        with self.create_session() as sess:
            etl = ETL(transform=MarcottiTransform, load=MarcottiLoad, session=sess)
            csv_obj = CSVExtractor(None)
            for entity in ['years', 'seasons']:
                logger.info("Loading {}".format(entity.capitalize()))
                data = getattr(csv_obj, entity)(self.settings.START_YEAR, self.settings.END_YEAR)
                etl.workflow(entity, data)

            csv_validation = CSVExtractor('data')
            for entity in ['countries', 'modifiers', 'positions', 'surfaces', 'timezones']:
                logger.info("Loading {}".format(entity.capitalize()))
                if entity == 'countries':
                    lang_element = [lang] if lang else []
                    data_file = '{}.csv'.format('-'.join([entity]+lang_element))
                else:
                    data_file = '{}.csv'.format(entity)
                data = getattr(csv_validation, entity)(data_file)
                etl.workflow(entity, data)

    @contextmanager
    def create_session(self):
        session = Session(self.connection)
        logger.info("Create session {0} with {1}".format(
            id(session), self._public_db_uri(str(self.engine.url))))
        try:
            yield session
            session.commit()
            logger.info("Committing remaining transactions to database")
        except Exception as ex:
            session.rollback()
            logger.error("Database transactions rolled back")
            raise ex
        finally:
            logger.info("Session {0} with {1} closed".format(
                id(session), self._public_db_uri(str(self.engine.url))))
            session.close()


class MarcottiConfig(object):
    """
    Base configuration class for Marcotti-Events.  Contains one method that defines the database URI.

    This class is to be subclassed and its attributes defined therein.
    """

    @property
    def database_uri(self):
        if getattr(self, 'DIALECT') == 'sqlite':
            uri = r'sqlite://{p.DBNAME}'.format(p=self)
        else:
            uri = r'{p.DIALECT}://{p.DBUSER}:{p.DBPASSWD}@{p.HOSTNAME}:{p.PORT}/{p.DBNAME}'.format(p=self)
        return uri
