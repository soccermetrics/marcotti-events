import re
from contextlib import contextmanager

from sqlalchemy.engine import create_engine
from sqlalchemy.orm.session import Session

from etl.ecsv import CSVExtractor
from etl import ETL, MarcottiTransform, MarcottiLoad


class Marcotti(object):

    def __init__(self, config):
        self.settings = config
        self.engine = create_engine(config.DATABASE_URI)
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
        print "Creating data models"
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
                print "Loading {}".format(entity.capitalize())
                data = getattr(csv_obj, entity)(self.settings.START_YEAR, self.settings.END_YEAR)
                etl.workflow(entity, data)

            csv_validation = CSVExtractor('data')
            for entity in ['countries', 'modifiers', 'positions', 'surfaces', 'timezones']:
                print "Loading {}".format(entity.capitalize())
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
        try:
            yield session
            session.commit()
        except Exception as ex:
            session.rollback()
            raise ex
        finally:
            session.close()


if __name__ == "__main__":
    from models.club import ClubSchema
    from local import LocalConfig

    marcotti = Marcotti(LocalConfig())
    marcotti.create_db(ClubSchema)
    marcotti.initial_load(lang='es')
