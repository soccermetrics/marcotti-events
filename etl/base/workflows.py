import pandas as pd
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound

from models.common.suppliers import Suppliers


class ETL(object):
    """
    Top-level ETL workflow.

    Receive extracted data from XML or CSV sources, transform/validate it, and load it to database.
    """

    def __init__(self, **kwargs):
        self.xml = kwargs.get('xml')(kwargs.get('settings'))
        self.csv = kwargs.get('csv', None)
        self.transformer = kwargs.get('transform')(kwargs.get('session'), self.xml.supplier)
        self.loader = kwargs.get('load')(kwargs.get('session'), self.xml.supplier)

    def workflow(self, entity):
        """
        Implement ETL workflow for a specific data entity:

        1. Extract and combine data from XML and CSV sources (if applicable).
        2. Transform and validate combined data into IDs and enums in the Marcotti database.
        3. Load transformed data into the database if it is not already there.

        :param entity: Data model name
        """
        getattr(self.loader, entity)(getattr(self.transformer, entity)(self.combiner(entity)))

    def combiner(self, entity):
        """
        Combine data from XML and supplemental CSV sources using unique ID of XML records.

        Returns a Pandas DataFrame of the combined data.

        :param entity: Data model name
        :return: DataFrame of combined data.
        """
        xml_document = self.xml.extract()
        main_data = pd.DataFrame(getattr(self.xml, entity)(xml_document))
        if self.csv is not None:
            supplemental_data = pd.DataFrame(getattr(self.csv, entity)())
            return pd.merge(main_data, supplemental_data, on=['remote_id'])
        return main_data


class WorkflowBase(object):

    def __init__(self, session, supplier):
        self.session = session
        self.supplier_id = self.get_id(Suppliers, name=supplier)

    def get_id(self, model, **conditions):
        try:
            record_id = self.session.query(model).filter_by(**conditions).one().id
        except NoResultFound as ex:
            print "{} has no records in Marcotti database for: {}".format(model.__name__, conditions)
            raise ex
        except MultipleResultsFound as ex:
            print "{} has multiple records in Marcotti database for: {}".format(model.__name__, conditions)
            raise ex
        return record_id
