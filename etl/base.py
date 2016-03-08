import pandas as pd
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound

from models.common.enums import ConfederationType
from models.common.overview import (Countries, Timezones, Competitions, Seasons, Venues,
                                    DomesticCompetitions, InternationalCompetitions)
from models.common.personnel import Players, Managers, Referees, Positions
from models.club import Clubs, ClubLeagueMatches


class ETL(object):
    """
    Top-level ETL workflow.

    Receive extracted data from XML or CSV sources, transform/validate it, and load it to database.
    """

    def __init__(self, **kwargs):
        self.xml = kwargs.get('xml')
        self.csv = kwargs.get('csv', None)
        self.transformer = MarcottiTransform(kwargs.get('session'))
        self.loader = MarcottiLoad(kwargs.get('session'))

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


class MarcottiTransform(object):
    """
    Transform and validate extracted data.
    """

    def __init__(self, session):
        self.session = session

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

    def competitions(self, data_frame):
        if 'country' in data_frame.columns:
            lambdafunc = lambda x: pd.Series(self.get_id(Countries, name=x['country']))
            id_frame = data_frame.apply(lambdafunc, axis=1)
            id_frame.columns = ['country_id']
        elif 'confederation' in data_frame.columns:
            lambdafunc = lambda x: pd.Series(ConfederationType.from_string(x['confed']))
            id_frame = data_frame.apply(lambdafunc, axis=1)
            id_frame.columns = ['confederation']
        else:
            print "Cannot insert Competition record: No Country or Confederation data present"
        return data_frame.join(id_frame)

    def clubs(self, data_frame):
        if 'country' in data_frame.columns:
            lambdafunc = lambda x: pd.Series(self.get_id(Countries, name=x['country']))
            id_frame = data_frame.apply(lambdafunc, axis=1)
            id_frame.columns = ['country_id']
        else:
            print "Cannot insert Club record: No Country data present"
        return data_frame.join(id_frame)

    def venues(self, data_frame):
        lambdafunc = lambda x: pd.Series([
            self.get_id(Countries, name=x['country']),
            self.get_id(Timezones, name=x['timezone'])
        ])
        ids_frame = data_frame.apply(lambdafunc, axis=1)
        ids_frame.columns = ['country_id', 'timezone_id']
        return data_frame.join(ids_frame)

    def positions(self, data_frame):
        lambdafunc = lambda x: pd.Series(self.get_id(Positions, name=x['position']))
        id_frame = data_frame.apply(lambdafunc, axis=1)
        id_frame.columns = ['position_id']
        return data_frame.join(id_frame)

    def players(self, data_frame):
        lambdafunc = lambda x: pd.Series([
            self.get_id(Countries, name=x['country']),
            self.get_id(Positions, name=x['position'])
        ])
        ids_frame = data_frame.apply(lambdafunc, axis=1)
        ids_frame.columns = ['country_id', 'position_id']
        return data_frame.join(ids_frame)

    def managers(self, data_frame):
        lambdafunc = lambda x: pd.Series(self.get_id(Countries, name=x['country']))
        id_frame = data_frame.apply(lambdafunc, axis=1)
        id_frame.columns = ['country_id']
        return data_frame.join(id_frame)

    def referees(self, data_frame):
        lambdafunc = lambda x: pd.Series(self.get_id(Countries, name=x['country']))
        id_frame = data_frame.apply(lambdafunc, axis=1)
        id_frame.columns = ['country_id']
        return data_frame.join(id_frame)

    def matches(self, data_frame):
        lambdafunc = lambda x: pd.Series([
            self.get_id(Competitions, name=x['competition']),
            self.get_id(Seasons, name=x['season']),
            self.get_id(Venues, name=x['venue']),
            self.get_id(Clubs, name=x['home_team']),
            self.get_id(Clubs, name=x['away_team']),
            self.get_id(Managers, full_name=x['home_manager']),
            self.get_id(Managers, full_name=x['away_manager']),
            self.get_id(Referees, full_name=x['referee'])
        ])
        ids_frame = data_frame.apply(lambdafunc, axis=1)
        ids_frame.columns = ['competition_id', 'season_id', 'venue_id', 'home_team_id', 'away_team_id',
                             'home_manager_id', 'away_manager_id', 'referee_id']
        return data_frame.join(ids_frame)

    def lineups(self, data_frame):
        lambdafunc = lambda x: pd.Series([
            self.get_id(ClubLeagueMatches,
                        competition_id=self.get_id(Competitions, name=x['competition']),
                        season_id=self.get_id(Seasons, name=x['season']),
                        matchday=x['matchday'],
                        home_team_id=self.get_id(Clubs, name=x['home_team']),
                        away_team_id=self.get_id(Clubs, name=x['away_team'])),
            self.get_id(Clubs, name=x['player_team']),
            self.get_id(Players, full_name=x['player_name'])
        ])
        ids_frame = data_frame.apply(lambdafunc, axis=1)
        ids_frame.columns = ['match_id', 'team_id', 'player_id']
        return data_frame.join(ids_frame)

    def events(self, data_frame):
        pass


class MarcottiLoad(object):
    """
    Load transformed data into database.
    """

    def __init__(self, session):
        self.session = session

    def record_exists(self, model, **conditions):
        return self.session.query(model).filter_by(**conditions).count() != 0

    def competitions(self, data_frame):
        comp_records = []
        for row in data_frame.iterrows():
            if 'country' in data_frame.columns:
                comp_dict = dict(name=row['name'], level=row['level'], country_id=row['country_id'])
                if not self.record_exists(DomesticCompetitions, **comp_dict):
                    comp_records.append(DomesticCompetitions(**comp_dict))
                else:
                    print "Cannot insert Domestic Competition record: Record exists"
            elif 'confederation' in data_frame.columns:
                comp_dict = dict(name=row['name'], level=row['level'], confederation=row['confederation'])
                if not self.record_exists(InternationalCompetitions, **comp_dict):
                    comp_records.append(InternationalCompetitions(**comp_dict))
                else:
                    print "Cannot insert International Competition record: Record exists"
        self.session.add_all(comp_records)
        self.session.commit()

    def clubs(self, data_frame):
        club_records = []
        for row in data_frame.iterrows():
            club_dict = dict(name=row['name'], country_id=row['country_id'])
            if not self.record_exists(Clubs, **club_dict):
                club_records.append(Clubs(**club_dict))
            else:
                print "Cannot insert Club record: Record exists"
        self.session.add_all(club_records)
        self.session.commit()

    def venues(self):
        pass

    def positions(self):
        pass

    def players(self):
        pass

    def managers(self):
        pass

    def referees(self):
        pass

    def matches(self):
        pass

    def lineups(self):
        pass

    def events(self):
        pass
