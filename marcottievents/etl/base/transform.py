import pandas as pd

from marcottievents.models.common.enums import (ConfederationType, ActionType, ModifierType,
                                                ModifierCategoryType, NameOrderType, PositionType,
                                                GroupRoundType, KnockoutRoundType, SurfaceType)
from marcottievents.models.common.suppliers import (MatchEventMap, MatchMap, CompetitionMap,
                                                    VenueMap, PositionMap, PlayerMap, ManagerMap,
                                                    RefereeMap)
from marcottievents.models.common.overview import Countries, Timezones, Competitions, Seasons, Venues, Surfaces
from marcottievents.models.common.personnel import Players, Managers, Referees
from marcottievents.models.club import Clubs, ClubLeagueMatches, ClubMap
from .workflows import WorkflowBase


class MarcottiTransform(WorkflowBase):
    """
    Transform and validate extracted data.
    """

    @staticmethod
    def suppliers(data_frame):
        return data_frame

    @staticmethod
    def years(data_frame):
        return data_frame

    @staticmethod
    def seasons(data_frame):
        return data_frame

    def competitions(self, data_frame):
        if 'country' in data_frame.columns:
            transformed_field = 'country'
            lambdafunc = lambda x: pd.Series(self.get_id(Countries, name=x[transformed_field]))
            id_frame = data_frame.apply(lambdafunc, axis=1)
            id_frame.columns = ['country_id']
        elif 'confed' in data_frame.columns:
            transformed_field = 'confed'
            lambdafunc = lambda x: pd.Series(ConfederationType.from_string(x[transformed_field]))
            id_frame = data_frame.apply(lambdafunc, axis=1)
            id_frame.columns = ['confederation']
        else:
            raise KeyError("Cannot insert Competition record: No Country or Confederation data present")
        return data_frame.join(id_frame).drop(transformed_field, axis=1)

    def countries(self, data_frame):
        lambdafunc = lambda x: pd.Series(ConfederationType.from_string(x['confed']))
        id_frame = data_frame.apply(lambdafunc, axis=1)
        id_frame.columns = ['confederation']
        joined_frame = data_frame.join(id_frame).drop('confed', axis=1)
        return joined_frame

    def clubs(self, data_frame):
        if 'country' in data_frame.columns:
            lambdafunc = lambda x: pd.Series(self.get_id(Countries, name=x['country']))
            id_frame = data_frame.apply(lambdafunc, axis=1)
            id_frame.columns = ['country_id']
        else:
            raise KeyError("Cannot insert Club record: No Country data present")
        return data_frame.join(id_frame)

    def venues(self, data_frame):
        lambdafunc = lambda x: pd.Series([
            self.get_id(Countries, name=x['country']),
            self.get_id(Timezones, name=x['timezone']),
            self.get_id(Surfaces, description=x['surface']),
            self.make_date_object(x['config_date'])
        ])
        ids_frame = data_frame.apply(lambdafunc, axis=1)
        ids_frame.columns = ['country_id', 'timezone_id', 'surface_id', 'eff_date']
        joined_frame = data_frame.join(ids_frame).drop(['country', 'timezone', 'surface', 'config_date'], axis=1)
        new_frame = joined_frame.where((pd.notnull(joined_frame)), None)
        return new_frame

    def timezones(self, data_frame):
        lambdafunc = lambda x: pd.Series(ConfederationType.from_string(x['confed']))
        id_frame = data_frame.apply(lambdafunc, axis=1)
        id_frame.columns = ['confederation']
        joined_frame = data_frame.join(id_frame).drop('confed', axis=1)
        return joined_frame

    def positions(self, data_frame):
        lambdafunc = lambda x: pd.Series(PositionType.from_string(x['position_type']))
        id_frame = data_frame.apply(lambdafunc, axis=1)
        id_frame.columns = ['type']
        joined_frame = data_frame.join(id_frame).drop('position_type', axis=1)
        return joined_frame

    def surfaces(self, data_frame):
        lambdafunc = lambda x: pd.Series(SurfaceType.from_string(x['surface_type']))
        id_frame = data_frame.apply(lambdafunc, axis=1)
        id_frame.columns = ['type']
        joined_frame = data_frame.join(id_frame).drop('surface_type', axis=1)
        return joined_frame

    def players(self, data_frame):
        lambdafunc = lambda x: pd.Series([
            self.make_date_object(x['dob']),
            NameOrderType.from_string(x['name_order'] or 'Western'),
            self.get_id(Countries, name=x['country']),
            self.get_id(PositionMap, remote_id=x['remote_position_id'], supplier_id=self.supplier_id)
        ])
        ids_frame = data_frame.apply(lambdafunc, axis=1)
        ids_frame.columns = ['birth_date', 'order', 'country_id', 'position_id']
        joined_frame = data_frame.join(ids_frame).drop(
            ['dob', 'name_order', 'country', 'remote_position_id'], axis=1)
        return joined_frame

    def managers(self, data_frame):
        lambdafunc = lambda x: pd.Series([
            self.make_date_object(x['dob']),
            NameOrderType.from_string(x['name_order'] or 'Western'),
            self.get_id(Countries, name=x['country'])
        ])
        ids_frame = data_frame.apply(lambdafunc, axis=1)
        ids_frame.columns = ['birth_date', 'order', 'country_id']
        joined_frame = data_frame.join(ids_frame).drop(['dob', 'name_order', 'country'], axis=1)
        return joined_frame

    def referees(self, data_frame):
        lambdafunc = lambda x: pd.Series([
            self.make_date_object(x['dob']),
            NameOrderType.from_string(x['name_order'] or 'Western'),
            self.get_id(Countries, name=x['country'])
        ])
        ids_frame = data_frame.apply(lambdafunc, axis=1)
        ids_frame.columns = ['birth_date', 'order', 'country_id']
        joined_frame = data_frame.join(ids_frame).drop(['dob', 'name_order', 'country'], axis=1)
        return joined_frame

    def league_matches(self, data_frame):
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

    def match_lineups(self, data_frame):
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

    def modifiers(self, data_frame):
        lambdafunc = lambda x: pd.Series([
            ModifierType.from_string(x['modifier']),
            ModifierCategoryType.from_string(x['modifier_category'])
        ])
        id_frame = data_frame.apply(lambdafunc, axis=1)
        id_frame.columns = ['type', 'category']
        joined_frame = data_frame.join(id_frame).drop(["modifier", "modifier_category"], axis=1)
        return joined_frame


class MarcottiEventTransform(MarcottiTransform):

    def league_matches(self, data_frame):
        lambdafunc = lambda x: pd.Series([
            self.get_id(CompetitionMap, remote_id=x['remote_competition_id'], supplier_id=self.supplier_id),
            self.get_id(Seasons, name=x['season_name']),
            self.get_id(VenueMap, remote_id=x['remote_venue_id'], supplier_id=self.supplier_id),
            self.get_id(ClubMap, remote_id=x['remote_home_team_id'], supplier_id=self.supplier_id),
            self.get_id(ClubMap, remote_id=x['remote_away_team_id'], supplier_id=self.supplier_id),
            self.get_id(ManagerMap, remote_id=x['remote_home_manager_id'], supplier_id=self.supplier_id),
            self.get_id(ManagerMap, remote_id=x['remote_away_manager_id'], supplier_id=self.supplier_id),
            self.get_id(RefereeMap, remote_id=x['remote_referee_id'], supplier_id=self.supplier_id),
            self.make_date_object(x['date'])
        ])
        ids_frame = data_frame.apply(lambdafunc, axis=1)
        ids_frame.columns = ['competition_id', 'season_id', 'venue_id', 'home_team_id', 'away_team_id',
                             'home_manager_id', 'away_manager_id', 'referee_id', 'match_date']
        joined_frame = data_frame.join(ids_frame).drop(['season_name', 'date'], axis=1)
        return joined_frame

    def knockout_matches(self, data_frame):
        lambdafunc = lambda x: pd.Series([
            self.get_id(CompetitionMap, remote_id=x['remote_competition_id'], supplier_id=self.supplier_id),
            self.get_id(Seasons, name=x['season_name']),
            self.get_id(VenueMap, remote_id=x['remote_venue_id'], supplier_id=self.supplier_id),
            self.get_id(ClubMap, remote_id=x['remote_home_team_id'], supplier_id=self.supplier_id),
            self.get_id(ClubMap, remote_id=x['remote_away_team_id'], supplier_id=self.supplier_id),
            self.get_id(ManagerMap, remote_id=x['remote_home_manager_id'], supplier_id=self.supplier_id),
            self.get_id(ManagerMap, remote_id=x['remote_away_manager_id'], supplier_id=self.supplier_id),
            self.get_id(RefereeMap, remote_id=x['remote_referee_id'], supplier_id=self.supplier_id),
            KnockoutRoundType.from_string(x['round']),
            self.make_date_object(x['date'])
        ])
        ids_frame = data_frame.apply(lambdafunc, axis=1)
        ids_frame.columns = ['competition_id', 'season_id', 'venue_id', 'home_team_id', 'away_team_id',
                             'home_manager_id', 'away_manager_id', 'referee_id', 'ko_round', 'match_date']
        joined_frame = data_frame.join(ids_frame).drop(['season_name', 'date', 'round'], axis=1)
        return joined_frame

    def group_matches(self, data_frame):
        lambdafunc = lambda x: pd.Series([
            self.get_id(CompetitionMap, remote_id=x['remote_competition_id'], supplier_id=self.supplier_id),
            self.get_id(Seasons, name=x['season_name']),
            self.get_id(VenueMap, remote_id=x['remote_venue_id'], supplier_id=self.supplier_id),
            self.get_id(ClubMap, remote_id=x['remote_home_team_id'], supplier_id=self.supplier_id),
            self.get_id(ClubMap, remote_id=x['remote_away_team_id'], supplier_id=self.supplier_id),
            self.get_id(ManagerMap, remote_id=x['remote_home_manager_id'], supplier_id=self.supplier_id),
            self.get_id(ManagerMap, remote_id=x['remote_away_manager_id'], supplier_id=self.supplier_id),
            self.get_id(RefereeMap, remote_id=x['remote_referee_id'], supplier_id=self.supplier_id),
            GroupRoundType.from_string(x['round']),
            self.make_date_object(x['date'])
        ])
        ids_frame = data_frame.apply(lambdafunc, axis=1)
        ids_frame.columns = ['competition_id', 'season_id', 'venue_id', 'home_team_id', 'away_team_id',
                             'home_manager_id', 'away_manager_id', 'referee_id', 'group_round', 'match_date']
        joined_frame = data_frame.join(ids_frame).drop(['season_name', 'date', 'round'], axis=1)
        return joined_frame

    def match_lineups(self, data_frame):
        lambdafunc = lambda x: pd.Series([
            self.get_id(MatchMap, remote_id=x['remote_match_id'], supplier_id=self.supplier_id),
            self.get_id(PlayerMap, remote_id=x['remote_player_id'], supplier_id=self.supplier_id),
            self.get_id(ClubMap, remote_id=x['remote_team_id'], supplier_id=self.supplier_id),
            self.get_id(PositionMap, remote_id=x['remote_position_id'], supplier_id=self.supplier_id)
        ])
        ids_frame = data_frame.apply(lambdafunc, axis=1)
        ids_frame.columns = ['match_id', 'player_id', 'team_id', 'position_id']
        return data_frame.join(ids_frame).drop(['remote_match_id', 'remote_player_id',
                                                'remote_team_id', 'remote_position_id'], axis=1)

    def events(self, data_frame):
        lambdafunc = lambda x: pd.Series([
            self.get_id(MatchMap, remote_id=x['remote_match_id'], supplier_id=self.supplier_id),
            self.get_id(ClubMap, remote_id=x['remote_team_id'], supplier_id=self.supplier_id)
        ])
        ids_frame = data_frame.apply(lambdafunc, axis=1)
        ids_frame.columns = ['match_id', 'team_id']
        joined_frame = data_frame.join(ids_frame).drop(['remote_match_id', 'remote_team_id'], axis=1)
        new_frame = joined_frame.where((pd.notnull(joined_frame)), None)
        return new_frame

    def actions(self, data_frame):
        match_event_dict = {rec.remote_id: rec.id for rec in
                            self.session.query(MatchEventMap).filter_by(supplier_id=self.supplier_id)}
        match_map_dict = {rec.remote_id: rec.id for rec in
                          self.session.query(MatchMap).filter_by(supplier_id=self.supplier_id)}
        player_map_dict = {rec.remote_id: rec.id for rec in
                           self.session.query(PlayerMap).filter_by(supplier_id=self.supplier_id)}
        lambdafunc = lambda x: pd.Series([
            match_event_dict.get(x['remote_event_id'], None),
            match_map_dict.get(x['remote_match_id'], None),
            player_map_dict.get(x['remote_player_id'], None),
            ActionType.from_string(x['action_type']),
        ])
        ids_frame = data_frame.apply(lambdafunc, axis=1)
        ids_frame.columns = ['event_id', 'match_id', 'player_id', 'type']
        joined_frame = data_frame.join(ids_frame).drop(['remote_event_id', 'remote_match_id',
                                                        'remote_player_id', 'action_type'], axis=1)
        new_frame = joined_frame.where((pd.notnull(joined_frame)), None)
        return new_frame
