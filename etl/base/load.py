import pandas as pd

import models.common.enums as enums
import models.common.suppliers as mcs
import models.common.overview as mco
import models.common.personnel as mcp
import models.common.match as mcm
import models.common.events as mce
import models.club as mc
from .workflows import WorkflowBase


class MarcottiLoad(WorkflowBase):
    """
    Load transformed data into database.
    """

    def record_exists(self, model, **conditions):
        return self.session.query(model).filter_by(**conditions).count() != 0

    def suppliers(self, data_frame):
        supplier_records = [mcs.Suppliers(**data_row) for idx, data_row in data_frame.iterrows()
                            if not self.record_exists(mcs.Suppliers, name=data_row['name'])]
        self.session.add_all(supplier_records)
        self.session.commit()

    def years(self, data_frame):
        year_records = [mco.Years(**data_row) for idx, data_row in data_frame.iterrows()
                        if not self.record_exists(mco.Years, yr=data_row['yr'])]
        self.session.add_all(year_records)
        self.session.commit()

    def seasons(self, data_frame):
        season_records = []
        map_records = []
        for idx, row in data_frame.iterrows():
            if 'name' not in row:
                if row['start_year'] == row['end_year']:
                    yr_obj = self.session.query(mco.Years).filter_by(yr=row['start_year']).one()
                    season_records.append(mco.Seasons(start_year=yr_obj, end_year=yr_obj))
                else:
                    start_yr_obj = self.session.query(mco.Years).filter_by(yr=row['start_year']).one()
                    end_yr_obj = self.session.query(mco.Years).filter_by(yr=row['end_year']).one()
                    season_records.append(mco.Seasons(start_year=start_yr_obj, end_year=end_yr_obj))
                self.session.add_all(season_records)
            else:
                if not self.record_exists(mcs.SeasonMap, remote_id=row['remote_id'], supplier_id=self.supplier_id):
                    map_records.append(mcs.SeasonMap(id=self.get_id(mco.Seasons, name=row['name']),
                                                     remote_id=row['remote_id'],
                                                     supplier_id=self.supplier_id))
                self.session.add_all(map_records)
        self.session.commit()

    def countries(self, data_frame):
        remote_ids = []
        country_records = []
        fields = ['name', 'code', 'confederation']
        for idx, row in data_frame.iterrows():
            country_dict = {field: row[field] for field in fields if row[field]}
            if not self.record_exists(mco.Countries, name=row['name']):
                country_records.append(mco.Countries(**country_dict))
                remote_ids.append(row['remote_id'])
        self.session.add_all(country_records)
        self.session.commit()
        map_records = [mcs.CountryMap(id=country_record.id, remote_id=remote_id, supplier_id=self.supplier_id)
                       for remote_id, country_record in zip(remote_ids, country_records) if remote_id]
        self.session.add_all(map_records)
        self.session.commit()

    def competitions(self, data_frame):
        remote_ids = []
        comp_records = []
        for idx, row in data_frame.iterrows():
            if 'country_id' in data_frame.columns:
                fields = ['name', 'level', 'country_id']
                comp_dict = {field: row[field] for field in fields if row[field]}
                if not self.record_exists(mco.DomesticCompetitions, **comp_dict):
                    comp_records.append(mco.DomesticCompetitions(**comp_dict))
                    remote_ids.append(row['remote_id'])
            elif 'confederation' in data_frame.columns:
                fields = ['name', 'level', 'confederation']
                comp_dict = {field: row[field] for field in fields if row[field]}
                if not self.record_exists(mco.InternationalCompetitions, **comp_dict):
                    comp_records.append(mco.InternationalCompetitions(**comp_dict))
                    remote_ids.append(row['remote_id'])
        self.session.add_all(comp_records)
        self.session.commit()
        map_records = [mcs.CompetitionMap(id=comp_record.id, remote_id=remote_id, supplier_id=self.supplier_id)
                       for remote_id, comp_record in zip(remote_ids, comp_records) if remote_id]
        self.session.add_all(map_records)
        self.session.commit()

    def clubs(self, data_frame):
        remote_ids = []
        club_records = []
        fields = ['short_name', 'name', 'country_id']
        for idx, row in data_frame.iterrows():
            club_dict = {field: row[field] for field in fields if row[field]}
            if not self.record_exists(mc.Clubs, **club_dict):
                remote_ids.append(row['remote_id'])
                club_records.append(mc.Clubs(**club_dict))
        self.session.add_all(club_records)
        self.session.commit()
        map_records = [mc.ClubMap(id=club_record.id, remote_id=remote_id, supplier_id=self.supplier_id)
                       for remote_id, club_record in zip(remote_ids, club_records) if remote_id]
        self.session.add_all(map_records)
        self.session.commit()

    def venues(self, data_frame):
        remote_ids = []
        venue_records = []
        history_dicts = []
        fields = ['name', 'city', 'region', 'latitude', 'longitude', 'altitude', 'country_id', 'timezone_id']
        history_fields = ['eff_date', 'length', 'width', 'capacity', 'seats', 'surface_id']
        for idx, row in data_frame.iterrows():
            venue_dict = {field: row[field] for field in fields if row[field]}
            if not self.record_exists(mco.Venues, **venue_dict):
                venue_records.append(mco.Venues(**venue_dict))
                history_dicts.append({field: row[field] for field in history_fields if row[field]})
                remote_ids.append(row['remote_id'])
        self.session.add_all(venue_records)
        self.session.commit()
        history_records = [mco.VenueHistory(venue_id=venue_record.id, **history_dict)
                           for history_dict, venue_record in zip(history_dicts, venue_records)]
        self.session.add_all(history_records)
        map_records = [mcs.VenueMap(id=venue_record.id, remote_id=remote_id, supplier_id=self.supplier_id)
                       for remote_id, venue_record in zip(remote_ids, venue_records) if remote_id]
        self.session.add_all(map_records)
        self.session.commit()

    def surfaces(self, data_frame):
        surface_records = [mco.Surfaces(**row) for indx, row in data_frame.iterrows()
                           if not self.record_exists(mco.Surfaces, description=row['description'])]
        self.session.add_all(surface_records)
        self.session.commit()

    def timezones(self, data_frame):
        tz_records = [mco.Timezones(**row) for indx, row in data_frame.iterrows()
                      if not self.record_exists(mco.Timezones, name=row['name'])]
        self.session.add_all(tz_records)
        self.session.commit()

    def players(self, data_frame):
        player_set = set()
        player_records = []
        remote_countryids = []
        remote_ids = []
        fields = ['known_first_name', 'first_name', 'middle_name', 'last_name', 'second_last_name',
                  'nick_name', 'birth_date', 'order', 'country_id', 'position_id', 'remote_id',
                  'remote_country_id']

        for _, row in data_frame.iterrows():
            player_set.add(tuple([(field, row[field]) for field in fields
                                  if field in row and row[field] is not None]))
        for elements in player_set:
            player_dict = dict(elements)
            remote_id = player_dict.pop('remote_id')
            remote_country_id = player_dict.pop('remote_country_id', None)
            if not self.record_exists(mcp.Players, **player_dict):
                remote_ids.append(remote_id)
                remote_countryids.append(remote_country_id)
                player_records.append(mcp.Players(**player_dict))

        self.session.add_all(player_records)
        self.session.commit()
        map_records = [mcs.PlayerMap(id=player_record.id, remote_id=remote_id, supplier_id=self.supplier_id)
                       for remote_id, player_record in zip(remote_ids, player_records) if remote_id]
        self.session.add_all(map_records)
        self.session.commit()
        for remote_id, player_record in zip(remote_countryids, player_records):
            if remote_id and not self.record_exists(
                    mcs.CountryMap, remote_id=remote_id, supplier_id=self.supplier_id):
                self.session.add(mcs.CountryMap(id=player_record.country_id, remote_id=remote_id,
                                                supplier_id=self.supplier_id))
                self.session.commit()

    def managers(self, data_frame):
        manager_records = []
        remote_ids = []
        fields = ['known_first_name', 'first_name', 'middle_name', 'last_name', 'second_last_name',
                  'nick_name', 'birth_date', 'order', 'country_id']
        for indx, row in data_frame.iterrows():
            manager_dict = {field: row[field] for field in fields if row[field]}
            if not self.record_exists(mcp.Managers, **manager_dict):
                remote_ids.append(row['remote_id'])
                manager_records.append(mcp.Managers(**manager_dict))
        self.session.add_all(manager_records)
        self.session.commit()
        map_records = [mcs.ManagerMap(id=manager_record.id, remote_id=remote_id, supplier_id=self.supplier_id)
                       for remote_id, manager_record in zip(remote_ids, manager_records) if remote_id]
        self.session.add_all(map_records)
        self.session.commit()

    def referees(self, data_frame):
        referee_records = []
        remote_ids = []
        fields = ['known_first_name', 'first_name', 'middle_name', 'last_name', 'second_last_name',
                  'nick_name', 'birth_date', 'order', 'country_id']
        for indx, row in data_frame.iterrows():
            referee_dict = {field: row[field] for field in fields if row[field]}
            if not self.record_exists(mcp.Referees, **referee_dict):
                remote_ids.append(row['remote_id'])
                referee_records.append(mcp.Referees(**referee_dict))
        self.session.add_all(referee_records)
        self.session.commit()
        map_records = [mcs.RefereeMap(id=referee_record.id, remote_id=remote_id, supplier_id=self.supplier_id)
                       for remote_id, referee_record in zip(remote_ids, referee_records) if remote_id]
        self.session.add_all(map_records)
        self.session.commit()

    def positions(self, data_frame):
        position_record = []
        for indx, row in data_frame.iterrows():
            if row['remote_id'] and self.supplier_id:
                if not self.record_exists(mcs.PositionMap, remote_id=row['remote_id'], supplier_id=self.supplier_id):
                    position_record.append(mcs.PositionMap(
                        id=self.get_id(mcp.Positions, name=row['name']),
                        remote_id=row['remote_id'], supplier_id=self.supplier_id))
            else:
                if not self.record_exists(mcp.Positions, name=row['name']):
                    position_record.append(mcp.Positions(name=row['name'], type=row['type']))
        self.session.add_all(position_record)
        self.session.commit()

    def league_matches(self, data_frame):
        match_records = []
        remote_ids = []
        fields = ['match_date', 'competition_id', 'season_id', 'venue_id', 'home_team_id', 'away_team_id',
                  'home_manager_id', 'away_manager_id', 'referee_id', 'attendance', 'matchday']
        condition_fields = ['kickoff_time', 'kickoff_temp', 'kickoff_humidity',
                            'kickoff_weather', 'halftime_weather', 'fulltime_weather']
        for idx, row in data_frame.iterrows():
            match_dict = {field: row[field] for field in fields if row[field]}
            condition_dict = {field: row[field] for field in condition_fields if field in row and row[field]}
            if not self.record_exists(mc.ClubLeagueMatches, **match_dict):
                match_records.append(mcm.MatchConditions(match=mc.ClubLeagueMatches(**match_dict), **condition_dict))
                remote_ids.append(row['remote_id'])
        self.session.add_all(match_records)
        self.session.commit()

        map_records = [mcs.MatchMap(id=match_record.id, remote_id=remote_id, supplier_id=self.supplier_id)
                       for remote_id, match_record in zip(remote_ids, match_records) if remote_id]
        self.session.add_all(map_records)
        self.session.commit()

    def match_lineups(self, data_frame):
        lineup_records = []
        fields = ['match_id', 'player_id', 'team_id', 'position_id', 'is_starting', 'is_captain']
        for idx, row in data_frame.iterrows():
            lineup_dict = {field: row[field] for field in fields if row[field] is not None}
            if not self.record_exists(mc.ClubMatchLineups, **lineup_dict):
                lineup_records.append(mc.ClubMatchLineups(**lineup_dict))
        self.session.add_all(lineup_records)
        self.session.commit()

    def modifiers(self, data_frame):
        mod_records = [mce.Modifiers(**row) for indx, row in data_frame.iterrows()
                       if not self.record_exists(mce.Modifiers, type=row['type'])]
        self.session.add_all(mod_records)
        self.session.commit()

    def events(self, data_frame):
        event_set = set()
        event_records = []
        remote_ids = []
        fields = ['timestamp', 'period', 'period_secs', 'x', 'y', 'match_id', 'team_id', 'remote_id']
        for _, row in data_frame.iterrows():
            event_set.add(tuple([(field, row[field]) for field in fields
                                 if field in row and row[field] is not None]))
        print "{} unique events".format(len(event_set))
        for indx, elements in enumerate(event_set):
            if indx and indx % 100 == 0:
                print "Processing {} events".format(indx)
            event_dict = dict(elements)
            remote_id = event_dict.pop('remote_id')
            if 'team_id' not in event_dict:
                if not self.record_exists(mce.MatchEvents, **event_dict):
                    event_records.append(mce.MatchEvents(**event_dict))
                    remote_ids.append(remote_id)
            else:
                if not self.record_exists(mc.ClubMatchEvents, **event_dict):
                    event_records.append(mc.ClubMatchEvents(**event_dict))
                    remote_ids.append(remote_id)
        self.session.add_all(event_records)
        self.session.commit()
        map_records = [mcs.MatchEventMap(id=event_record.id, remote_id=remote_id, supplier_id=self.supplier_id)
                       for remote_id, event_record in zip(remote_ids, event_records) if remote_id and not
                       self.record_exists(mcs.MatchEventMap, remote_id=remote_id, supplier_id=self.supplier_id)]
        self.session.add_all(map_records)
        self.session.commit()

    def actions(self, data_frame):
        action_set = set()
        action_records = []
        modifier_ids = []
        action_fields = ['event_id', 'type', 'x_end', 'y_end', 'z_end',
                         'is_success', 'match_id', 'player_id', 'modifier_type']
        for _, row in data_frame.iterrows():
            action_set.add(tuple([(field, row[field]) for field in action_fields
                                  if field in row and row[field] is not None]))
        print "{} unique actions".format(len(action_set))
        for indx, elements in enumerate(action_set):
            if indx and indx % 100 == 0:
                print "Processing {} actions".format(indx)
            action_dict = dict(elements)
            match_id = action_dict.pop('match_id')
            player_id = action_dict.pop('player_id', None)
            modifier_type = action_dict.pop('modifier_type', None)
            if player_id:
                action_dict['lineup_id'] = self.get_id(mcm.MatchLineups, match_id=match_id, player_id=player_id)
            if modifier_type:
                try:
                    modifier_id = self.get_id(mce.Modifiers,
                                              type=enums.ModifierType.from_string(modifier_type))
                except ValueError as ex:
                    print elements
                    raise ex
            else:
                modifier_id = None
            if not self.record_exists(mce.MatchActions, **action_dict):
                action_records.append(mce.MatchActions(**action_dict))
                modifier_ids.append(modifier_id)
        self.session.add_all(action_records)
        self.session.commit()
        modifier_records = [mce.MatchActionModifiers(action_id=action_record.id, modifier_id=modifier_id)
                            for modifier_id, action_record in zip(modifier_ids, action_records) if not
                            self.record_exists(mce.MatchActionModifiers, action_id=action_record.id,
                                               modifier_id=modifier_id)]
        self.session.add_all(modifier_records)
        self.session.commit()
