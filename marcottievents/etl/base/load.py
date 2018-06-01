import uuid
import logging

import marcottievents.models.common.enums as enums
import marcottievents.models.common.suppliers as mcs
import marcottievents.models.common.overview as mco
import marcottievents.models.common.personnel as mcp
import marcottievents.models.common.match as mcm
import marcottievents.models.common.events as mce
import marcottievents.models.club as mc
from .workflows import WorkflowBase


logger = logging.getLogger(__name__)


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
                    if not self.record_exists(mco.Seasons, start_year_id=yr_obj.id, end_year_id=yr_obj.id):
                        season_records.append(mco.Seasons(start_year=yr_obj, end_year=yr_obj))
                else:
                    start_yr_obj = self.session.query(mco.Years).filter_by(yr=row['start_year']).one()
                    end_yr_obj = self.session.query(mco.Years).filter_by(yr=row['end_year']).one()
                    if not self.record_exists(mco.Seasons, start_year_id=start_yr_obj.id, end_year_id=end_yr_obj.id):
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
        local_ids = []
        comp_records = []
        for idx, row in data_frame.iterrows():
            if 'country_id' in data_frame.columns:
                fields = ['name', 'level', 'country_id']
                comp_dict = {field: row[field] for field in fields if row[field]}
                if not self.record_exists(mco.DomesticCompetitions, **comp_dict):
                    comp_dict.update(id=uuid.uuid4())
                    comp_records.append(mco.DomesticCompetitions(**comp_dict))
                    remote_ids.append(row['remote_id'])
                    local_ids.append(comp_dict['id'])
            elif 'confederation' in data_frame.columns:
                fields = ['name', 'level', 'confederation']
                comp_dict = {field: row[field] for field in fields if row[field]}
                if not self.record_exists(mco.InternationalCompetitions, **comp_dict):
                    comp_dict.update(id=uuid.uuid4())
                    comp_records.append(mco.InternationalCompetitions(**comp_dict))
                    remote_ids.append(row['remote_id'])
                    local_ids.append(comp_dict['id'])
        self.session.bulk_save_objects(comp_records)
        map_records = [mcs.CompetitionMap(id=local_id, remote_id=remote_id, supplier_id=self.supplier_id)
                       for remote_id, local_id in zip(remote_ids, local_ids) if remote_id]
        self.session.bulk_save_objects(map_records)
        self.session.commit()

    def clubs(self, data_frame):
        remote_ids = []
        local_ids = []
        club_records = []
        fields = ['short_name', 'name', 'country_id']
        for idx, row in data_frame.iterrows():
            club_dict = {field: row[field] for field in fields if row[field]}
            if not self.record_exists(mc.Clubs, **club_dict):
                club_dict.update(id=uuid.uuid4())
                club_records.append(mc.Clubs(**club_dict))
                remote_ids.append(row['remote_id'])
                local_ids.append(club_dict['id'])
        self.session.bulk_save_objects(club_records)
        map_records = [mc.ClubMap(id=local_id, remote_id=remote_id, supplier_id=self.supplier_id)
                       for remote_id, local_id in zip(remote_ids, local_ids) if remote_id]
        self.session.bulk_save_objects(map_records)
        self.session.commit()

    def venues(self, data_frame):
        remote_ids = []
        local_ids = []
        venue_records = []
        history_records = []
        fields = ['name', 'city', 'region', 'latitude', 'longitude', 'altitude', 'country_id', 'timezone_id']
        history_fields = ['eff_date', 'length', 'width', 'capacity', 'seats', 'surface_id']
        for idx, row in data_frame.iterrows():
            venue_dict = {field: row[field] for field in fields if row[field]}
            if not self.record_exists(mco.Venues, **venue_dict):
                venue_dict.update(id=uuid.uuid4())
                venue_records.append(mco.Venues(**venue_dict))
                history_dict = {field: row[field] for field in history_fields if row[field]}
                history_records.append(mco.VenueHistory(venue_id=venue_dict['id'], **history_dict))
                remote_ids.append(row['remote_id'])
                local_ids.append(venue_dict['id'])
        self.session.bulk_save_objects(venue_records)
        self.session.bulk_save_objects(history_records)

        map_records = [mcs.VenueMap(id=local_id, remote_id=remote_id, supplier_id=self.supplier_id)
                       for remote_id, local_id in zip(remote_ids, local_ids) if remote_id]
        self.session.bulk_save_objects(map_records)
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
        local_ids = []
        fields = ['known_first_name', 'first_name', 'middle_name', 'last_name', 'second_last_name',
                  'nick_name', 'birth_date', 'order', 'country_id', 'position_id', 'remote_id',
                  'remote_country_id']

        for _, row in data_frame.iterrows():
            player_set.add(tuple([(field, row[field]) for field in fields
                                  if field in row and row[field] is not None]))
        logger.info("{} players in data feed".format(len(player_set)))
        for elements in player_set:
            player_dict = dict(elements)
            remote_id = player_dict.pop('remote_id')
            remote_country_id = player_dict.pop('remote_country_id', None)
            if not self.record_exists(mcs.PlayerMap, remote_id=remote_id):
                if not self.record_exists(mcp.Players, **player_dict):
                    player_dict.update(id=uuid.uuid4(), person_id=uuid.uuid4())
                    player_records.append(mcp.Players(**player_dict))
                    local_ids.append(player_dict['id'])
                    remote_ids.append(remote_id)
                    remote_countryids.append(remote_country_id)
                else:
                    player_id = self.session.query(mcp.Players).filter_by(**player_dict).one().id
                    local_ids.append(player_id)
                    remote_ids.append(remote_id)
            else:
                player_id = self.session.query(mcs.PlayerMap).filter_by(remote_id=remote_id).one().id
                if not self.record_exists(mcp.Players, **player_dict):
                    updated_records = self.session.query(mcp.Players).\
                        filter(mcp.Players.person_id == mcp.Persons.person_id).\
                        filter(mcp.Players.id == player_id)
                    for rec in updated_records:
                        for field, value in player_dict.items():
                            setattr(rec, field, value)
        if self.session.dirty:
            self.session.commit()

        logger.info("{} player records ingested".format(len(player_records)))
        self.session.bulk_save_objects(player_records)
        map_records = [mcs.PlayerMap(id=local_id, remote_id=remote_id, supplier_id=self.supplier_id)
                       for remote_id, local_id in zip(remote_ids, local_ids) if remote_id]
        self.session.bulk_save_objects(map_records)
        self.session.commit()

        for remote_id, player_record in zip(remote_countryids, player_records):
            if remote_id and not self.record_exists(mcs.CountryMap, remote_id=remote_id,
                                                    supplier_id=self.supplier_id):
                self.session.add(mcs.CountryMap(id=player_record.country_id, remote_id=remote_id,
                                                supplier_id=self.supplier_id))
                self.session.commit()

    def managers(self, data_frame):
        manager_records = []
        remote_ids = []
        local_ids = []
        fields = ['known_first_name', 'first_name', 'middle_name', 'last_name', 'second_last_name',
                  'nick_name', 'birth_date', 'order', 'country_id']
        for indx, row in data_frame.iterrows():
            manager_dict = {field: row[field] for field in fields if field in row and row[field]}
            if not self.record_exists(mcs.ManagerMap, remote_id=row['remote_id']):
                if not self.record_exists(mcp.Managers, **manager_dict):
                    manager_dict.update(id=uuid.uuid4(), person_id=uuid.uuid4())
                    manager_records.append(mcp.Managers(**manager_dict))
                    local_ids.append(manager_dict['id'])
                    remote_ids.append(row['remote_id'])
                else:
                    manager_id = self.session.query(mcp.Managers).filter_by(**manager_dict).one().id
                    local_ids.append(manager_id)
                    remote_ids.append(row['remote_id'])
            else:
                manager_id = self.session.query(mcs.ManagerMap).filter_by(remote_id=row['remote_id']).one().id
                if not self.record_exists(mcp.Managers, **manager_dict):
                    updated_records = self.session.query(mcp.Managers).\
                        filter(mcp.Managers.person_id == mcp.Persons.person_id).\
                        filter(mcp.Managers.id == manager_id)
                    for rec in updated_records:
                        for field, value in manager_dict.items():
                            setattr(rec, field, value)
        if self.session.dirty:
            self.session.commit()

        self.session.bulk_save_objects(manager_records)
        map_records = [mcs.ManagerMap(id=local_id, remote_id=remote_id, supplier_id=self.supplier_id)
                       for remote_id, local_id in zip(remote_ids, local_ids) if remote_id]
        self.session.bulk_save_objects(map_records)
        self.session.commit()

    def referees(self, data_frame):
        referee_records = []
        remote_ids = []
        local_ids = []
        fields = ['known_first_name', 'first_name', 'middle_name', 'last_name', 'second_last_name',
                  'nick_name', 'birth_date', 'order', 'country_id']
        for indx, row in data_frame.iterrows():
            referee_dict = {field: row[field] for field in fields if field in row and row[field]}
            if not self.record_exists(mcs.RefereeMap, remote_id=row['remote_id']):
                if not self.record_exists(mcp.Referees, **referee_dict):
                    referee_dict.update(id=uuid.uuid4(), person_id=uuid.uuid4())
                    referee_records.append(mcp.Referees(**referee_dict))
                    remote_ids.append(row['remote_id'])
                    local_ids.append(referee_dict['id'])
                else:
                    referee_id = self.session.query(mcp.Referees).filter_by(**referee_dict).one().id
                    local_ids.append(referee_id)
                    remote_ids.append(row['remote_id'])
            else:
                referee_id = self.session.query(mcs.RefereeMap).filter_by(remote_id=row['remote_id']).one().id
                if not self.record_exists(mcp.Referees, **referee_dict):
                    updated_records = self.session.query(mcp.Referees). \
                        filter(mcp.Referees.person_id == mcp.Persons.person_id). \
                        filter(mcp.Referees.id == referee_id)
                    for rec in updated_records:
                        for field, value in referee_dict.items():
                            setattr(rec, field, value)
        if self.session.dirty:
            self.session.commit()

        self.session.bulk_save_objects(referee_records)
        map_records = [mcs.RefereeMap(id=local_id, remote_id=remote_id, supplier_id=self.supplier_id)
                       for remote_id, local_id in zip(remote_ids, local_ids) if remote_id]
        self.session.bulk_save_objects(map_records)
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
        condition_records = []
        match_records = []
        remote_ids = []
        local_ids = []
        fields = ['match_date', 'competition_id', 'season_id', 'venue_id', 'home_team_id', 'away_team_id',
                  'home_manager_id', 'away_manager_id', 'referee_id', 'attendance', 'matchday']
        condition_fields = ['kickoff_time', 'kickoff_temp', 'kickoff_humidity',
                            'kickoff_weather', 'halftime_weather', 'fulltime_weather']
        for idx, row in data_frame.iterrows():
            match_dict = {field: row[field] for field in fields if field in row and row[field] is not None}
            condition_dict = {field: row[field] for field in condition_fields
                              if field in row and row[field] is not None}
            if not self.record_exists(mc.ClubLeagueMatches, **match_dict):
                match_dict.update(id=uuid.uuid4())
                match_records.append(mc.ClubLeagueMatches(**match_dict))
                condition_records.append(mcm.MatchConditions(id=match_dict['id'], **condition_dict))
                remote_ids.append(row['remote_id'])
                local_ids.append(match_dict['id'])

        self.session.bulk_save_objects(match_records)
        self.session.bulk_save_objects(condition_records)

        map_records = [mcs.MatchMap(id=local_id, remote_id=remote_id, supplier_id=self.supplier_id)
                       for remote_id, local_id in zip(remote_ids, local_ids) if remote_id]
        self.session.bulk_save_objects(map_records)
        self.session.commit()

    def group_matches(self, data_frame):
        condition_records = []
        match_records = []
        remote_ids = []
        local_ids = []
        fields = ['match_date', 'competition_id', 'season_id', 'venue_id', 'home_team_id', 'away_team_id',
                  'home_manager_id', 'away_manager_id', 'referee_id', 'attendance', 'matchday',
                  'group_round', 'group']
        condition_fields = ['kickoff_time', 'kickoff_temp', 'kickoff_humidity',
                            'kickoff_weather', 'halftime_weather', 'fulltime_weather']
        for idx, row in data_frame.iterrows():
            match_dict = {field: row[field] for field in fields if field in row and row[field] is not None}
            condition_dict = {field: row[field] for field in condition_fields
                              if field in row and row[field] is not None}
            if not self.record_exists(mc.ClubGroupMatches, **match_dict):
                match_dict.update(id=uuid.uuid4())
                match_records.append(mc.ClubGroupMatches(**match_dict))
                condition_records.append(mcm.MatchConditions(id=match_dict['id'], **condition_dict))
                remote_ids.append(row['remote_id'])
                local_ids.append(match_dict['id'])

        self.session.bulk_save_objects(match_records)
        self.session.bulk_save_objects(condition_records)

        map_records = [mcs.MatchMap(id=local_id, remote_id=remote_id, supplier_id=self.supplier_id)
                       for remote_id, local_id in zip(remote_ids, local_ids) if remote_id]
        self.session.bulk_save_objects(map_records)
        self.session.commit()

    def knockout_matches(self, data_frame):
        condition_records = []
        match_records = []
        remote_ids = []
        local_ids = []
        fields = ['match_date', 'competition_id', 'season_id', 'venue_id', 'home_team_id', 'away_team_id',
                  'home_manager_id', 'away_manager_id', 'referee_id', 'attendance', 'matchday', 'ko_round',
                  'extra_time']
        condition_fields = ['kickoff_time', 'kickoff_temp', 'kickoff_humidity',
                            'kickoff_weather', 'halftime_weather', 'fulltime_weather']
        for idx, row in data_frame.iterrows():
            match_dict = {field: row[field] for field in fields if field in row and row[field] is not None}
            condition_dict = {field: row[field] for field in condition_fields
                              if field in row and row[field] is not None}
            if not self.record_exists(mc.ClubKnockoutMatches, **match_dict):
                match_dict.update(id=uuid.uuid4())
                match_records.append(mc.ClubKnockoutMatches(**match_dict))
                condition_records.append(mcm.MatchConditions(id=match_dict['id'], **condition_dict))
                remote_ids.append(row['remote_id'])
                local_ids.append(match_dict['id'])

        self.session.bulk_save_objects(match_records)
        self.session.bulk_save_objects(condition_records)

        map_records = [mcs.MatchMap(id=local_id, remote_id=remote_id, supplier_id=self.supplier_id)
                       for remote_id, local_id in zip(remote_ids, local_ids) if remote_id]
        self.session.bulk_save_objects(map_records)
        self.session.commit()

    def match_lineups(self, data_frame):
        lineup_records = []
        fields = ['match_id', 'player_id', 'team_id', 'position_id', 'is_starting', 'is_captain', 'number']
        for idx, row in data_frame.iterrows():
            if not row['player_id']:
                continue
            lineup_dict = {field: row[field] for field in fields if row[field] is not None}
            if not self.record_exists(mc.ClubMatchLineups, **lineup_dict):
                lineup_dict.update(id=uuid.uuid4())
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
        local_ids = []
        fields = ['timestamp', 'period', 'period_secs', 'x', 'y', 'match_id', 'team_id', 'remote_id']
        for _, row in data_frame.iterrows():
            event_set.add(tuple([(field, row[field]) for field in fields
                                 if field in row and row[field] is not None]))
        logger.info("{} unique events".format(len(event_set)))
        for indx, elements in enumerate(event_set):
            if indx and indx % 100 == 0:
                logger.info("Processing {} events".format(indx))
            event_dict = dict(elements)
            remote_id = event_dict.pop('remote_id')
            if 'team_id' not in event_dict:
                # if not self.record_exists(mce.MatchEvents, **event_dict):
                event_dict.update(id=uuid.uuid4())
                event_records.append(mce.MatchEvents(**event_dict))
                remote_ids.append(remote_id)
                local_ids.append(event_dict['id'])
            else:
                # if not self.record_exists(mc.ClubMatchEvents, **event_dict):
                event_dict.update(id=uuid.uuid4())
                event_records.append(mc.ClubMatchEvents(**event_dict))
                remote_ids.append(remote_id)
                local_ids.append(event_dict['id'])
        self.session.bulk_save_objects(event_records)

        map_records = [mcs.MatchEventMap(id=local_id, remote_id=remote_id, supplier_id=self.supplier_id)
                       for remote_id, local_id in zip(remote_ids, local_ids) if remote_id]
        self.session.bulk_save_objects(map_records)
        self.session.commit()

    def actions(self, data_frame):
        action_set = set()
        action_records = []
        lineup_dict = None
        modifier_ids = []
        local_ids = []
        action_fields = ['event_id', 'type', 'x_end', 'y_end', 'z_end',
                         'is_success', 'match_id', 'player_id', 'modifier_type']
        for _, row in data_frame.iterrows():
            action_set.add(tuple([(field, row[field]) for field in action_fields
                                  if field in row and row[field] is not None]))
        logger.info("{} unique actions".format(len(action_set)))
        for indx, elements in enumerate(action_set):
            if indx and indx % 100 == 0:
                logger.info("Processing {} actions".format(indx))
            action_dict = dict(elements)
            match_id = action_dict.pop('match_id')
            player_id = action_dict.pop('player_id', None)
            modifier_type = action_dict.pop('modifier_type', None)
            if not lineup_dict:
                records = self.session.query(mcm.MatchLineups).filter_by(match_id=match_id).all()
                lineup_dict = {rec.player_id: rec.id for rec in records}
            if player_id:
                action_dict['lineup_id'] = lineup_dict[player_id]
            if modifier_type:
                try:
                    modifier_id = self.get_id(mce.Modifiers,
                                              type=enums.ModifierType.from_string(modifier_type))
                except ValueError as ex:
                    logger.info(elements)
                    raise ex
            else:
                modifier_id = None
            # if not self.record_exists(mce.MatchActions, **action_dict):
            action_dict.update(id=uuid.uuid4())
            action_records.append(mce.MatchActions(**action_dict))
            modifier_ids.append(modifier_id)
            local_ids.append(action_dict['id'])
        self.session.bulk_save_objects(action_records)

        modifier_records = [mce.MatchActionModifiers(action_id=local_id, modifier_id=modifier_id)
                            for modifier_id, local_id in zip(modifier_ids, local_ids)]
        self.session.bulk_save_objects(modifier_records)
        self.session.commit()
