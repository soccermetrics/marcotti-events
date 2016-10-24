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
        player_records = []
        remote_countryids = []
        remote_ids = []
        fields = ['known_first_name', 'first_name', 'middle_name', 'last_name', 'second_last_name',
                  'nick_name', 'birth_date', 'order', 'country_id', 'position_id']
        for indx, row in data_frame.iterrows():
            player_dict = {field: row[field] for field in fields if row[field]}
            if not self.record_exists(mcp.Players, **player_dict):
                remote_ids.append(row['remote_id'])
                remote_countryids.append(row.get('remote_country_id', None))
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
        for row in data_frame.iterrows():
            match_dict = dict(date=row['date'], competition_id=row['competition_id'],
                              season_id=row['season_id'], venue_id=row['venue_id'],
                              home_team_id=row['home_team_id'], away_team_id=row['away_team_id'],
                              home_manager_id=row['home_manager_id'], away_manager_id=row['away_manager_id'],
                              referee_id=row['referee_id'], matchday=row['matchday'])
            if not self.record_exists(mc.ClubLeagueMatches, **match_dict):
                match_records.append(mcm.MatchConditions(match=mc.ClubLeagueMatches(**match_dict),
                                                     kickoff_time=row['kickoff_time']))
            else:
                print "Cannot insert League Match record: Record exists"
        self.session.add_all(match_records)
        self.session.commit()
        map_records = [mcs.MatchMap(id=match_record.id, remote_id=data_row['remote_match_id'],
                                supplier_id=self.supplier_id)
                       for data_row, match_record in zip(data_frame.iterrows(), match_records)]
        self.session.add_all(map_records)
        self.session.commit()

    def lineups(self, data_frame):
        lineup_records = []
        for row in data_frame.iterrows():
            position_id = self.session.query(mcp.Players).get(row['player_id']).position_id
            lineup_dict = dict(match_id=row['match_id'], player_id=row['player_id'], team_id=row['team_id'],
                               position_id=position_id, is_starting=row['is_starting'], is_captain=row['is_captain'])
            if not self.record_exists(mc.ClubMatchLineups, **lineup_dict):
                lineup_records.append(mc.ClubMatchLineups(**lineup_dict))
            else:
                print "Cannot insert Match Lineup record: Record exists"
        self.session.add_all(lineup_records)
        self.session.commit()

    def modifiers(self, data_frame):
        mod_records = [mce.Modifiers(**row) for indx, row in data_frame.iterrows()
                       if not self.record_exists(mce.Modifiers, type=row['type'])]
        self.session.add_all(mod_records)
        self.session.commit()

    def events(self, data_frame):
        event_records = []
        for row in data_frame.iterrows():
            event_dict = dict(timestamp=row['timestamp'], period=row['period'], period_secs=row['period_secs'],
                              x=row['x'], y=row['y'], match_id=row['match_id'], team_id=row['team_id'])
            if not self.record_exists(mc.ClubMatchEvents, **event_dict):
                event_records.append(mc.ClubMatchEvents(**event_dict))
            else:
                print "Cannot insert Match Event record: Record exists"
        self.session.add_all(event_records)
        self.session.commit()
        map_records = [mcs.MatchEventMap(id=event_record.id, remote_id=data_row['remote_event_id'],
                                         supplier_id=self.supplier_id)
                       for data_row, event_record in zip(data_frame.iterrows(), event_records)]
        self.session.add_all(map_records)
        self.session.commit()

    def actions(self, data_frame):
        for row in data_frame.iterrows():
            action_dict = dict(event_id=row['event_id'], lineup_id=row['lineup_id'],
                               type=row['action_type'], is_success=row['is_success'],
                               x_end=row['x_end'], y_end=row['y_end'], z_end=row['z_end'])
            if not self.record_exists(mce.MatchActions, **action_dict):
                self.session.add(mce.MatchActions(**action_dict))
                self.session.commit()
            action_id = self.session.query(mce.MatchActions).filter_by(**action_dict).one().id
            modifier_dict = dict(action_id=action_id, modifier_id=row['modifier_id'])
            if not self.record_exists(mce.MatchActionModifiers, **modifier_dict):
                self.session.add(mce.MatchActionModifiers(**modifier_dict))
                self.session.commit()
