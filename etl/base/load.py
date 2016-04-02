from models.common.suppliers import MatchEventMap, MatchMap, CompetitionMap, SeasonMap, PlayerMap
from models.common.overview import Seasons, DomesticCompetitions, InternationalCompetitions, Venues
from models.common.personnel import Players, Managers, Referees
from models.common.match import MatchConditions
from models.common.events import MatchActions, MatchActionModifiers
from models.club import Clubs, ClubMatchLineups, ClubLeagueMatches, ClubMatchEvents, ClubMap
from .workflows import WorkflowBase


class MarcottiLoad(WorkflowBase):
    """
    Load transformed data into database.
    """

    def record_exists(self, model, **conditions):
        return self.session.query(model).filter_by(**conditions).count() != 0

    def seasons(self, data_frame):
        map_records = [SeasonMap(id=self.get_id(Seasons, name=data_row['name']), remote_id=data_row['remote_id'],
                                 supplier_id=self.supplier_id) for data_row in data_frame.iterrows()]
        self.session.add_all(map_records)
        self.session.commit()

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
        map_records = [CompetitionMap(id=comp_record.id, remote_id=data_row['remote_competition_id'],
                                      supplier_id=self.supplier_id)
                       for data_row, comp_record in zip(data_frame.iterrows(), comp_records)]
        self.session.add_all(map_records)
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
        map_records = [ClubMap(id=club_record.id, remote_id=data_row['remote_club_id'], supplier_id=self.supplier_id)
                       for data_row, club_record in zip(data_frame.iterrows(), club_records)]
        self.session.add_all(map_records)
        self.session.commit()

    def venues(self, data_frame):
        venue_records = []
        for row in data_frame.iterrows():
            venue_dict = dict(name=row['name'], city=row['city'], region=row['region'],
                              latitude=row['latitude'], longitude=row['longitude'], altitude=row['altitude'],
                              country_id=row['country_id'], timezone_id=row['timezone_id'])
            if not self.record_exists(Venues, **venue_dict):
                venue_records.append(Venues(**venue_dict))
            else:
                print "Cannot insert Venue record: Record exists"
        self.session.add_all(venue_records)
        self.session.commit()

    def players(self, data_frame):
        player_records = []
        for row in data_frame.iterrows():
            player_dict = dict(first_name=row['first_name'], middle_name=row['middle_name'],
                               last_name=row['last_name'], second_last_name=row['second_last_name'],
                               nick_name=row['nick_name'], birth_date=row['birth_date'],
                               order=row['name_order'], country_id=row['country_id'],
                               position_id=row['position_id'])
            if not self.record_exists(Players, **player_dict):
                player_records.append(Players(**player_dict))
            else:
                print "Cannot insert Player record: Record exists"
        self.session.add_all(player_records)
        self.session.commit()
        map_records = [PlayerMap(id=player_record.id, remote_id=data_row['remote_player_id'],
                                 supplier_id=self.supplier_id)
                       for data_row, player_record in zip(data_frame.iterrows(), player_records)]
        self.session.add_all(map_records)
        self.session.commit()

    def managers(self, data_frame):
        manager_records = []

        for row in data_frame.iterrows():
            manager_dict = dict(first_name=row['first_name'], middle_name=row['middle_name'],
                                last_name=row['last_name'], second_last_name=row['second_last_name'],
                                nick_name=row['nick_name'], birth_date=row['birth_date'],
                                order=row['name_order'], country_id=row['country_id'])
            if not self.record_exists(Managers, **manager_dict):
                manager_records.append(Managers(**manager_dict))
            else:
                print "Cannot insert Manager record: Record exists"
        self.session.add_all(manager_records)
        self.session.commit()

    def referees(self, data_frame):
        referee_records = []
        for row in data_frame.iterrows():
            referee_dict = dict(first_name=row['first_name'], middle_name=row['middle_name'],
                                last_name=row['last_name'], second_last_name=row['second_last_name'],
                                nick_name=row['nick_name'], birth_date=row['birth_date'],
                                order=row['name_order'], country_id=row['country_id'])
            if not self.record_exists(Referees, **referee_dict):
                referee_records.append(Referees(**referee_dict))
            else:
                print "Cannot insert Referee record: Record exists"
        self.session.add_all(referee_records)
        self.session.commit()

    def league_matches(self, data_frame):
        match_records = []
        for row in data_frame.iterrows():
            match_dict = dict(date=row['match_date'], competition_id=row['competition_id'],
                              season_id=row['season_id'], venue_id=row['venue_id'],
                              home_team_id=row['home_team_id'], away_team_id=row['away_team_id'],
                              home_manager_id=row['home_manager_id'], away_manager_id=row['away_manager_id'],
                              referee_id=row['referee_id'], matchday=row['matchday'])
            if not self.record_exists(ClubLeagueMatches, **match_dict):
                match_records.append(MatchConditions(match=ClubLeagueMatches(**match_dict),
                                                     kickoff_time=row['kickoff_time']))
            else:
                print "Cannot insert League Match record: Record exists"
        self.session.add_all(match_records)
        self.session.commit()
        map_records = [MatchMap(id=match_record.id, remote_id=data_row['remote_match_id'],
                                supplier_id=self.supplier_id)
                       for data_row, match_record in zip(data_frame.iterrows(), match_records)]
        self.session.add_all(map_records)
        self.session.commit()

    def lineups(self, data_frame):
        lineup_records = []
        for row in data_frame.iterrows():
            position_id = self.session.query(Players).get(row['player_id']).position_id
            lineup_dict = dict(match_id=row['match_id'], player_id=row['player_id'], team_id=row['team_id'],
                               position_id=position_id, is_starting=row['is_starting'], is_captain=row['is_captain'])
            if not self.record_exists(ClubMatchLineups, **lineup_dict):
                lineup_records.append(ClubMatchLineups(**lineup_dict))
            else:
                print "Cannot insert Match Lineup record: Record exists"
        self.session.add_all(lineup_records)
        self.session.commit()

    def events(self, data_frame):
        event_records = []
        for row in data_frame.iterrows():
            event_dict = dict(timestamp=row['timestamp'], period=row['period'], period_secs=row['period_secs'],
                              x=row['x'], y=row['y'], match_id=row['match_id'], team_id=row['team_id'])
            if not self.record_exists(ClubMatchEvents, **event_dict):
                event_records.append(ClubMatchEvents(**event_dict))
            else:
                print "Cannot insert Match Event record: Record exists"
        self.session.add_all(event_records)
        self.session.commit()
        map_records = [MatchEventMap(id=event_record.id, remote_id=data_row['remote_event_id'],
                                     supplier_id=self.supplier_id)
                       for data_row, event_record in zip(data_frame.iterrows(), event_records)]
        self.session.add_all(map_records)
        self.session.commit()

    def actions(self, data_frame):
        for row in data_frame.iterrows():
            action_dict = dict(event_id=row['event_id'], lineup_id=row['lineup_id'],
                               type=row['action_type'], is_success=row['is_success'],
                               x_end=row['x_end'], y_end=row['y_end'], z_end=row['z_end'])
            if not self.record_exists(MatchActions, **action_dict):
                self.session.add(MatchActions(**action_dict))
                self.session.commit()
            action_id = self.session.query(MatchActions).filter_by(**action_dict).one().id
            modifier_dict = dict(action_id=action_id, modifier_id=row['modifier_id'])
            if not self.record_exists(MatchActionModifiers, **modifier_dict):
                self.session.add(MatchActionModifiers(**modifier_dict))
                self.session.commit()
