from .base import BaseCSV, extract


class CSVExtractor(BaseCSV):

    @extract
    def suppliers(self, *args, **kwargs):
        return [dict(name=self.column_unicode("Name", **keys)) for keys in kwargs.get('data')]

    @staticmethod
    def years(start_yr, end_yr):
        return [dict(yr=yr) for yr in range(start_yr, end_yr+1)]

    @staticmethod
    def seasons(start_yr, end_yr):
        year_range = range(start_yr, end_yr+1)
        return [dict(start_year=yr, end_year=yr) for yr in year_range] + \
            [dict(start_year=start, end_year=end) for start, end in zip(year_range[:-1], year_range[1:])]

    @extract
    def countries(self, *args, **kwargs):
        return [dict(remote_id=self.column("ID", **keys),
                     name=self.column_unicode("Name", **keys),
                     code=self.column("Code", **keys),
                     confed=self.column("Confederation", **keys))
                for keys in kwargs.get('data')]

    @extract
    def competitions(self, *args, **kwargs):
        return [dict(remote_id=self.column("ID", **keys),
                     name=self.column_unicode("Name", **keys),
                     level=self.column_int("Level", **keys),
                     country=self.column_unicode("Country", **keys),
                     confed=self.column("Confederation", **keys))
                for keys in kwargs.get('data')]

    @extract
    def venues(self, *args, **kwargs):
        return [dict(remote_id=self.column("ID", **keys),
                     name=self.column_unicode("Venue Name", **keys),
                     city=self.column_unicode("City", **keys),
                     region=self.column_unicode("Region", **keys),
                     country=self.column_unicode("Country", **keys),
                     timezone=self.column_unicode("Timezone", **keys),
                     latitude=self.column_float("Latitude", **keys),
                     longitude=self.column_float("Longitude", **keys),
                     altitude=self.column_int("Altitude", **keys),
                     config_date=self.column("Config Date", **keys),
                     surface=self.column_unicode("Surface", **keys),
                     length=self.column_int("Length", **keys),
                     width=self.column_int("Width", **keys),
                     capacity=self.column_int("Capacity", **keys),
                     seats=self.column_int("Seats", **keys))
                for keys in kwargs.get('data')]

    @extract
    def surfaces(self, *args, **kwargs):
        return [dict(description=self.column_unicode("Description", **keys),
                     surface_type=self.column("Type", **keys))
                for keys in kwargs.get('data')]

    @extract
    def timezones(self, *args, **kwargs):
        return [dict(name=self.column_unicode("Name", **keys),
                     confed=self.column("Confederation", **keys),
                     offset=self.column_float("Offset", **keys))
                for keys in kwargs.get('data')]

    @extract
    def clubs(self, *args, **kwargs):
        return [dict(remote_id=self.column("ID", **keys),
                     name=self.column_unicode("Name", **keys),
                     short_name=self.column_unicode("Short Name", **keys),
                     country=self.column_unicode("Country", **keys))
                for keys in kwargs.get('data')]

    @extract
    def managers(self, *args, **kwargs):
        return [dict(remote_id=self.column("ID", **keys),
                     first_name=self.column_unicode("First Name", **keys),
                     known_first_name=self.column_unicode("Known First Name", **keys),
                     middle_name=self.column_unicode("Middle Name", **keys),
                     last_name=self.column_unicode("Last Name", **keys),
                     second_last_name=self.column_unicode("Second Last Name", **keys),
                     nick_name=self.column_unicode("Nickname", **keys),
                     name_order=self.column("Name Order", **keys),
                     dob=self.column("Birthdate", **keys),
                     country=self.column_unicode("Country", **keys))
                for keys in kwargs.get('data')]

    @extract
    def referees(self, *args, **kwargs):
        return [dict(remote_id=self.column("ID", **keys),
                     first_name=self.column_unicode("First Name", **keys),
                     known_first_name=self.column_unicode("Known First Name", **keys),
                     middle_name=self.column_unicode("Middle Name", **keys),
                     last_name=self.column_unicode("Last Name", **keys),
                     second_last_name=self.column_unicode("Second Last Name", **keys),
                     nick_name=self.column_unicode("Nickname", **keys),
                     name_order=self.column("Name Order", **keys),
                     dob=self.column("Birthdate", **keys),
                     country=self.column_unicode("Country", **keys))
                for keys in kwargs.get('data')]

    @extract
    def players(self, *args, **kwargs):
        return [dict(remote_id=self.column("ID", **keys),
                     first_name=self.column_unicode("First Name", **keys),
                     known_first_name=self.column_unicode("Known First Name", **keys),
                     middle_name=self.column_unicode("Middle Name", **keys),
                     last_name=self.column_unicode("Last Name", **keys),
                     second_last_name=self.column_unicode("Second Last Name", **keys),
                     nick_name=self.column_unicode("Nickname", **keys),
                     name_order=self.column("Name Order", **keys),
                     dob=self.column("Birthdate", **keys),
                     country=self.column_unicode("Country", **keys),
                     position_name=self.column("Position", **keys))
                for keys in kwargs.get('data')]

    @extract
    def positions(self, *args, **kwargs):
        return [dict(remote_id=self.column("ID", **keys),
                     name=self.column_unicode("Position", **keys),
                     position_type=self.column("Type", **keys))
                for keys in kwargs.get('data')]

    @extract
    def league_matches(self, *args, **kwargs):
        return [dict(remote_id=self.column("ID", **keys),
                     competition=self.column_unicode("Competition", **keys),
                     season=self.column("Season", **keys),
                     match_date=self.column("Match Date", **keys),
                     match_time=self.column("KO Time", **keys),
                     matchday=self.column_int("Matchday", **keys),
                     venue=self.column_unicode("Venue", **keys),
                     home_team=self.column_unicode("Home Team", **keys),
                     away_team=self.column_unicode("Away Team", **keys),
                     home_manager=self.column_unicode("Home Manager", **keys),
                     away_manager=self.column_unicode("Away Manager", **keys),
                     referee=self.column_unicode("Referee", **keys),
                     attendance=self.column_int("Attendance", **keys),
                     kickoff_temp=self.column_float("KO Temp", **keys),
                     kickoff_humid=self.column_float("KO Humidity", **keys),
                     kickoff_wx=self.column("KO Wx", **keys),
                     halftime_wx=self.column("HT Wx", **keys),
                     fulltime_wx=self.column("FT Wx", **keys))
                for keys in kwargs.get('data')]

    @extract
    def group_matches(self, *args, **kwargs):
        return [dict(remote_id=self.column("ID", **keys),
                     competition=self.column_unicode("Competition", **keys),
                     season=self.column("Season", **keys),
                     match_date=self.column("Match Date", **keys),
                     match_time=self.column("KO Time", **keys),
                     group_round=self.column("Group Round", **keys),
                     group=self.column("Group", **keys),
                     matchday=self.column_int("Matchday", **keys),
                     venue=self.column_unicode("Venue", **keys),
                     home_team=self.column_unicode("Home Team", **keys),
                     away_team=self.column_unicode("Away Team", **keys),
                     home_manager=self.column_unicode("Home Manager", **keys),
                     away_manager=self.column_unicode("Away Manager", **keys),
                     referee=self.column_unicode("Referee", **keys),
                     attendance=self.column_int("Attendance", **keys),
                     kickoff_temp=self.column_float("KO Temp", **keys),
                     kickoff_humid=self.column_float("KO Humidity", **keys),
                     kickoff_wx=self.column("KO Wx", **keys),
                     halftime_wx=self.column("HT Wx", **keys),
                     fulltime_wx=self.column("FT Wx", **keys))
                for keys in kwargs.get('data')]

    @extract
    def knockout_matches(self, *args, **kwargs):
        return [dict(remote_id=self.column("ID", **keys),
                     competition=self.column_unicode("Competition", **keys),
                     season=self.column("Season", **keys),
                     match_date=self.column("Match Date", **keys),
                     match_time=self.column("KO Time", **keys),
                     knockout_round=self.column("Knockout Round", **keys),
                     matchday=self.column_int("Matchday", **keys),
                     venue=self.column_unicode("Venue", **keys),
                     home_team=self.column_unicode("Home Team", **keys),
                     away_team=self.column_unicode("Away Team", **keys),
                     home_manager=self.column_unicode("Home Manager", **keys),
                     away_manager=self.column_unicode("Away Manager", **keys),
                     referee=self.column_unicode("Referee", **keys),
                     attendance=self.column_int("Attendance", **keys),
                     kickoff_temp=self.column_float("KO Temp", **keys),
                     kickoff_humid=self.column_float("KO Humidity", **keys),
                     kickoff_wx=self.column("KO Wx", **keys),
                     halftime_wx=self.column("HT Wx", **keys),
                     fulltime_wx=self.column("FT Wx", **keys),
                     extra_time=self.column_bool("Extra Time", **keys))
                for keys in kwargs.get('data')]

    @extract
    def match_lineups(self, *args, **kwargs):
        return [dict(competition=self.column_unicode("Competition", **keys),
                     season=self.column("Season", **keys),
                     matchday=self.column_int("Matchday", **keys),
                     home_team=self.column_unicode("Home Team", **keys),
                     away_team=self.column_unicode("Away Team", **keys),
                     player_team=self.column_unicode("Player's Team", **keys),
                     player_name=self.column_unicode("Player", **keys),
                     starter=self.column_bool("Starting", **keys),
                     captain=self.column_bool("Captain", **keys))
                for keys in kwargs.get('data')]

    @extract
    def modifiers(self, *args, **kwargs):
        return [dict(modifier=self.column("Modifier", **keys),
                     modifier_category=self.column("Category", **keys))
                for keys in kwargs.get('data')]
