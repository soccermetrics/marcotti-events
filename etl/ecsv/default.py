from .base import BaseCSV, extract


class CSVExtractor(BaseCSV):

    @extract
    def countries(self, *args, **kwargs):
        return [dict(remote_id=self.column("ID", **keys),
                     name=self.column_unicode("Name", **keys),
                     confederation=self.column("Confederation", **keys))
                for keys in kwargs.get('data')]

    @extract
    def competitions(self, *args, **kwargs):
        return [dict(remote_id=self.column("ID", **keys),
                     name=self.column_unicode("Name", **keys),
                     level=self.column_int("Level", **keys),
                     country=self.column_unicode("Country", **keys),
                     confederation=self.column("Confederation", **keys))
                for keys in kwargs.get('data')]

    @extract
    def venues(self, *args, **kwargs):
        return [dict(remote_id=self.column("ID", **keys),
                     name=self.column_unicode("Venue Name", **keys),
                     city=self.column_unicode("City", **keys),
                     region=self.column_unicode("Region", **keys),
                     country=self.column_unicode("Country", **keys),
                     timezone=self.column("Timezone", **keys),
                     latitude=self.column_float("Latitude", **keys),
                     longitude=self.column_float("Longitude", **keys),
                     altitude=self.column_int("Altitude", **keys),
                     surface=self.column("Surface", **keys),
                     length=self.column_int("Length", **keys),
                     width=self.column_int("Width", **keys),
                     capacity=self.column_int("Capacity", **keys),
                     seats=self.column_int("Seats", **keys))
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
                     name_order=self.column("Name Order", **keys),
                     birth_date=self.column("Birthdate", **keys),
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
                     name_order=self.column("Name Order", **keys),
                     birth_date=self.column("Birthdate", **keys),
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
                     name_order=self.column("Name Order", **keys),
                     birth_date=self.column("Birthdate", **keys),
                     country=self.column_unicode("Country", **keys))
                for keys in kwargs.get('data')]
