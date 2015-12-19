from copy import deepcopy

from sqlalchemy import Column, ForeignKey, select, join, text
from sqlalchemy.orm import relationship, backref
from sqlalchemy.sql.expression import label
from sqlalchemy.ext.declarative import declared_attr, declarative_base

from models import GUID, view
from models.common import BaseSchema
import models.common.overview as mco
import models.common.personnel as mcp
import models.common.match as mcm
import models.common.events as mce


NatlSchema = declarative_base(name="National Teams", metadata=BaseSchema.metadata,
                              class_registry=deepcopy(BaseSchema._decl_class_registry))


class NationalMixin(object):

    @declared_attr
    def team_id(cls):
        return Column(GUID, ForeignKey('countries.id'))


class NationalMatchMixin(object):

    @declared_attr
    def home_team_id(cls):
        return Column(GUID, ForeignKey('countries.id'))

    @declared_attr
    def away_team_id(cls):
        return Column(GUID, ForeignKey('countries.id'))


class FriendlyMixin(object):

    @declared_attr
    def home_team(cls):
        return relationship('Countries', foreign_keys="{}.home_team_id".format(cls.__name__),
                            backref=backref('home_friendly_matches'))

    @declared_attr
    def away_team(cls):
        return relationship('Countries', foreign_keys="{}.away_team_id".format(cls.__name__),
                            backref=backref('away_friendly_matches'))


class GroupMixin(object):

    @declared_attr
    def home_team(cls):
        return relationship('Countries', foreign_keys="{}.home_team_id".format(cls.__name__),
                            backref=backref('home_group_matches'))

    @declared_attr
    def away_team(cls):
        return relationship('Countries', foreign_keys="{}.away_team_id".format(cls.__name__),
                            backref=backref('away_group_matches'))


class KnockoutMixin(object):

    @declared_attr
    def home_team(cls):
        return relationship('Countries', foreign_keys="{}.home_team_id".format(cls.__name__),
                            backref=backref('home_knockout_matches'))

    @declared_attr
    def away_team(cls):
        return relationship('Countries', foreign_keys="{}.away_team_id".format(cls.__name__),
                            backref=backref('away_knockout_matches'))


class NationalFriendlyMatches(FriendlyMixin, NationalMatchMixin, NatlSchema, mcm.Matches):
    __tablename__ = "natl_friendly_matches"
    __mapper_args__ = {'polymorphic_identity': 'friendly'}

    id = Column(GUID, ForeignKey('matches.id'), primary_key=True)

    def __repr__(self):
        return u"<NationalFriendlyMatch(home={}, away={}, competition={}, date={})>".format(
            self.home_team.name, self.away_team.name, self.competition.name, self.date.isoformat()
        ).encode('utf-8')

    def __unicode__(self):
        return u"<NationalFriendlyMatch(home={}, away={}, competition={}, date={})>".format(
            self.home_team.name, self.away_team.name, self.competition.name, self.date.isoformat()
        )


class NationalGroupMatches(GroupMixin, NationalMatchMixin, NatlSchema, mcm.GroupMatches, mcm.Matches):
    __tablename__ = "natl_group_matches"
    __mapper_args__ = {'polymorphic_identity': 'group'}

    id = Column(GUID, ForeignKey('matches.id'), primary_key=True)

    def __repr__(self):
        return u"<NationalGroupMatch(home={}, away={}, competition={}, round={}, group={}, matchday={}, date={})>".format(
            self.home_team.name, self.away_team.name, self.competition.name, self.group_round.value,
            self.group, self.matchday, self.date.isoformat()
        ).encode('utf-8')

    def __unicode__(self):
        return u"<NationalGroupMatch(home={}, away={}, competition={}, round={}, group={}, matchday={}, date={})>".format(
            self.home_team.name, self.away_team.name, self.competition.name, self.group_round.value,
            self.group, self.matchday, self.date.isoformat()
        )


class NationalKnockoutMatches(KnockoutMixin, NationalMatchMixin, NatlSchema, mcm.KnockoutMatches, mcm.Matches):
    __tablename__ = "natl_knockout_matches"
    __mapper_args__ = {'polymorphic_identity': 'knockout'}

    id = Column(GUID, ForeignKey('matches.id'), primary_key=True)

    def __repr__(self):
        return u"<NationalKnockoutMatch(home={}, away={}, competition={}, round={}, matchday={}, date={})>".format(
            self.home_team.name, self.away_team.name, self.competition.name,
            self.ko_round.value, self.matchday, self.date.isoformat()
        ).encode('utf-8')

    def __unicode__(self):
        return u"<NationalKnockoutMatch(home={}, away={}, competition={}, round={}, matchday={}, date={})>".format(
            self.home_team.name, self.away_team.name, self.competition.name,
            self.ko_round.value, self.matchday, self.date.isoformat()
        )


class NationalMatchLineups(NationalMixin, NatlSchema, mcm.MatchLineups):
    __tablename__ = "natl_match_lineups"
    __mapper_args__ = {'polymorphic_identity': 'national'}

    id = Column(GUID, ForeignKey('lineups.id'), primary_key=True)

    team = relationship('Countries', foreign_keys="NationalMatchLineups.team_id", backref=backref("lineups"))

    def __repr__(self):
        return u"<NationalMatchLineup(match={}, player={}, team={}, position={}, starter={}, captain={})>".format(
            self.match_id, self.full_name, self.team.name, self.position.name, self.is_starting, self.is_captain
        ).encode('utf-8')

    def __unicode__(self):
        return u"<NationalMatchLineup(match={}, player={}, team={}, position={}, starter={}, captain={})>".format(
            self.match_id, self.full_name, self.team.name, self.position.name, self.is_starting, self.is_captain
        )


class NationalMatchEvents(NationalMixin, NatlSchema, mce.MatchEvents):
    __tablename__ = 'natl_match_events'
    __mapper_args__ = {'polymorphic_identity': 'national'}

    id = Column(GUID, ForeignKey('match_events.id'), primary_key=True)

    team = relationship('Countries', foreign_keys="NationalMatchEvents.team_id", backref=backref("match_events"))


class NationalPenaltyShootoutOpeners(NationalMixin, NatlSchema, mce.PenaltyShootoutOpeners):
    __tablename__ = 'natl_penalty_shootout_openers'
    __mapper_args__ = {'polymorphic_identity': 'national'}

    match_id = Column(GUID, ForeignKey('penalty_shootout_openers.match_id'), primary_key=True)

    team = relationship('Countries', foreign_keys="NationalPenaltyShootoutOpeners.team_id",
                        backref=backref("shootout_openers"))

    def __repr__(self):
        return u"<NationalPenaltyShootoutOpener(match={}, team={})>".format(
            self.match_id, self.team.name).decode('utf-8')

    def __unicode__(self):
        return u"<NationalPenaltyShootoutOpener(match={}, team={})>".format(
            self.match_id, self.team.name)


goals_view = view("natl_goals_view", BaseSchema.metadata,
                  select([NationalMatchEvents.id, label('action_id', mce.MatchActions.id),
                          NationalMatchEvents.match_id, NationalMatchEvents.team_id, NationalMatchEvents.period,
                          NationalMatchEvents.period_secs, mce.MatchActions.lineup_id,
                          NationalMatchEvents.x, NationalMatchEvents.y]).
                  select_from(join(NationalMatchEvents, mce.MatchActions)).
                  where(mce.MatchActions.type == text("'Goal'")))


penalty_view = view("natl_penalty_view", BaseSchema.metadata,
                    select([NationalMatchEvents.id, NationalMatchEvents.match_id, NationalMatchEvents.team_id,
                            NationalMatchEvents.period, NationalMatchEvents.period_secs, mce.MatchActions.lineup_id]).
                    select_from(join(NationalMatchEvents, mce.MatchActions)).
                    where(mce.MatchActions.type == text("'Penalty'")))


booking_view = view("natl_booking_view", BaseSchema.metadata,
                    select([NationalMatchEvents.id, NationalMatchEvents.match_id, NationalMatchEvents.team_id,
                            NationalMatchEvents.period, NationalMatchEvents.period_secs, mce.MatchActions.lineup_id]).
                    select_from(join(NationalMatchEvents, mce.MatchActions)).
                    where(mce.MatchActions.type == text("'Card'")))


subs_view = view("natl_subs_view", BaseSchema.metadata,
                 select([NationalMatchEvents.id, NationalMatchEvents.match_id, NationalMatchEvents.team_id,
                         NationalMatchEvents.period, NationalMatchEvents.period_secs, mce.MatchActions.lineup_id]).
                 select_from(join(NationalMatchEvents, mce.MatchActions)).
                 where(mce.MatchActions.type == text("'Substitution'")))


shootout_view = view("natl_shootout_view", BaseSchema.metadata,
                     select([NationalMatchEvents.id, NationalMatchEvents.match_id, NationalMatchEvents.team_id,
                             NationalMatchEvents.period, NationalMatchEvents.period_secs, mce.MatchActions.lineup_id]).
                     select_from(join(NationalMatchEvents, mce.MatchActions)).
                     where(mce.MatchActions.type == text("'Shootout Penalty'")))


class NationalGoals(BaseSchema):
    __table__ = goals_view


class NationalPenalties(BaseSchema):
    __table__ = penalty_view


class NationalBookables(BaseSchema):
    __table__ = booking_view


class NationalSubstitutions(BaseSchema):
    __table__ = subs_view


class NationalPenaltyShootouts(BaseSchema):
    __table__ = shootout_view
