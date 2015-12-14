import uuid

from sqlalchemy import Column, Boolean, Integer, Numeric, String, Sequence, ForeignKey, select, join, text
from sqlalchemy.schema import CheckConstraint
from sqlalchemy.orm import relationship, backref

from models import GUID, view
from models.common import BaseSchema
import models.common.enums as enums


class MatchEvents(BaseSchema):
    __tablename__ = "match_events"

    id = Column(GUID, primary_key=True, default=uuid.uuid4)

    domain = Column(String)
    period = Column(Integer, CheckConstraint('period >= 1 AND period <= 4'))
    period_secs = Column(Integer)
    x = Column(Numeric(4, 1, asdecimal=False, decimal_return_scale=1), nullable=True)
    y = Column(Numeric(4, 1, asdecimal=False, decimal_return_scale=1), nullable=True)

    match_id = Column(GUID, ForeignKey('matches.id'))
    match = relationship('Matches', backref=backref('events'))

    __mapper_args__ = {
        'polymorphic_identity': 'goals',
        'polymorphic_on': domain
    }


class MatchActions(BaseSchema):
    __tablename__ = 'match_actions'

    id = Column(GUID, primary_key=True, default=uuid.uuid4)

    type = Column(enums.ActionType.db_type())
    x_end = Column(Numeric(4, 1, asdecimal=False, decimal_return_scale=1), nullable=True)
    y_end = Column(Numeric(4, 1, asdecimal=False, decimal_return_scale=1), nullable=True)
    z_end = Column(Numeric(4, 1, asdecimal=False, decimal_return_scale=1), nullable=True)
    is_success = Column(Boolean, default=True)

    event_id = Column(GUID, ForeignKey('match_events.id'), nullable=False)
    lineup_id = Column(GUID, ForeignKey('lineups.id'))

    event = relationship('MatchEvents', backref=backref('actions'))
    lineup = relationship('MatchLineups', backref=backref('actions'))


class MatchActionModifiers(BaseSchema):
    __tablename__ = 'action_modifiers'

    id = Column(GUID, primary_key=True, default=uuid.uuid4)

    action_id = Column(GUID, ForeignKey('match_actions.id'))
    modifier_id = Column(Integer, ForeignKey('modifiers.id'))

    action = relationship('MatchActions', backref=backref('modifiers'))
    modifier = relationship('Modifiers')


class Modifiers(BaseSchema):
    __tablename__ = 'modifiers'

    id = Column(Integer, Sequence('modifier_id_seq', start=100), primary_key=True)

    type = Column(enums.ModifierType.db_type())
    category = Column(enums.ModifierCategoryType.db_type())


class PenaltyShootoutOpeners(BaseSchema):
    __tablename__ = "penalty_shootout_openers"

    match_id = Column(GUID, ForeignKey('matches.id'), primary_key=True)
    domain = Column(String)

    __mapper_args__ = {
        'polymorphic_identity': 'penalty_shootout_openers',
        'polymorphic_on': domain
    }
