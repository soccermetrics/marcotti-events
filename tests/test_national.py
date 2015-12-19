# coding=utf-8
import pytest

import models.national as mn
import models.common.overview as mco
import models.common.personnel as mcp
import models.common.events as mce
import models.common.enums as enums


natl_only = pytest.mark.skipif(
    pytest.config.getoption("--schema") != "natl",
    reason="Test only valid for national team databases"
)


@pytest.fixture
def natl_match_lineup(session, national_data, person_data, position_data):
    match = mn.NationalFriendlyMatches(**national_data)
    session.add(match)
    session.commit()

    nation = session.query(mco.Countries).filter(mco.Countries.name == u"Mexico").one()

    lineups = [
        mn.NationalMatchLineups(
            match_id=match.id,
            team_id=nation.id,
            player=mcp.Players(**plyr),
            position=pos,
            is_starting=True,
            is_captain=False)
        for j, (plyr, pos) in enumerate(zip(person_data['player'], position_data))
        ]
    session.add_all(lineups)
    session.commit()

    return nation, match, lineups


@natl_only
def test_natl_friendly_match_insert(session, national_data):
    friendly_match = mn.NationalFriendlyMatches(**national_data)
    session.add(friendly_match)

    match_from_db = session.query(mn.NationalFriendlyMatches).one()

    assert unicode(match_from_db) == u"<NationalFriendlyMatch(home=France, away=Mexico, " \
                                     u"competition=International Cup, date=1997-11-12)>"
    assert match_from_db.season.name == "1997-1998"
    assert match_from_db.competition.confederation.value == u"FIFA"
    assert match_from_db.venue.name == u"Emirates Stadium"
    assert match_from_db.home_manager.full_name == u"Arsène Wenger"
    assert match_from_db.away_manager.full_name == u"Gary Simpson"
    assert match_from_db.referee.full_name == u"Pierluigi Collina"


@natl_only
def test_natl_group_match_insert(session, national_data):
    group_match = mn.NationalGroupMatches(
        group_round=enums.GroupRoundType.first_round,
        group='C',
        matchday=2,
        **national_data
    )
    session.add(group_match)

    match_from_db = session.query(mn.NationalGroupMatches).one()

    assert unicode(match_from_db) == u"<NationalGroupMatch(home=France, away=Mexico, competition=International Cup, " \
                                     u"round=First Round, group=C, matchday=2, date=1997-11-12)>"
    assert match_from_db.season.name == "1997-1998"
    assert match_from_db.competition.confederation.value == u"FIFA"
    assert match_from_db.venue.name == u"Emirates Stadium"
    assert match_from_db.home_manager.full_name == u"Arsène Wenger"
    assert match_from_db.away_manager.full_name == u"Gary Simpson"
    assert match_from_db.referee.full_name == u"Pierluigi Collina"


@natl_only
def test_natl_knockout_match_insert(session, national_data):
    knockout_match = mn.NationalKnockoutMatches(
        ko_round=enums.KnockoutRoundType.semifinal,
        **national_data
    )
    session.add(knockout_match)

    match_from_db = session.query(mn.NationalKnockoutMatches).filter_by(ko_round=enums.KnockoutRoundType.semifinal)

    assert match_from_db.count() == 1
    assert unicode(match_from_db[0]) == u"<NationalKnockoutMatch(home=France, away=Mexico, " \
                                        u"competition=International Cup, " \
                                        u"round=Semi-Final (1/2), matchday=1, date=1997-11-12)>"
    assert match_from_db[0].season.name == "1997-1998"
    assert match_from_db[0].competition.confederation.value == u"FIFA"
    assert match_from_db[0].venue.name == u"Emirates Stadium"
    assert match_from_db[0].home_manager.full_name == u"Arsène Wenger"
    assert match_from_db[0].away_manager.full_name == u"Gary Simpson"
    assert match_from_db[0].referee.full_name == u"Pierluigi Collina"


@natl_only
def test_natl_match_lineup_insert(session, national_data, person_data, position_data):
    match = mn.NationalGroupMatches(
        group_round=enums.GroupRoundType.first_round,
        group='C',
        matchday=2,
        **national_data
    )
    session.add(match)
    session.commit()

    nation_from_db = session.query(mco.Countries).filter(mco.Countries.name == u"Mexico").one()

    player_data = person_data['player'][0]
    del player_data['country']
    player_data['country_id'] = nation_from_db.id
    player = mcp.Players(position=position_data[0], **player_data)
    session.add(player)
    session.commit()

    lineup = mn.NationalMatchLineups(
        match_id=match.id,
        team_id=nation_from_db.id,
        player_id=player.player_id,
        position_id=player.position_id
    )
    session.add(lineup)

    lineup_from_db = session.query(mn.NationalMatchLineups).join(mn.NationalGroupMatches).\
        filter(mn.NationalGroupMatches.id == match.id)

    assert lineup_from_db.count() == 1
    assert unicode(lineup_from_db[0]) == u"<NationalMatchLineup(match={}, player=Miguel Ángel Ponce, team=Mexico, " \
                                         u"position=Left back, starter=False, captain=False)>".format(match.id)


@natl_only
def test_natl_match_action_insert(session, natl_match_lineup):
    nation, match, lineups = natl_match_lineup

    events = [
        mce.MatchActions(
                event=mn.NationalMatchEvents(match_id=match.id, period=1, period_secs=0),
                type=enums.ActionType.start_period),
        mce.MatchActions(
                event=mn.NationalMatchEvents(match_id=match.id, period=1, period_secs=2902),
                type=enums.ActionType.end_period
        )
    ]
    session.add_all(events)
    session.commit()

    event_from_db = session.query(mn.NationalMatchEvents)
    assert event_from_db.count() == 2

    start_event = session.query(mce.MatchActions).filter_by(type=enums.ActionType.start_period).one()
    assert start_event.event == events[0].event
    assert start_event.lineup_id is None

    end_event = session.query(mce.MatchActions).filter_by(type=enums.ActionType.end_period).one()
    assert end_event.event == events[1].event
    assert end_event.lineup_id is None

    non_touch_events = session.query(mce.MatchActions).join(mn.NationalMatchEvents)\
        .filter(mn.NationalMatchEvents.x.is_(None), mn.NationalMatchEvents.y.is_(None))
    assert non_touch_events.count() == 2
    assert set([rec.type for rec in non_touch_events]) == {enums.ActionType.start_period, enums.ActionType.end_period}


@natl_only
def test_natl_match_action_insert(session, natl_match_lineup):
    nation, match, lineups = natl_match_lineup

    events = [
        mce.MatchActions(
            event=mn.NationalMatchEvents(match_id=match.id, period=1, period_secs=0),
            type=enums.ActionType.start_period
        ),
        mce.MatchActions(
            event=mn.NationalMatchEvents(match_id=match.id, team_id=nation.id, period=1, period_secs=1143,
                                         x=67.3, y=62.1),
            type=enums.ActionType.ball_pass,
            lineup_id=lineups[0].id,
            x_end=82.2,
            y_end=50.0
        ),
        mce.MatchActions(
            event=mn.NationalMatchEvents(match_id=match.id, team_id=nation.id, period=1, period_secs=1150,
                                         x=84.0, y=48.8),
            type=enums.ActionType.goal,
            lineup_id=lineups[1].id,
            x_end=100.0,
            y_end=45.0,
            z_end=10.0
        )
    ]
    session.add_all(events)
    session.commit()

    pass_actions = session.query(mce.MatchActions).join(mn.NationalMatchEvents).\
        filter(mce.MatchActions.type == enums.ActionType.ball_pass)
    assert pass_actions.count() == 1

    pass_event = session.query(mn.NationalMatchEvents).get(pass_actions[0].event_id)
    assert pass_event.period == 1
    assert pass_event.period_secs == 1143
    assert pass_event.x == 67.3
    assert pass_event.y == 62.1


@natl_only
def test_natl_goal_event_view(session, natl_match_lineup):
    nation, match, lineups = natl_match_lineup

    events = [
        mce.MatchActions(
            event=mn.NationalMatchEvents(match_id=match.id, period=1, period_secs=0),
            type=enums.ActionType.start_period
        ),
        mce.MatchActions(
            event=mn.NationalMatchEvents(match_id=match.id, team_id=nation.id, period=1, period_secs=1143,
                                         x=67.3, y=62.1),
            type=enums.ActionType.ball_pass,
            lineup_id=lineups[0].id,
            x_end=82.2,
            y_end=50.0
        ),
        mce.MatchActions(
            event=mn.NationalMatchEvents(match_id=match.id, team_id=nation.id, period=1, period_secs=1150,
                                         x=84.0, y=48.8),
            type=enums.ActionType.goal,
            lineup_id=lineups[1].id,
            x_end=100.0,
            y_end=45.0,
            z_end=10.0
        )
    ]
    session.add_all(events)
    session.commit()

    goal_events = session.query(mn.NationalGoals)
    assert goal_events.count() == 1
    assert goal_events[0].match_id == match.id
    assert goal_events[0].lineup_id == lineups[1].id
    assert goal_events[0].team_id == lineups[1].team_id
    assert goal_events[0].period == 1
    assert goal_events[0].period_secs == 1150
    assert goal_events[0].x == 84.0
    assert goal_events[0].y == 48.8


@natl_only
def test_natl_goal_modifier_insert(session, natl_match_lineup, modifiers):
    nation, match, lineups = natl_match_lineup

    modifier_objs = [mce.Modifiers(type=enums.ModifierType.from_string(record['modifier']),
                                   category=enums.ModifierCategoryType.from_string(record['category']))
                     for record in modifiers]
    session.add_all(modifier_objs)
    session.commit()

    goal_action = mce.MatchActions(
            event=mn.NationalMatchEvents(match_id=match.id, team_id=nation.id, period=1, period_secs=1150,
                                         x=84.0, y=48.8),
            type=enums.ActionType.goal,
            lineup_id=lineups[1].id,
            x_end=100.0,
            y_end=45.0,
            z_end=10.0
    )
    session.add(goal_action)
    session.commit()

    goal_modifiers = [
        mce.MatchActionModifiers(
            action_id=goal_action.id,
            modifier_id=session.query(mce.Modifiers.id).filter_by(type=enums.ModifierType.head).scalar()
        ),
        mce.MatchActionModifiers(
            action_id=goal_action.id,
            modifier_id=session.query(mce.Modifiers.id).filter_by(type=enums.ModifierType.center_penalty_area).scalar()
        ),
        mce.MatchActionModifiers(
            action_id=goal_action.id,
            modifier_id=session.query(mce.Modifiers.id).filter_by(type=enums.ModifierType.lower_right).scalar()
        )
    ]
    session.add_all(goal_modifiers)
    session.commit()

    action_from_db = session.query(mn.NationalGoals).filter_by(action_id=goal_action.id).one()

    goal_body_part = session.query(mce.Modifiers).join(mce.MatchActionModifiers)\
        .filter(mce.MatchActionModifiers.action_id == action_from_db.action_id,
                mce.Modifiers.category == enums.ModifierCategoryType.bodypart)
    assert goal_body_part.count() == 1
    assert goal_body_part[0].type.value == "Head"

    goal_sector = session.query(mce.Modifiers).join(mce.MatchActionModifiers)\
        .filter(mce.MatchActionModifiers.action_id == action_from_db.action_id,
                mce.Modifiers.category == enums.ModifierCategoryType.field_sector)
    assert goal_sector.count() == 1
    assert goal_sector[0].type.value == "Central Penalty Area"

    goal_goal_region = session.query(mce.Modifiers).join(mce.MatchActionModifiers)\
        .filter(mce.MatchActionModifiers.action_id == action_from_db.action_id,
                mce.Modifiers.category == enums.ModifierCategoryType.goal_region)
    assert goal_goal_region.count() == 1
    assert goal_goal_region[0].type.value == "Lower Right"


@natl_only
def test_natl_match_action_insert(session, natl_match_lineup):
    nation, match, lineups = natl_match_lineup

    events = [
        mce.MatchActions(
            event=mn.NationalMatchEvents(match_id=match.id, period=1, period_secs=0),
            type=enums.ActionType.start_period
        ),
        mce.MatchActions(
            event=mn.NationalMatchEvents(match_id=match.id, team_id=nation.id, period=1, period_secs=1143,
                                         x=67.3, y=62.1),
            type=enums.ActionType.ball_pass,
            lineup_id=lineups[0].id,
            x_end=82.2,
            y_end=50.0
        ),
        mce.MatchActions(
            event=mn.NationalMatchEvents(match_id=match.id, team_id=nation.id, period=1, period_secs=1150,
                                         x=84.0, y=48.8),
            type=enums.ActionType.goal,
            lineup_id=lineups[1].id,
            x_end=100.0,
            y_end=45.0,
            z_end=10.0
        )
    ]
    session.add_all(events)
    session.commit()

    pass_actions = session.query(mce.MatchActions).join(mn.NationalMatchEvents).\
        filter(mce.MatchActions.type == enums.ActionType.ball_pass)
    assert pass_actions.count() == 1

    pass_event = session.query(mn.NationalMatchEvents).get(pass_actions[0].event_id)
    assert pass_event.period == 1
    assert pass_event.period_secs == 1143
    assert pass_event.x == 67.3
    assert pass_event.y == 62.1


@natl_only
def test_natl_goal_event_view(session, natl_match_lineup):
    nation, match, lineups = natl_match_lineup

    events = [
        mce.MatchActions(
            event=mn.NationalMatchEvents(match_id=match.id, period=1, period_secs=0),
            type=enums.ActionType.start_period
        ),
        mce.MatchActions(
            event=mn.NationalMatchEvents(match_id=match.id, team_id=nation.id, period=1, period_secs=1143,
                                         x=67.3, y=62.1),
            type=enums.ActionType.ball_pass,
            lineup_id=lineups[0].id,
            x_end=82.2,
            y_end=50.0
        ),
        mce.MatchActions(
            event=mn.NationalMatchEvents(match_id=match.id, team_id=nation.id, period=1, period_secs=1150,
                                         x=84.0, y=48.8),
            type=enums.ActionType.goal,
            lineup_id=lineups[1].id,
            x_end=100.0,
            y_end=45.0,
            z_end=10.0
        )
    ]
    session.add_all(events)
    session.commit()

    goal_events = session.query(mn.NationalGoals)
    assert goal_events.count() == 1
    assert goal_events[0].match_id == match.id
    assert goal_events[0].lineup_id == lineups[1].id
    assert goal_events[0].team_id == lineups[1].team_id
    assert goal_events[0].period == 1
    assert goal_events[0].period_secs == 1150
    assert goal_events[0].x == 84.0
    assert goal_events[0].y == 48.8


@natl_only
def test_natl_goal_modifier_insert(session, natl_match_lineup, modifiers):
    nation, match, lineups = natl_match_lineup

    modifier_objs = [mce.Modifiers(type=enums.ModifierType.from_string(record['modifier']),
                                   category=enums.ModifierCategoryType.from_string(record['category']))
                     for record in modifiers]
    session.add_all(modifier_objs)
    session.commit()

    goal_action = mce.MatchActions(
            event=mn.NationalMatchEvents(match_id=match.id, team_id=nation.id, period=1, period_secs=1150,
                                         x=84.0, y=48.8),
            type=enums.ActionType.goal,
            lineup_id=lineups[1].id,
            x_end=100.0,
            y_end=45.0,
            z_end=10.0
    )
    session.add(goal_action)
    session.commit()

    goal_modifiers = [
        mce.MatchActionModifiers(
            action_id=goal_action.id,
            modifier_id=session.query(mce.Modifiers.id).filter_by(type=enums.ModifierType.head).scalar()
        ),
        mce.MatchActionModifiers(
            action_id=goal_action.id,
            modifier_id=session.query(mce.Modifiers.id).filter_by(type=enums.ModifierType.center_penalty_area).scalar()
        ),
        mce.MatchActionModifiers(
            action_id=goal_action.id,
            modifier_id=session.query(mce.Modifiers.id).filter_by(type=enums.ModifierType.lower_right).scalar()
        )
    ]
    session.add_all(goal_modifiers)
    session.commit()

    action_from_db = session.query(mn.NationalGoals).filter_by(action_id=goal_action.id).one()

    goal_body_part = session.query(mce.Modifiers).join(mce.MatchActionModifiers)\
        .filter(mce.MatchActionModifiers.action_id == action_from_db.action_id,
                mce.Modifiers.category == enums.ModifierCategoryType.bodypart)
    assert goal_body_part.count() == 1
    assert goal_body_part[0].type.value == "Head"

    goal_sector = session.query(mce.Modifiers).join(mce.MatchActionModifiers)\
        .filter(mce.MatchActionModifiers.action_id == action_from_db.action_id,
                mce.Modifiers.category == enums.ModifierCategoryType.field_sector)
    assert goal_sector.count() == 1
    assert goal_sector[0].type.value == "Central Penalty Area"

    goal_goal_region = session.query(mce.Modifiers).join(mce.MatchActionModifiers)\
        .filter(mce.MatchActionModifiers.action_id == action_from_db.action_id,
                mce.Modifiers.category == enums.ModifierCategoryType.goal_region)
    assert goal_goal_region.count() == 1
    assert goal_goal_region[0].type.value == "Lower Right"


@natl_only
def test_natl_missed_pass_event(session, natl_match_lineup, modifiers):
    nation, match, lineups = natl_match_lineup

    modifier_objs = [mce.Modifiers(type=enums.ModifierType.from_string(record['modifier']),
                                   category=enums.ModifierCategoryType.from_string(record['category']))
                     for record in modifiers]
    session.add_all(modifier_objs)
    session.commit()

    events = [
        mce.MatchActions(
            event=mn.NationalMatchEvents(match_id=match.id, team_id=nation.id, period=1, period_secs=921,
                                         x=75.3, y=98.4),
            type=enums.ActionType.ball_pass,
            lineup_id=lineups[0].id,
            x_end=82.2,
            y_end=75.0,
        ),
        mce.MatchActions(
            event=mn.NationalMatchEvents(match_id=match.id, team_id=nation.id, period=1, period_secs=1143,
                                         x=67.3, y=62.1),
            type=enums.ActionType.ball_pass,
            lineup_id=lineups[0].id,
            x_end=82.2,
            y_end=50.0,
            is_success=False
        )
    ]
    session.add_all(events)
    session.commit()

    pass_actions = session.query(mce.MatchActions).join(mn.NationalMatchEvents).\
        filter(mce.MatchActions.type == enums.ActionType.ball_pass)
    assert pass_actions.count() == 2

    missed_passes = pass_actions.filter(mce.MatchActions.is_success.in_([False]))
    assert missed_passes.count() == 1

    missed_pass_event = session.query(mn.NationalMatchEvents).get(missed_passes[0].event_id)
    assert missed_pass_event.period_secs == 1143
    assert missed_pass_event.x == 67.3
    assert missed_pass_event.y == 62.1
    assert missed_pass_event.team_id == nation.id
    assert missed_passes[0].lineup_id == lineups[0].id


@natl_only
def test_natl_penalty_shootout_opener_insert(session, national_data):
    match = mn.NationalKnockoutMatches(
        ko_round=enums.KnockoutRoundType.final,
        **national_data
    )
    session.add(match)
    session.commit()

    result = session.query(mn.NationalKnockoutMatches.home_team_id, mn.NationalKnockoutMatches.away_team_id)\
        .filter_by(id=match.id)

    home, away = result[0]

    shootout = mn.NationalPenaltyShootoutOpeners(match_id=match.id, team_id=away)
    session.add(shootout)

    shootout_from_db = session.query(mn.NationalPenaltyShootoutOpeners)\
        .filter(mn.NationalPenaltyShootoutOpeners.match_id == match.id).one()

    assert unicode(shootout_from_db) == u"<NationalPenaltyShootoutOpener(match={}, team=Mexico)>".format(match.id)
