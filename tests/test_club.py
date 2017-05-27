# coding=utf-8
import pytest
from sqlalchemy.exc import DataError

import marcottievents.models.club as mc
import marcottievents.models.common.overview as mco
import marcottievents.models.common.personnel as mcp
import marcottievents.models.common.events as mce
import marcottievents.models.common.enums as enums


club_only = pytest.mark.skipif(
    pytest.config.getoption("--schema") != "club",
    reason="Test only valid for club databases"
)


@pytest.fixture
def club_match_lineup(session, club_data, person_data, position_data):
    match = mc.ClubLeagueMatches(matchday=15, **club_data)
    session.add(match)
    session.commit()

    club = session.query(mc.Clubs).filter(mc.Clubs.name == u"Arsenal FC").one()

    lineups = [
        mc.ClubMatchLineups(
            match_id=match.id,
            team_id=club.id,
            player=mcp.Players(**plyr),
            position=pos,
            is_starting=True,
            is_captain=False)
        for j, (plyr, pos) in enumerate(zip(person_data['player'], position_data))
        ]
    session.add_all(lineups)
    session.commit()

    return club, match, lineups


@club_only
def test_club_insert(session):
    club = mc.Clubs(name=u"Arsenal",
                    country=mco.Countries(name=u"England", confederation=enums.ConfederationType.europe))
    session.add(club)

    result = session.query(mc.Clubs).one()
    assert result.name == u"Arsenal"
    assert repr(result) == "<Club(name=Arsenal, country=England)>"


@club_only
def test_club_unicode_insert(session):
    club = mc.Clubs(name=u"Фк Спартак Москва",
                    country=mco.Countries(name=u"Russia", confederation=enums.ConfederationType.europe))
    session.add(club)

    result = session.query(mc.Clubs).join(mco.Countries).filter(mco.Countries.name == u"Russia").one()

    assert result.name == u"Фк Спартак Москва"
    assert unicode(result) == u"<Club(name=Фк Спартак Москва, country=Russia)>"


@club_only
def test_club_name_overflow(session):
    too_long_name = "blahblah" * 8
    too_long_club = mc.Clubs(name=too_long_name,
                             country=mco.Countries(name=u"foo", confederation=enums.ConfederationType.fifa))
    with pytest.raises(DataError):
        session.add(too_long_club)
        session.commit()


@club_only
def test_club_friendly_match_insert(session, club_data):
    friendly_match = mc.ClubFriendlyMatches(**club_data)
    session.add(friendly_match)

    match_from_db = session.query(mc.ClubFriendlyMatches).one()

    assert match_from_db.season.name == "2014-2015"
    assert match_from_db.competition.name == u"Test Competition"
    assert match_from_db.competition.country.name == u"England"
    assert match_from_db.venue.name == u"Emirates Stadium"
    assert match_from_db.home_team.name == u"Arsenal FC"
    assert match_from_db.away_team.name == u"Lincoln City FC"
    assert match_from_db.home_manager.full_name == u"Arsène Wenger"
    assert match_from_db.away_manager.full_name == u"Gary Simpson"
    assert match_from_db.referee.full_name == u"Mark Clattenburg"


@club_only
def test_club_league_match_insert(session, club_data):
    league_match = mc.ClubLeagueMatches(matchday=5, **club_data)
    session.add(league_match)

    match_from_db = session.query(mc.ClubLeagueMatches).join(mco.DomesticCompetitions)\
        .filter(mco.DomesticCompetitions.name == club_data['competition'].name).all()[0]

    assert match_from_db.phase == "league"
    assert match_from_db.matchday == 5
    assert match_from_db.season.name == "2014-2015"
    assert match_from_db.competition.name == u"Test Competition"
    assert match_from_db.competition.country.name == u"England"
    assert match_from_db.venue.name == u"Emirates Stadium"
    assert match_from_db.home_team.name == u"Arsenal FC"
    assert match_from_db.away_team.name == u"Lincoln City FC"
    assert match_from_db.home_manager.full_name == u"Arsène Wenger"
    assert match_from_db.away_manager.full_name == u"Gary Simpson"
    assert match_from_db.referee.full_name == u"Mark Clattenburg"


@club_only
def test_club_group_match_insert(session, club_data):
    group_match = mc.ClubGroupMatches(
        group_round=enums.GroupRoundType.group_stage,
        group='A',
        matchday=1,
        **club_data)
    session.add(group_match)

    match_from_db = session.query(mc.ClubGroupMatches).one()

    assert match_from_db.phase == "group"
    assert match_from_db.group_round.value == u"Group Stage"
    assert match_from_db.group == "A"
    assert match_from_db.matchday == 1
    assert match_from_db.season.name == "2014-2015"
    assert match_from_db.competition.name == u"Test Competition"
    assert match_from_db.competition.country.name == u"England"
    assert match_from_db.venue.name == u"Emirates Stadium"
    assert match_from_db.home_team.name == u"Arsenal FC"
    assert match_from_db.away_team.name == u"Lincoln City FC"
    assert match_from_db.home_manager.full_name == u"Arsène Wenger"
    assert match_from_db.away_manager.full_name == u"Gary Simpson"
    assert match_from_db.referee.full_name == u"Mark Clattenburg"


@club_only
def test_club_knockout_match_insert(session, club_data):
    knockout_match = mc.ClubKnockoutMatches(
        ko_round=enums.KnockoutRoundType.quarterfinal,
        **club_data)
    session.add(knockout_match)

    match_from_db = session.query(mc.ClubKnockoutMatches).filter(
        mc.ClubKnockoutMatches.ko_round == enums.KnockoutRoundType.quarterfinal)

    assert match_from_db[0].phase == "knockout"
    assert match_from_db[0].ko_round.value == u"Quarterfinal (1/4)"
    assert match_from_db[0].matchday == 1
    assert match_from_db[0].extra_time is False
    assert match_from_db[0].season.name == "2014-2015"
    assert match_from_db[0].competition.name == u"Test Competition"
    assert match_from_db[0].competition.country.name == u"England"
    assert match_from_db[0].venue.name == u"Emirates Stadium"
    assert match_from_db[0].home_team.name == u"Arsenal FC"
    assert match_from_db[0].away_team.name == u"Lincoln City FC"
    assert match_from_db[0].home_manager.full_name == u"Arsène Wenger"
    assert match_from_db[0].away_manager.full_name == u"Gary Simpson"
    assert match_from_db[0].referee.full_name == u"Mark Clattenburg"


@club_only
def test_club_match_lineup_insert(session, club_data, person_data, position_data):
    match = mc.ClubLeagueMatches(matchday=15, **club_data)
    session.add(match)
    player = mcp.Players(position=position_data[0], **person_data['player'][0])
    session.add(player)
    session.commit()

    club_from_db = session.query(mc.Clubs).filter(mc.Clubs.name == u"Arsenal FC").one()

    lineup = mc.ClubMatchLineups(
        match_id=match.id,
        team_id=club_from_db.id,
        player_id=player.player_id,
        position_id=player.position_id
    )
    session.add(lineup)

    lineup_from_db = session.query(mc.ClubMatchLineups).join(mc.ClubLeagueMatches)\
        .filter(mc.ClubLeagueMatches.id == match.id)

    assert lineup_from_db.count() == 1
    assert unicode(lineup_from_db[0]) == u"<ClubMatchLineup(match={}, player=Miguel Ángel Ponce, team=Arsenal FC, " \
                                         u"position=Left back, starter=False, captain=False)>".format(match.id)


@club_only
def test_club_match_action_insert(session, club_match_lineup):
    club, match, lineups = club_match_lineup

    events = [
        mce.MatchActions(
                event=mc.ClubMatchEvents(match_id=match.id, period=1, period_secs=0),
                type=enums.ActionType.start_period),
        mce.MatchActions(
                event=mc.ClubMatchEvents(match_id=match.id, period=1, period_secs=2902),
                type=enums.ActionType.end_period
        )
    ]
    session.add_all(events)
    session.commit()

    event_from_db = session.query(mc.ClubMatchEvents)
    assert event_from_db.count() == 2

    start_event = session.query(mce.MatchActions).filter_by(type=enums.ActionType.start_period).one()
    assert start_event.event == events[0].event
    assert start_event.lineup_id is None

    end_event = session.query(mce.MatchActions).filter_by(type=enums.ActionType.end_period).one()
    assert end_event.event == events[1].event
    assert end_event.lineup_id is None

    non_touch_events = session.query(mce.MatchActions).join(mc.ClubMatchEvents)\
        .filter(mc.ClubMatchEvents.x.is_(None), mc.ClubMatchEvents.y.is_(None))
    assert non_touch_events.count() == 2
    assert set([rec.type for rec in non_touch_events]) == {enums.ActionType.start_period, enums.ActionType.end_period}


@club_only
def test_club_match_action_insert(session, club_match_lineup):
    club, match, lineups = club_match_lineup

    events = [
        mce.MatchActions(
            event=mc.ClubMatchEvents(match_id=match.id, period=1, period_secs=0),
            type=enums.ActionType.start_period
        ),
        mce.MatchActions(
            event=mc.ClubMatchEvents(match_id=match.id, team_id=club.id, period=1, period_secs=1143, x=67.3, y=62.1),
            type=enums.ActionType.ball_pass,
            lineup_id=lineups[0].id,
            x_end=82.2,
            y_end=50.0
        ),
        mce.MatchActions(
            event=mc.ClubMatchEvents(match_id=match.id, team_id=club.id, period=1, period_secs=1150, x=84.0, y=48.8),
            type=enums.ActionType.goal,
            lineup_id=lineups[1].id,
            x_end=100.0,
            y_end=45.0,
            z_end=10.0
        )
    ]
    session.add_all(events)
    session.commit()

    pass_actions = session.query(mce.MatchActions).join(mc.ClubMatchEvents).\
        filter(mce.MatchActions.type == enums.ActionType.ball_pass)
    assert pass_actions.count() == 1

    pass_event = session.query(mc.ClubMatchEvents).get(pass_actions[0].event_id)
    assert pass_event.period == 1
    assert pass_event.period_secs == 1143
    assert pass_event.x == 67.3
    assert pass_event.y == 62.1


@club_only
def test_club_goal_event_view(session, club_match_lineup):
    club, match, lineups = club_match_lineup

    events = [
        mce.MatchActions(
            event=mc.ClubMatchEvents(match_id=match.id, period=1, period_secs=0),
            type=enums.ActionType.start_period
        ),
        mce.MatchActions(
            event=mc.ClubMatchEvents(match_id=match.id, team_id=club.id, period=1, period_secs=1143, x=67.3, y=62.1),
            type=enums.ActionType.ball_pass,
            lineup_id=lineups[0].id,
            x_end=82.2,
            y_end=50.0
        ),
        mce.MatchActions(
            event=mc.ClubMatchEvents(match_id=match.id, team_id=club.id, period=1, period_secs=1150, x=84.0, y=48.8),
            type=enums.ActionType.goal,
            lineup_id=lineups[1].id,
            x_end=100.0,
            y_end=45.0,
            z_end=10.0
        )
    ]
    session.add_all(events)
    session.commit()

    goal_events = session.query(mc.ClubGoals)
    assert goal_events.count() == 1
    assert goal_events[0].match_id == match.id
    assert goal_events[0].lineup_id == lineups[1].id
    assert goal_events[0].team_id == lineups[1].team_id
    assert goal_events[0].period == 1
    assert goal_events[0].period_secs == 1150
    assert goal_events[0].x == 84.0
    assert goal_events[0].y == 48.8


@club_only
def test_club_goal_modifier_insert(session, club_match_lineup, modifiers):
    club, match, lineups = club_match_lineup

    modifier_objs = [mce.Modifiers(type=enums.ModifierType.from_string(record['modifier']),
                                   category=enums.ModifierCategoryType.from_string(record['category']))
                     for record in modifiers]
    session.add_all(modifier_objs)
    session.commit()

    goal_action = mce.MatchActions(
            event=mc.ClubMatchEvents(match_id=match.id, team_id=club.id, period=1, period_secs=1150, x=84.0, y=48.8),
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

    action_from_db = session.query(mc.ClubGoals).filter_by(action_id=goal_action.id).one()

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


@club_only
def test_club_missed_pass_event(session, club_match_lineup, modifiers):
    club, match, lineups = club_match_lineup

    modifier_objs = [mce.Modifiers(type=enums.ModifierType.from_string(record['modifier']),
                                   category=enums.ModifierCategoryType.from_string(record['category']))
                     for record in modifiers]
    session.add_all(modifier_objs)
    session.commit()

    events = [
        mce.MatchActions(
            event=mc.ClubMatchEvents(match_id=match.id, team_id=club.id, period=1, period_secs=921, x=75.3, y=98.4),
            type=enums.ActionType.ball_pass,
            lineup_id=lineups[0].id,
            x_end=82.2,
            y_end=75.0,
        ),
        mce.MatchActions(
            event=mc.ClubMatchEvents(match_id=match.id, team_id=club.id, period=1, period_secs=1143, x=67.3, y=62.1),
            type=enums.ActionType.ball_pass,
            lineup_id=lineups[0].id,
            x_end=82.2,
            y_end=50.0,
            is_success=False
        )
    ]
    session.add_all(events)
    session.commit()

    pass_actions = session.query(mce.MatchActions).join(mc.ClubMatchEvents).\
        filter(mce.MatchActions.type == enums.ActionType.ball_pass)
    assert pass_actions.count() == 2

    missed_passes = pass_actions.filter(mce.MatchActions.is_success.in_([False]))
    assert missed_passes.count() == 1


@club_only
def test_club_penalty_shootout_opener_insert(session, club_data):
    match = mc.ClubKnockoutMatches(ko_round=enums.KnockoutRoundType.semifinal, **club_data)
    session.add(match)
    session.commit()

    result = session.query(mc.ClubKnockoutMatches.home_team_id, mc.ClubKnockoutMatches.away_team_id)\
        .filter_by(id=match.id)

    home, away = result[0]

    shootout = mc.ClubPenaltyShootoutOpeners(match_id=match.id, team_id=home)
    session.add(shootout)

    shootout_from_db = session.query(mc.ClubPenaltyShootoutOpeners)\
        .filter(mc.ClubPenaltyShootoutOpeners.match_id == match.id).one()

    assert unicode(shootout_from_db) == u"<ClubPenaltyShootoutOpener(match={}, team=Arsenal FC)>".format(match.id)
