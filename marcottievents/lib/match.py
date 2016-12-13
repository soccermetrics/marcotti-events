from numpy import mean
from sqlalchemy.sql.expression import false

from .base import Analytics
import marcottievents.models.common.events as mce
import marcottievents.models.common.suppliers as mcs
import marcottievents.models.common.enums as enums


def coroutine(func):
    """Coroutine decorator: instantiate object, proceed to first yield statement.

    :param func: coroutine function
    :returns: cr
    :rtype: coroutine object
    """
    def start(*args, **kwargs):
        cr = func(*args, **kwargs)
        cr.next()
        return cr
    return start


def calc_interval(start, end):
    """Time interval between two micro-events"""
    return end.secs - start.secs


@coroutine
def int_receiver():
    """Compute elapsed match time in interval of continuous play.

    Yield variable is a tuple *(start,end)* where *start* and *end*
    contain *(period,seconds)*.

    +----------+----------------------+
    | Variable | Description          |
    +==========+======================+
    | period   | Match period         |
    +----------+----------------------+
    | seconds  | Time in match period |
    +----------+----------------------+

    """
    effective = {}
    try:
        while True:
            interval = (yield)
            (start, end) = interval
            eff_secs = effective.get(str(start.period), {'match': start.match, 'secs': 0})
            eff_secs['secs'] += calc_interval(start, end)
            effective[str(start.period)] = eff_secs
    except GeneratorExit:
        with open('effective.csv', 'a') as f:
            for key in effective:
                f.write("{},{},{}\n".format(effective[key]['match'], key, effective[key]['secs']))


@coroutine
def parse_possessions_alt(interval_pipe=None, pause_pipe=None, poss_pipe=None):
    """Implements a state machine that models continuous play and stoppages in a
        football match for which ball-out events are not recorded.  The match has the
        following states:

        - MATCH STOPPED: Match period has ended or not yet started
        - PLAY STARTED: Continuous match play has started
        - PLAY STOPPED: Continuous match play has stopped
        - PLAY RESTARTED: Continuous match play has restarted after a stoppage
        - POSSESSION CHANGE: Match possession has changed from one team to another

        Upon transition into these states, the following processors are called:

        - PLAY STARTED:
            * Match pause processor
        - POSSESSION CHANGE:
            * Possession processor
        - PLAY STOPPED:
            * Effective play processor
        - PLAY RESTARTED:
            * Effective play processor
            * Match pause processor
        - MATCH STOPPED:
            * Effective play processor
            * Possession processor

        The resulting information is extracted from the match model:

        * effective match time
        * quantity, type, and duration of pauses
        * summary data of each possession

        :param interval_pipe: Effective play processor.
        :param pause_pipe: Match pause processor.
        :param poss_pipe: Possession processor.
    """
    START_EVENT = [enums.ActionType.start_period]
    STOP_EVENTS = [getattr(enums.ActionType, event) 
                   for event in ['ball_out', 'foul', 'offside', 'card', 'goal', 'substitution', 'stopped']]
    RESTART_EVENTS = [getattr(enums.ActionType, event)
                      for event in ['throwin', 'corner_kick', 'free_kick', 'goal_kick', 'ball_pass']]
    # GOAL_EVENT = enums.ActionType.goal
    END_EVENT = [enums.ActionType.end_period]
    SUB_EVENT = [enums.ActionType.substitution]

    # Outer loop (STATE: match stopped)
    while True:
        event = (yield)
        # poss = []  # possession structure
        # chain = []  # current chain of possession
        # curr = event['team']  # current team in possession

        if event.action in START_EVENT:
            # STATE: play started
            start = event
            while True:
                # if event['team'] != curr:
                #     # STATE: possession change
                #     curr = event['team']
                #     if chain:
                #         poss.append(chain)
                #     if poss_pipe:
                #         poss_pipe.send(poss)
                #     poss = []
                #     chain = []
                #
                # # add current event to possession chain
                # chain.append(event)

                # set previous event to current event
                prev = event

                # get next event
                event = (yield)

                if event.action in RESTART_EVENTS:
                    # STATE: match restarted (only used if not Ball Out events)
                    pause_start = prev
                    # pause_start.action = event.action
                    end = event
                    if start.secs < prev.secs:
                        if interval_pipe and start: 
                            interval_pipe.send((start, prev))
                        start = event

                        # if prev not in STOP_EVENTS:
                        #     prev['action'] = end['action']
                        # (pause_start, pause_end) = (prev, end)
                        if pause_start:
                            pause_end = end
                        if pause_pipe: 
                            pause_pipe.send((pause_start, pause_end))
                        pause_start = ()

                if event.action in STOP_EVENTS:
                    # STATE: play stopped
                    end = event
                    # if event['action'] in GOAL_EVENT:
                    #     chain.append(event)
                    if event.action not in END_EVENT:
                        pause_start = end
                    if interval_pipe and start: 
                        interval_pipe.send((start, end))
                    start = ()

                    # poss.append(chain)
                    while True:
                        prev = event
                        event = (yield)
                        if event.action in SUB_EVENT:
                            # used to ID stoppages where substitutions take place
                            if pause_start: 
                                pause_start = event
                        if event.action in RESTART_EVENTS:
                            # STATE: play restarted (if no ball-out events present)
                            start = event
                            if pause_start:
                                pause_end = start
                                if pause_pipe: 
                                    pause_pipe.send((pause_start, pause_end))
                                pause_start = ()
                            # chain = []
                            break
                        elif event.action in END_EVENT:
                            # STATE: match stopped
                            # poss.append(chain)
                            # if poss_pipe:
                            #     poss_pipe.send(poss)
                            pause_start = ()
                            # poss = []
                            # chain = []
                            break
                        elif event.action not in STOP_EVENTS:
                            # STATE: play started
                            start = event
                            if pause_start:
                                pause_end = start
                                if pause_pipe: 
                                    pause_pipe.send((pause_start, pause_end))
                                pause_start = ()
                            # chain = []
                            break
                elif event.action in END_EVENT:
                    # STATE: match stopped
                    end = event
                    if interval_pipe and start: 
                        interval_pipe.send((start, end))
                    start = ()
                    end = ()

                    # STATE: possession end
                    # poss.append(chain)
                    # if poss_pipe:
                    #     poss_pipe.send(poss)
                    pause_start = ()
                    # poss = []
                    # chain = []

                    while True:
                        event = (yield)
                        if event.action in START_EVENT:
                            # STATE: play started
                            start = event
                            # curr = event['team']
                            break


class MatchAnalytics(Analytics):

    IGNORED_EVENTS = [getattr(enums.ActionType, event) for event in ['save', 'shootout', 'penalty', 'assist']]
    STOP_EVENTS = [getattr(enums.ActionType, event)
                   for event in ['throwin', 'ball_out', 'foul', 'offside', 'goal', 'substitution', 'stopped']]

    def match_length(self, match_id):
        records = self.session.query(mce.MatchEvents).join(mce.MatchActions).filter(
            mce.MatchActions.type == enums.ActionType.end_period,
            mce.MatchEvents.match_id == match_id).all()
        return [(record.period, record.period_secs) for record in records]

    def foul_times(self, match_id, period):
        foul_records = self.session.query(mce.MatchEvents).join(mce.MatchActions).filter(
            mce.MatchActions.type == enums.ActionType.foul,
            mce.MatchActions.is_success == false(),
            mce.MatchEvents.match_id == match_id,
            mce.MatchEvents.period == period)
        return sorted([record.period_secs for record in foul_records])

    def stoppage_times(self, match_id, period):
        stoppage_records = self.session.query(mce.MatchEvents).join(mce.MatchActions).filter(
            mce.MatchActions.type.in_(MatchAnalytics.STOP_EVENTS),
            mce.MatchEvents.match_id == match_id,
            mce.MatchEvents.period == period)
        return sorted(list(set([record.period_secs for record in stoppage_records])))

    def mean_time_between_fouls(self, match_id, period):
        time_of_fouls = self.foul_times(match_id, period)
        time_between_fouls = [y - x for (x, y) in zip(time_of_fouls[:-1], time_of_fouls[1:])]
        return mean(time_between_fouls)

    def mean_time_between_stoppages(self, match_id, period):
        time_of_stoppages = self.stoppage_times(match_id, period)
        time_between_stoppages = [y - x for (x, y) in zip(time_of_stoppages[:-1], time_of_stoppages[1:])]
        return mean(time_between_stoppages)

    def calc_effective_time(self, match_id, period):
        match_events = self.session.query(mce.MatchEvents.period,
                                          mce.MatchEvents.period_secs.label('secs'),
                                          mce.MatchActions.type.label('action'),
                                          mcs.MatchMap.remote_id.label('match')).join(
            mce.MatchActions).filter(
            mce.MatchEvents.match_id == match_id,
            mce.MatchEvents.period == period,
            mcs.MatchMap.id == mce.MatchEvents.match_id
        ).order_by(mce.MatchEvents.period_secs)
        c = parse_possessions_alt(interval_pipe=int_receiver())
        for event in match_events:
            c.send(event)
        c.close()
