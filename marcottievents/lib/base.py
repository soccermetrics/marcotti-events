class Analytics(object):
    """
    Base class for analytics classes.
    """

    def __init__(self, session):
        self.session = session
