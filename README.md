Marcotti-Events
===============

Marcotti-Events (formerly named the Football Match Event Database) is a data schema that captures historical data 
and the individual micro events that make up a football match.  Data captured include the following:

* Friendly matches and matches that make up league, knockout or hybrid (group + knockout) football competitions, and 
involve either clubs or national team selections.
* Participating personnel such as players, managers, and match officials.
* Top-level data on the football match, including match date, competition name, participating teams, venues, 
and environmental conditions.
* Every event -- touch or non-touch -- that occurs in the match, parametrized by match time, field coordinates, the 
players involved, and the player actions.
* All modifying information specific to player actions.

The Marcotti-Events data schema is made up of backend-independent SQLAlchemy objects, and club and national team 
databases are built from these objects.


## Installation

Marcotti-Events is written in Python and uses the Pandas and SQLAlchemy packages heavily.

While not required, [virtualenv](https://pypi.python.org/pypi/virtualenv) is strongly recommended and
[virtualenvwrapper](https://pypi.python.org/pypi/virtualenvwrapper) is very convenient.

Installation instructions:

1. Setup the virtual environment, and use `pip` to install the package:

        $ cd /path/to/working/dir
        $ mkvirtualenv marcotti
        (marcotti) $ pip install git+https://github.com/soccermetrics/marcotti-mls.git[@{release tag}]

2. Run the `dbsetup` command and answer the setup questions to create configuration and data loading scripts.

    ```shell
    (marcotti-mls) $ dbsetup
    #### Please answer the following questions to setup the folder ####
    Work folder (must exist): [.] /path/to/files
    Logging folder (must exist): [.] /path/to/logs
    Config file name: [local]
    Config class name: [LocalConfig]
    ```
    The command will produce three files in the working folder:

    * `local.py`: A user-defined database configuration file
    * `logging.json`: Default logging configuration file
    * `loader.py`: Data loading module
    
## Data Models

Two data schemas are created - one for clubs, the other for national teams.  There is a collection of common data 
models upon which both schemas are based, and data models specific to either schema.

The common data models are classified into four categories:

* **Overview**: High-level data about the football competition
* **Personnel**: Participants and officials in the football match
* **Match**: High-level data about the match
* **Match Events**: The micro events that occur during the football match

### Common Data Models

#### Overview

* Competitions
* Countries
* DomesticCompetitions
* InternationalCompetitions
* Seasons
* Surfaces
* Timezones
* VenueHistory
* Venues
* Years

#### Personnel

* Managers
* Persons
* Players
* PlayerHistory
* Positions
* Referees

#### Match

* MatchConditions
* Matches
* MatchLineups

#### Events

* MatchEvents
* MatchActions
* MatchActionModifiers
* Modifiers
* PenaltyShootoutOpeners

### Club-Specific Data Models

* Clubs
* ClubFriendlyMatches
* ClubGroupMatches
* ClubKnockoutMatches
* ClubLeagueMatches
* ClubMatchLineups
* ClubMatchEvents
* ClubGoals (read-only view)
* ClubPenalties (read-only view)
* ClubBookables (read-only view)
* ClubSubstitutions (read-only view)
* ClubPenaltyShootouts (read-only view)
* ClubPenaltyShootoutOpeners

### National Team-Specific Data Models

* NationalFriendlyMatches
* NationalGroupMatches
* NationalKnockoutMatches
* NationalMatchLineups
* NationalMatchEvents
* NationalGoals (read-only view)
* NationalPenalties (read-only view)
* NationalBookables (read-only view)
* NationalSubstitutions (read-only view)
* NationalPenaltyShootouts (read-only view)
* NationalPenaltyShootoutOpeners

### Enumerated Types

* ActionType
* ConfederationType
* GroupRoundType
* KnockoutRoundType
* ModifierCategoryType
* ModifierType
* NameOrderType
* PositionType
* SurfaceType
* WeatherConditionType

## Validation Data

Marcotti-Events ships with data that is used to populate the remaining validation models that can't be converted to 
enumerated types.  The data is in CSV and JSON formats. 

| Data File            | Data Model |
|:---------------------|:-----------|
| countries.{csv,json} | Countries  |
| positions.{csv,json} | Positions  |
| surfaces.{csv,json}  | Surfaces   |
| timezones.{csv,json} | Timezones  |
| modifiers.{csv,json} | Modifiers  |


## Testing

The test suite uses [py.test](http://www.pytest.org) and a PostgreSQL database.  A blank database named `test-marcotti-db` must be created before the tests are run.

Use the following command from the top-level directory of the repository to run the tests:

        $ py.test [--schema club|natl]

If the `schema` option is not passed, only the tests on common data models are run.  The `club` parameter will run the common and club-specific models, while the `natl` parameter will run tests on the common and national-team-specific models.

The tests should work for other server-based RMDBSs such as MySQL or SQL Server.  There _may_ be issues with SQLite backends, but this hasn't been confirmed.

## License

(c) 2015-2019 Soccermetrics Research, LLC. Created under MIT license.  See `LICENSE` file for details.
