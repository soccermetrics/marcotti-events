from marcottievents.models import DeclEnum


class GroupRoundType(DeclEnum):
    """
    Enumerated names of rounds in group stages of football competitions.
    """
    group_stage = "Group Stage", "Group Stage"
    first_round = "First Round", "First Round"
    second_round = "Second Round", "Second Round"
    third_round = "Third Round", "Third Round"
    fourth_round = "Fourth Round", "Fourth Round"
    final_round = "Final Round", "Final Round"
    playoff = "Playoff Group", "Playoff Group"
    championship = "Championship Group", "Championship Group"
    promotion = "Promotion Group", "Promotion Group"
    relegation = "Relegation Group", "Relegation Group"


class KnockoutRoundType(DeclEnum):
    """
    Enumerated names of rounds in knockout stages of football competitions.
    """
    extra_prelim = "Extra Preliminary Round", "Extra Preliminary Round"
    prelim = "Preliminary Round", "Preliminary Round"
    first_qualifying = "First Qualifying Round", "First Qualifying Round"
    second_qualifying = "Second Qualifying Round", "Second Qualifying Round"
    third_qualifying = "Third Qualifying Round", "Third Qualifying Round"
    fourth_qualifying = "Fourth Qualifying Round", "Fourth Qualifying Round"
    playoff = "Playoff Round", "Playoff Round"
    first_round = "First Round", "First Round"
    second_round = "Second Round", "Second Round"
    third_round = "Third Round", "Third Round"
    fourth_round = "Fourth Round", "Fourth Round"
    fifth_round = "Fifth Round", "Fifth Round"
    sixth_round = "Sixth Round", "Sixth Round"
    seventh_round = "Seventh Round", "Seventh Round"
    eighth_round = "Eighth Round", "Eighth Round"
    round_64 = "Round of 64 (1/32)", "Round of 64 (1/32)"
    round_32 = "Round of 32 (1/16)", "Round of 32 (1/16)"
    round_16 = "Round of 16 (1/8)", "Round of 16 (1/8)"
    quarterfinal = "Quarterfinal (1/4)", "Quarterfinal (1/4)"
    semifinal = "Semi-Final (1/2)", "Semi-Final (1/2)"
    third_place = "Third Place Playoff", "Third Place Playoff"
    final = "Final", "Final"
    qualifying_final = "Qualifying Final", "Qualifying Final"
    prelim_final = "Preliminary Final", "Preliminary Final"
    grand_final = "Grand Final", "Grand Final"


class ConfederationType(DeclEnum):
    """
    Enumerated names of the international football confederations.
    """
    africa = "CAF", "Confederation of African Football"
    asia = "AFC", "Asian Football Confederation"
    europe = "UEFA", "Union of European Football Associations"
    north_america = "CONCACAF", "Confederation of North, Central American, and Caribbean Association Football"
    oceania = "OFC", "Oceania Football Confederation"
    south_america = "CONMEBOL", "South American Football Confederation"
    fifa = "FIFA", "International Federation of Association Football"


class PositionType(DeclEnum):
    """
    Enumerated categories of football player positions.
    """
    goalkeeper = "Goalkeeper", "Goalkeepers"
    defender = "Defender", "Defending positions"
    midfielder = "Midfielder", "Midfield positions"
    forward = "Forward", "Forward positions"
    unknown = "Unknown", "Unknown player position"


class NameOrderType(DeclEnum):
    """
    Enumerated types of naming order conventions.
    """
    western = "Western", "Western"
    middle = "Middle", "Middle"
    eastern = "Eastern", "Eastern"


class SurfaceType(DeclEnum):
    """
    Enumerated types of playing surfaces.
    """
    natural = "Natural", "Natural"
    artificial = "Artificial", "Artificial"
    hybrid = "Hybrid", "Hybrid"


class WeatherConditionType(DeclEnum):
    """
    Enumerated types of NWS/NOAA weather conditions.
    """
    clear = "Clear", "Clear"
    partly_cloudy = "Partly Cloudy", "Partly Cloudy"
    mostly_cloudy = "Mostly Cloudy", "Mostly Cloudy"
    few_clouds = "Few Clouds", "Few Clouds"
    dry_hot = "Hot and Dry", "Hot and Dry"
    humid_hot = "Hot and Humid", "Hot and Humid"
    overcast = "Overcast", "Overcast"
    fog = "Fog/Mist", "Fog/Mist"
    light_rain = "Light Rain", "Light Rain"
    rain = "Rain", "Rain"
    heavy_rain = "Heavy Rain", "Heavy Rain"
    windy_clear = "Clear and Windy", "Clear and Windy"
    windy_mostly_cloudy = "Mostly Cloudy and Windy", "Mostly Cloudy and Windy"
    windy_partly_cloudy = "Partly Cloudy and Windy", "Partly Cloudy and Windy"
    windy_overcast = "Overcast and Windy", "Overcast and Windy"
    flurries = "Snow Flurries", "Snow Flurries"
    light_snow = "Light Snow", "Light Snow"
    heavy_snow = "Heavy Snow", "Heavy Snow"


class ActionType(DeclEnum):
    start_period = "Start Period", "Start Period"
    end_period = "End Period", "End Period"
    ball_pass = "Pass", "Pass"
    dribble = "Dribble", "Dribble"
    cross = "Cross", "Cross"
    throwin = "Throw-In", "Throw-In"
    ball_out = "Out of Play", "Out of Play"
    shot = "Shot", "Shot"
    goal = "Goal", "Goal"
    penalty = "Penalty", "Penalty"
    offside = "Offside", "Offside"
    save = "Save", "Save"
    foul = "Foul", "Foul"
    card = "Card", "Card"
    error = "Error", "Error"
    challenge = "Challenge", "Challenge"
    block = "Block", "Block"
    tackle = "Tackle", "Tackle"
    interception = "Interception", "Interception"
    goalkeeper = "Goalkeeper Action", "Goalkeeper Action"
    clearance = "Clearance", "Clearance"
    corner_kick = "Corner Kick", "Corner Kick"
    free_kick = "Free Kick", "Free Kick"
    goal_kick = "Goal Kick", "Goal Kick"
    substitution = "Substitution", "Substitution"
    shootout = "Shootout Penalty", "Shootout Penalty"
    stopped = "Match Stoppage", "Match Stoppage"


class ModifierCategoryType(DeclEnum):
    substitution = "Substitution", "Substitution"
    bodypart = "Body Part", "Body Part"
    goal_region = "Goal Region", "Goal Region"
    shot_direction = "Shot Direction", "Shot Direction"
    field_sector = "Field Sector", "Field Sector"
    pass_type = "Pass Type", "Pass Type"
    play_outcome = "Play Outcome", "Play Outcome"
    setpiece_type = "Set-piece Type", "Set-piece Type"
    shot_type = "Shot Type", "Shot Type"
    foul_type = "Foul Type", "Foul Type"
    card_type = "Card Type", "Card Type"
    shot_distance = "Shot Distance", "Shot Distance"
    goalkeeper = "Goalkeeper Action", "Goalkeeper Action"
    shot_outcome = "Shot Outcome", "Shot Outcome"
    important = "Important Play", "Important Play"
    misc = "Miscellaneous", "Miscellaneous"


class ModifierType(DeclEnum):
    # Body Part
    left_foot = "Left foot", "Left foot"
    right_foot = "Right foot", "Right foot"
    foot = "Foot", "Foot"
    head = "Head", "Head"
    chest = "Chest", "Chest"
    hand = "Hand", "Hand"
    other_body_part = "Other body part", "Other Body Part"
    unknown_body_part = "Unknown body part", "Unknown Body Part"
    # Field Sector
    center_flank = "Center Flank", "Center Flank"
    center_goal_area = "Central Goal Area", "Central Goal Area"
    center_penalty_area = "Central Penalty Area", "Central Penalty Area"
    left_byline = "Left Byline", "Left Byline"
    left_channel = "Left Channel", "Left Channel"
    left_flank = "Left Flank", "Left Flank"
    left_goal_area = "Left Goal Area", "Left Goal Area"
    left_penalty_area = "Left Penalty Area", "Left Penalty Area"
    left_wing = "Left Wing", "Left Wing"
    own_half = "Own Half", "Own Half"
    penalty_spot = "Penalty Spot", "Penalty Spot"
    right_byline = "Right Byline", "Right Byline"
    right_channel = "Right Channel", "Right Channel"
    right_flank = "Right Flank", "Right Flank"
    right_goal_area = "Right Goal Area", "Right Goal Area"
    right_penalty_area = "Right Penalty Area", "Right Penalty Area"
    right_wing = "Right Wing", "Right Wing"
    # Foul Type
    unknown_foul = "Unknown Foul", "Unknown"
    handball = "Handball", "Handball"
    holding = "Holding", "Holding"
    off_ball = "Off-ball infraction", "Off-ball infraction"
    dangerous = "Dangerous play", "Dangerous play"
    reckless = "Reckless challenge", "Reckless challenge"
    over_celebration = "Excessive celebration", "Excessive celebration"
    simulation = "Simulation", "Simulation"
    dissent = "Dissent", "Dissent"
    repeated_fouling = "Persistent infringement", "Persistent infringement"
    delay_restart = "Delaying restart", "Delaying restart"
    encroachment = "Dead ball encroachment", "Dead ball encroachment"
    field_unauthorized = "Unauthorized field entry/exit", "Unauthorized field entry/exit"
    serious_foul_play = "Serious foul play", "Serious foul play"
    violent_conduct = "Violent conduct", "Violent conduct"
    verbal_abuse = "Offensive/abusive language or gestures", "Offensive/abusive language or gestures"
    spitting = "Spitting", "Spitting"
    professional = "Professional foul", "Professional foul"
    unsporting = "Unsporting behavior", "Unsporting behavior"
    handball_block_goal = "Handball denied obvious scoring opportunity", "Handball denied obvious scoring opportunity"
    # Play Outcome
    result_ball_out = "Ball Out", "Ball Out"
    result_clearance = "Clearance", "Clearance"
    result_corner = "Corner Kick Conceded", "Corner Kick Conceded"
    result_cross = "Cross", "Cross"
    result_foul = "Foul", "Foul"
    result_free_kick = "Free Kick", "Free Kick"
    result_open_play = "Open Play", "Open Play"
    result_pass = "Pass", "Pass"
    result_penalty = "Penalty", "Penalty"
    result_shot = "Shot", "Shot"
    result_no_goal = "Goal Disallowed", "Goal Disallowed"
    # Card Type
    yellow = "Yellow", "Yellow"
    yellow_red = "Yellow/Red", "Yellow/Red"
    red = "Red", "Red"
    # Goal Region
    lower_center = "Lower Center", "Lower Center"
    lower_left = "Lower Left", "Lower Left"
    lower_right = "Lower Right", "Lower Right"
    upper_center = "Upper Center", "Upper Center"
    upper_left = "Upper Left", "Upper Left"
    upper_right = "Upper Right", "Upper Right"
    # Goalkeeper Action
    catch = "Catch", "Catch"
    block = "Block", "Block"
    smother = "Smother", "Smother"
    deflect = "Deflect away", "Deflect away"
    kick_away = "Goalkeeper kick away", "Goalkeeper kick away"
    fumble = "Fumble", "Fumble"
    parry = "Parry", "Parry"
    punch = "Punch", "Punch"
    tip_over = "Tip over bar", "Tip over bar"
    throw = "Keeper throw", "Keeper throw"
    # Pass Type
    long_pass = "Long ball", "Long ball"
    cross_pass = "Crossing ball", "Crossing ball"
    head_pass = "Head pass", "Head pass"
    through_pass = "Through ball", "Through ball"
    freekick_pass = "Free kick", "Free kick"
    corner_pass = "Corner kick", "Corner kick"
    assist = "Goal assist", "Goal assist"
    chance_created = "Scoring chance created", "Scoring chance created"
    # Setpiece Type
    attacking = "Attacking", "Attacking"
    defending = "Defending", "Defending"
    direct = "Direct", "Direct"
    indirect = "Indirect", "Indirect"
    # Shot Direction
    far_post = "Far post", "Far post"
    inswinger = "Inswinger", "Inswinger"
    near_post = "Near post", "Near post"
    outswinger = "Outswinger", "Outswinger"
    wide_left_post = "Wide of left post", "Wide of left post"
    wide_right_post = "Wide of right post", "Wide of right post"
    # Shot Outcome
    goal = "Goal", "Goal"
    mishit = "Mishit", "Mishit"
    blocked = "Blocked", "Blocked"
    saved = "Saved", "Saved"
    wide = "Wide of posts", "Wide of posts"
    over = "Over crossbar", "Over crossbar"
    post = "Hit post", "Hit post"
    bar = "Hit crossbar", "Hit crossbar"
    wood = "Hit woodwork", "Hit woodwork"
    wall = "Hit defensive wall", "Hit defensive wall"
    # Shot
    curled = "Curled", "Curled"
    deflected = "Deflected", "Deflected"
    floated = "Floated", "Floated"
    header = "Header", "Header"
    lob = "Lob", "Lob"
    overhead_kick = "Overhead kick", "Overhead kick"
    placed = "Placed", "Placed"
    power = "Power", "Power"
    scramble = "Goalmouth scramble", "Goalmouth scramble"
    tap_in = "Close-range redirection", "Close-range redirection"
    own_goal = "Own goal", "Own goal"
    volley = "Volley", "Volley"
    half_volley = "Half-volley", "Half-volley"
    shot_open = "Shot from open play", "Shot from open play"
    shot_corner = "Shot from corner kick", "Shot from corner kick"
    shot_freekick = "Shot from free kick", "Shot from free kick"
    shot_throwin = "Shot from throw in", "Shot from throw in"
    shot_counter = "Shot from counterattack play", "Shot from counterattack play"
    shot_penalty = "Penalty kick", "Penalty kick"
    # Sub Type
    injury = "Injury substitution", "Injury substitution"
    sub_off = "Subbed off", "Subbed off"
    sub_on = "Subbed on", "Subbed on"
    tactical = "Tactical substitution", "Tactical substitution"
    withdrawal = "Withdrawn player", "Withdrawn player"
    # Important
    free = "Undefended", "Player left undefended"
    kpi = "KPI", "Internal KPI"
    anti = "Anti KPI", "Internal negative KPI"
    # Misc
    referee_delay = "Referee delays play", "Referee delays play"
    referee_stop = "Referee stops play", "Referee stops play"
    weather = "Weather stops play", "Weather stops play"
    player_injury = "Player injury stops play", "Player injury stops play"
    crowd_disturbance = "Crowd disturbance/invasion", "Crowd disturbance/invasion"
    restart_play = "Play restarted", "Play restarted"
