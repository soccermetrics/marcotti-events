# coding=utf-8

import pytest

import marcottievents.models.common.enums as enums
import marcottievents.models.common.events as mce


def test_modifiers(session, modifiers):
    modifier_objs = [mce.Modifiers(type=enums.ModifierType.from_string(record['modifier']),
                                   category=enums.ModifierCategoryType.from_string(record['category']))
                     for record in modifiers]
    session.add_all(modifier_objs)
    session.commit()

    assert session.query(mce.Modifiers).count() == len(modifiers)

    shot_type_mods = session.query(mce.Modifiers).filter_by(category=enums.ModifierCategoryType.shot_type)
    assert shot_type_mods.count() == 1
    assert shot_type_mods[0].type.value == "Volley"

    shot_outcome_mods = session.query(mce.Modifiers).filter_by(category=enums.ModifierCategoryType.shot_outcome)
    assert shot_outcome_mods.count() == 1
    assert shot_outcome_mods[0].type.value == "Saved"

    shot_direction_mods = session.query(mce.Modifiers).filter_by(category=enums.ModifierCategoryType.shot_direction)
    assert shot_direction_mods.count() == 1
    assert shot_direction_mods[0].type.value == "Wide of right post"
