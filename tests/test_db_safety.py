"""Test SQL safety utilities."""

import pytest
from webapp.db import validate_ident, validate_table


def test_valid_identifiers():
    assert validate_ident("v025") == "v025"
    assert validate_ident("caseid") == "caseid"
    assert validate_ident("hv_001") == "hv_001"
    assert validate_ident("_overflow") == "_overflow"


def test_invalid_identifiers():
    with pytest.raises(ValueError):
        validate_ident("DROP TABLE")
    with pytest.raises(ValueError):
        validate_ident("'; DELETE FROM --")
    with pytest.raises(ValueError):
        validate_ident("v025; DROP")
    with pytest.raises(ValueError):
        validate_ident("123abc")
    with pytest.raises(ValueError):
        validate_ident("")


def test_valid_tables():
    assert validate_table("microdata.mw_dhs_2015_ir") == "microdata.mw_dhs_2015_ir"
    assert validate_table("catalog.country") == "catalog.country"


def test_invalid_tables():
    with pytest.raises(ValueError):
        validate_table("microdata.observation; DROP TABLE")
    with pytest.raises(ValueError):
        validate_table("just_a_name")
    with pytest.raises(ValueError):
        validate_table("")
