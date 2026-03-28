"""Shared constants used across routes."""

RECODE_LABELS = {
    "HR": "Household",
    "IR": "Individual (Women)",
    "PR": "Household Members",
    "KR": "Children",
    "BR": "Birth History",
    "MR": "Men",
    "CR": "Couples",
    "FW": "Fieldworker",
    "WI": "Wealth Index",
    "GR": "Geographic",
    "SR": "Service / Schedule",
    "NR": "Neonatal / Antenatal",
    "AR": "HIV Test",
}

NUMERIC_PG_TYPES = {
    "SMALLINT", "INTEGER", "BIGINT", "REAL",
    "DOUBLE PRECISION", "NUMERIC", "FLOAT",
}
