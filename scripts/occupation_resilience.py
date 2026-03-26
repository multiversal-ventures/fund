# scripts/occupation_resilience.py
"""Compute AI-resistant workforce index from Census C24010 occupation data.

Uses C24010 (Sex by Occupation for Civilian Employed Population 16+)
with resilience scores derived from:
- Frey & Osborne (2017) automation probabilities
- Eloundou et al. (2023) LLM/AI exposure scores
Combined 50/50 → Resilience = 1 - combined_risk
"""
import pandas as pd
from pathlib import Path

# C24010 variable codes — correct table for ACS 5-Year
C24010_VARIABLES = [
    "C24010_001E",  # Total civilian employed 16+
    # Male occupation subgroups
    "C24010_005E",  # Management occupations (male)
    "C24010_006E",  # Business and financial operations (male)
    "C24010_008E",  # Computer and mathematical (male)
    "C24010_009E",  # Architecture and engineering (male)
    "C24010_010E",  # Life, physical, social science (male)
    "C24010_012E",  # Community and social service (male)
    "C24010_013E",  # Legal (male)
    "C24010_014E",  # Educational instruction, library (male)
    "C24010_015E",  # Arts, design, entertainment, sports, media (male)
    "C24010_017E",  # Health diagnosing/treating practitioners (male)
    "C24010_018E",  # Health technologists and technicians (male)
    "C24010_020E",  # Healthcare support (male)
    "C24010_022E",  # Firefighting, prevention, protective service (male)
    "C24010_023E",  # Law enforcement (male)
    "C24010_024E",  # Food preparation and serving (male)
    "C24010_025E",  # Building and grounds maintenance (male)
    "C24010_026E",  # Personal care and service (male)
    "C24010_028E",  # Sales (male)
    "C24010_029E",  # Office and admin support (male)
    "C24010_031E",  # Farming, fishing, forestry (male)
    "C24010_032E",  # Construction and extraction (male)
    "C24010_033E",  # Installation, maintenance, repair (male)
    "C24010_035E",  # Production (male)
    "C24010_036E",  # Transportation (male)
    "C24010_037E",  # Material moving (male)
    # Female occupation subgroups
    "C24010_041E",  # Management occupations (female)
    "C24010_042E",  # Business and financial operations (female)
    "C24010_044E",  # Computer and mathematical (female)
    "C24010_045E",  # Architecture and engineering (female)
    "C24010_046E",  # Life, physical, social science (female)
    "C24010_048E",  # Community and social service (female)
    "C24010_049E",  # Legal (female)
    "C24010_050E",  # Educational instruction, library (female)
    "C24010_051E",  # Arts, design, entertainment, sports, media (female)
    "C24010_053E",  # Health diagnosing/treating practitioners (female)
    "C24010_054E",  # Health technologists and technicians (female)
    "C24010_056E",  # Healthcare support (female)
    "C24010_058E",  # Firefighting, prevention, protective service (female)
    "C24010_059E",  # Law enforcement (female)
    "C24010_060E",  # Food preparation and serving (female)
    "C24010_061E",  # Building and grounds maintenance (female)
    "C24010_062E",  # Personal care and service (female)
    "C24010_064E",  # Sales (female)
    "C24010_065E",  # Office and admin support (female)
    "C24010_067E",  # Farming, fishing, forestry (female)
    "C24010_068E",  # Construction and extraction (female)
    "C24010_069E",  # Installation, maintenance, repair (female)
    "C24010_071E",  # Production (female)
    "C24010_072E",  # Transportation (female)
    "C24010_073E",  # Material moving (female)
]

# Resilience scores per occupation subgroup
# Combined: 0.5 * (1 - Frey&Osborne automation prob) + 0.5 * (1 - LLM exposure)
# Higher = more resilient to both physical automation AND AI
OCCUPATION_RESILIENCE = {
    "management": {
        "male": "C24010_005E", "female": "C24010_041E",
        "resilience": 0.62,
    },
    "business_financial": {
        "male": "C24010_006E", "female": "C24010_042E",
        "resilience": 0.35,  # high LLM exposure (accountants, analysts)
    },
    "computer_math": {
        "male": "C24010_008E", "female": "C24010_044E",
        "resilience": 0.40,  # high LLM exposure (programmers, data entry)
    },
    "architecture_engineering": {
        "male": "C24010_009E", "female": "C24010_045E",
        "resilience": 0.65,  # physical design work, moderate LLM
    },
    "life_physical_social_science": {
        "male": "C24010_010E", "female": "C24010_046E",
        "resilience": 0.60,  # lab work hard to automate
    },
    "community_social_service": {
        "male": "C24010_012E", "female": "C24010_048E",
        "resilience": 0.75,  # social workers, counselors — human connection
    },
    "legal": {
        "male": "C24010_013E", "female": "C24010_049E",
        "resilience": 0.35,  # high LLM exposure (paralegals, research)
    },
    "education": {
        "male": "C24010_014E", "female": "C24010_050E",
        "resilience": 0.80,  # teachers — in-person, very low automation
    },
    "arts_media": {
        "male": "C24010_015E", "female": "C24010_051E",
        "resilience": 0.30,  # high LLM/generative AI exposure
    },
    "health_practitioners": {
        "male": "C24010_017E", "female": "C24010_053E",
        "resilience": 0.90,  # doctors, nurses — very low both risks
    },
    "health_technicians": {
        "male": "C24010_018E", "female": "C24010_054E",
        "resilience": 0.75,  # lab techs, radiology — some automation
    },
    "healthcare_support": {
        "male": "C24010_020E", "female": "C24010_056E",
        "resilience": 0.85,  # nursing aides, hands-on care
    },
    "fire_protective": {
        "male": "C24010_022E", "female": "C24010_058E",
        "resilience": 0.88,  # firefighters — physical, dangerous, impossible to automate
    },
    "law_enforcement": {
        "male": "C24010_023E", "female": "C24010_059E",
        "resilience": 0.82,  # police — physical presence required
    },
    "food_preparation": {
        "male": "C24010_024E", "female": "C24010_060E",
        "resilience": 0.40,  # fast food automatable, fine dining less so
    },
    "building_grounds": {
        "male": "C24010_025E", "female": "C24010_061E",
        "resilience": 0.70,  # janitors, landscapers — physical
    },
    "personal_care": {
        "male": "C24010_026E", "female": "C24010_062E",
        "resilience": 0.60,  # barbers, childcare — human touch
    },
    "sales": {
        "male": "C24010_028E", "female": "C24010_064E",
        "resilience": 0.25,  # retail highly automatable + LLM
    },
    "office_admin": {
        "male": "C24010_029E", "female": "C24010_065E",
        "resilience": 0.20,  # very high risk both dimensions
    },
    "farming_fishing": {
        "male": "C24010_031E", "female": "C24010_067E",
        "resilience": 0.55,  # some mechanization, low LLM
    },
    "construction_extraction": {
        "male": "C24010_032E", "female": "C24010_068E",
        "resilience": 0.82,  # electricians, plumbers, carpenters
    },
    "installation_maintenance_repair": {
        "male": "C24010_033E", "female": "C24010_069E",
        "resilience": 0.80,  # HVAC, mechanics
    },
    "production": {
        "male": "C24010_035E", "female": "C24010_071E",
        "resilience": 0.30,  # factory — high physical automation
    },
    "transportation": {
        "male": "C24010_036E", "female": "C24010_072E",
        "resilience": 0.35,  # trucking — autonomous vehicle risk
    },
    "material_moving": {
        "male": "C24010_037E", "female": "C24010_073E",
        "resilience": 0.28,  # warehouse — robots replacing fast
    },
}


def compute_resilience_index(occupation_df: pd.DataFrame) -> pd.DataFrame:
    """Compute AI-Resistant Workforce Index for each county.

    Returns DataFrame with fips, total_employed, resilience_index, and breakdown.
    """
    df = occupation_df.copy()

    for var in C24010_VARIABLES:
        if var in df.columns:
            df[var] = pd.to_numeric(df[var], errors="coerce").fillna(0)

    total = df["C24010_001E"]

    weighted_sum = pd.Series(0.0, index=df.index)

    for name, info in OCCUPATION_RESILIENCE.items():
        male_col = info["male"]
        female_col = info["female"]
        resilience = info["resilience"]

        m = df[male_col] if male_col in df.columns else 0
        f = df[female_col] if female_col in df.columns else 0
        count = m + f

        if isinstance(count, (int, float)):
            count = pd.Series(count, index=df.index)

        weighted_sum += count * resilience

    df["resilience_index"] = (weighted_sum / total.replace(0, float("nan"))).round(4)
    df["total_employed"] = total

    # Blue collar safe = construction + installation + protective + healthcare support + building grounds
    safe_cols_m = ["C24010_032E", "C24010_033E", "C24010_022E", "C24010_023E", "C24010_020E", "C24010_025E"]
    safe_cols_f = ["C24010_068E", "C24010_069E", "C24010_058E", "C24010_059E", "C24010_056E", "C24010_061E"]
    safe_trades = sum(df.get(c, 0) for c in safe_cols_m + safe_cols_f)
    df["blue_collar_safe_pct"] = (safe_trades / total.replace(0, float("nan"))).round(4)

    # White collar at risk = office admin + sales + arts/media
    risk_cols_m = ["C24010_029E", "C24010_028E", "C24010_015E"]
    risk_cols_f = ["C24010_065E", "C24010_064E", "C24010_051E"]
    at_risk = sum(df.get(c, 0) for c in risk_cols_m + risk_cols_f)
    df["white_collar_risk_pct"] = (at_risk / total.replace(0, float("nan"))).round(4)

    # Healthcare total
    hc_cols_m = ["C24010_017E", "C24010_018E", "C24010_020E"]
    hc_cols_f = ["C24010_053E", "C24010_054E", "C24010_056E"]
    healthcare = sum(df.get(c, 0) for c in hc_cols_m + hc_cols_f)
    df["healthcare_pct"] = (healthcare / total.replace(0, float("nan"))).round(4)

    keep = [
        "fips", "county", "state", "year", "total_employed",
        "resilience_index", "blue_collar_safe_pct",
        "white_collar_risk_pct", "healthcare_pct",
    ]
    return df[[c for c in keep if c in df.columns]].copy()


def compute_job_stability(occ_latest: pd.DataFrame, occ_earliest: pd.DataFrame) -> pd.DataFrame:
    """Compute job population stability — are resilient jobs growing or shrinking?

    Compares total employed and blue-collar-safe counts between two years.
    Positive growth in resilient sectors = more stable tenant base.
    """
    latest = occ_latest[["fips", "total_employed", "blue_collar_safe_pct"]].copy()
    earliest = occ_earliest[["fips", "total_employed", "blue_collar_safe_pct"]].copy()

    merged = latest.merge(
        earliest.rename(columns={
            "total_employed": "employed_prev",
            "blue_collar_safe_pct": "blue_collar_prev",
        }),
        on="fips",
        how="left",
    )

    merged["employment_growth"] = (
        (merged["total_employed"] - merged["employed_prev"]) /
        merged["employed_prev"].replace(0, float("nan"))
    ).round(4)

    merged["blue_collar_trend"] = (
        merged["blue_collar_safe_pct"] - merged["blue_collar_prev"]
    ).round(4)

    return merged[["fips", "employment_growth", "blue_collar_trend"]].copy()
