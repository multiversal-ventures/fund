# scripts/occupation_resilience.py
"""Compute AI-resistant workforce index from Census B24010 occupation data."""
import pandas as pd
from pathlib import Path

# B24010 variable codes for occupation subgroups (Male + Female combined)
# We need both male (B24010_003E etc) and female (B24010_039E etc) counts
# Total employed: B24010_001E
B24010_VARIABLES = [
    "B24010_001E",  # Total civilian employed 16+
    # Management, business, science, arts
    "B24010_003E",  # Management (male)
    "B24010_004E",  # Business and financial operations (male)
    "B24010_005E",  # Computer, engineering, science (male)
    "B24010_012E",  # Education, legal, community, arts, media (male)
    "B24010_019E",  # Healthcare practitioners (male)
    # Service
    "B24010_021E",  # Healthcare support (male)
    "B24010_022E",  # Protective service (male)
    "B24010_023E",  # Food preparation and serving (male)
    "B24010_024E",  # Building and grounds maintenance (male)
    "B24010_025E",  # Personal care and service (male)
    # Sales and office
    "B24010_027E",  # Sales (male)
    "B24010_028E",  # Office and admin support (male)
    # Natural resources, construction, maintenance
    "B24010_030E",  # Farming, fishing, forestry (male)
    "B24010_031E",  # Construction and extraction (male)
    "B24010_032E",  # Installation, maintenance, repair (male)
    # Production, transportation
    "B24010_034E",  # Production (male)
    "B24010_035E",  # Transportation and material moving (male)
    # Female equivalents (same structure, offset by 36)
    "B24010_039E",  # Management (female)
    "B24010_040E",  # Business and financial operations (female)
    "B24010_041E",  # Computer, engineering, science (female)
    "B24010_048E",  # Education, legal, community, arts, media (female)
    "B24010_055E",  # Healthcare practitioners (female)
    "B24010_057E",  # Healthcare support (female)
    "B24010_058E",  # Protective service (female)
    "B24010_059E",  # Food preparation and serving (female)
    "B24010_060E",  # Building and grounds maintenance (female)
    "B24010_061E",  # Personal care and service (female)
    "B24010_063E",  # Sales (female)
    "B24010_064E",  # Office and admin support (female)
    "B24010_066E",  # Farming, fishing, forestry (female)
    "B24010_067E",  # Construction and extraction (female)
    "B24010_068E",  # Installation, maintenance, repair (female)
    "B24010_070E",  # Production (female)
    "B24010_071E",  # Transportation and material moving (female)
]

# Resilience scores per occupation subgroup
# Combined: 0.5 * (1 - Frey&Osborne automation prob) + 0.5 * (1 - LLM exposure)
# Higher = more resilient to both physical automation AND AI
OCCUPATION_RESILIENCE = {
    "management": {
        "male": "B24010_003E", "female": "B24010_039E",
        "resilience": 0.62,  # moderate automation risk, moderate LLM exposure
    },
    "business_financial": {
        "male": "B24010_004E", "female": "B24010_040E",
        "resilience": 0.35,  # high LLM exposure (accountants, analysts)
    },
    "computer_engineering_science": {
        "male": "B24010_005E", "female": "B24010_041E",
        "resilience": 0.45,  # low physical automation, high LLM exposure
    },
    "education_legal_arts": {
        "male": "B24010_012E", "female": "B24010_048E",
        "resilience": 0.65,  # teachers very safe, lawyers mixed
    },
    "healthcare_practitioners": {
        "male": "B24010_019E", "female": "B24010_055E",
        "resilience": 0.90,  # very low automation AND LLM risk
    },
    "healthcare_support": {
        "male": "B24010_021E", "female": "B24010_057E",
        "resilience": 0.85,  # hands-on care, very hard to automate
    },
    "protective_service": {
        "male": "B24010_022E", "female": "B24010_058E",
        "resilience": 0.80,  # police, fire, security — physical presence required
    },
    "food_preparation": {
        "male": "B24010_023E", "female": "B24010_059E",
        "resilience": 0.40,  # moderate automation risk (fast food), low LLM
    },
    "building_grounds": {
        "male": "B24010_024E", "female": "B24010_060E",
        "resilience": 0.70,  # physical work, hard to automate
    },
    "personal_care": {
        "male": "B24010_025E", "female": "B24010_061E",
        "resilience": 0.60,  # mixed — some automatable, some not
    },
    "sales": {
        "male": "B24010_027E", "female": "B24010_063E",
        "resilience": 0.25,  # high automation AND LLM risk (retail, telemarketing)
    },
    "office_admin": {
        "male": "B24010_028E", "female": "B24010_064E",
        "resilience": 0.20,  # very high risk both dimensions
    },
    "farming_fishing": {
        "male": "B24010_030E", "female": "B24010_066E",
        "resilience": 0.55,  # some mechanization risk, low LLM
    },
    "construction_extraction": {
        "male": "B24010_031E", "female": "B24010_067E",
        "resilience": 0.82,  # electricians, plumbers, carpenters — very hard to automate
    },
    "installation_maintenance_repair": {
        "male": "B24010_032E", "female": "B24010_068E",
        "resilience": 0.80,  # HVAC, mechanics — physical dexterity required
    },
    "production": {
        "male": "B24010_034E", "female": "B24010_070E",
        "resilience": 0.30,  # factory work — high physical automation risk
    },
    "transportation": {
        "male": "B24010_035E", "female": "B24010_071E",
        "resilience": 0.35,  # trucking has high automation risk (autonomous vehicles)
    },
}


def compute_resilience_index(occupation_df: pd.DataFrame) -> pd.DataFrame:
    """Compute AI-Resistant Workforce Index for each county.

    Args:
        occupation_df: DataFrame with B24010 variables by county (from Census API).

    Returns:
        DataFrame with fips, total_employed, resilience_index, and breakdown columns.
    """
    df = occupation_df.copy()

    # Ensure numeric
    for var in B24010_VARIABLES:
        if var in df.columns:
            df[var] = pd.to_numeric(df[var], errors="coerce").fillna(0)

    total = df["B24010_001E"]

    # Compute weighted resilience
    weighted_sum = pd.Series(0.0, index=df.index)
    breakdown = {}

    for name, info in OCCUPATION_RESILIENCE.items():
        male_col = info["male"]
        female_col = info["female"]
        resilience = info["resilience"]

        count = df.get(male_col, 0) + df.get(female_col, 0)
        if isinstance(count, (int, float)):
            count = pd.Series(count, index=df.index)

        weighted_sum += count * resilience
        breakdown[f"occ_{name}_count"] = count
        breakdown[f"occ_{name}_pct"] = (count / total.replace(0, float("nan"))).round(4)

    df["resilience_index"] = (weighted_sum / total.replace(0, float("nan"))).round(4)
    df["total_employed"] = total

    # Add top occupation groups for context
    # "Blue collar safe" = construction + installation + protective + healthcare support + building grounds
    safe_trades = (
        df.get("B24010_031E", 0) + df.get("B24010_067E", 0) +  # construction
        df.get("B24010_032E", 0) + df.get("B24010_068E", 0) +  # install/repair
        df.get("B24010_022E", 0) + df.get("B24010_058E", 0) +  # protective
        df.get("B24010_021E", 0) + df.get("B24010_057E", 0) +  # healthcare support
        df.get("B24010_024E", 0) + df.get("B24010_060E", 0)    # building/grounds
    )
    df["blue_collar_safe_pct"] = (safe_trades / total.replace(0, float("nan"))).round(4)

    # "White collar at risk" = office admin + sales
    at_risk = (
        df.get("B24010_028E", 0) + df.get("B24010_064E", 0) +  # office
        df.get("B24010_027E", 0) + df.get("B24010_063E", 0)    # sales
    )
    df["white_collar_risk_pct"] = (at_risk / total.replace(0, float("nan"))).round(4)

    # Healthcare total (practitioners + support)
    healthcare = (
        df.get("B24010_019E", 0) + df.get("B24010_055E", 0) +  # practitioners
        df.get("B24010_021E", 0) + df.get("B24010_057E", 0)    # support
    )
    df["healthcare_pct"] = (healthcare / total.replace(0, float("nan"))).round(4)

    keep = [
        "fips", "county", "state", "year", "total_employed",
        "resilience_index", "blue_collar_safe_pct",
        "white_collar_risk_pct", "healthcare_pct",
    ]
    return df[[c for c in keep if c in df.columns]].copy()
