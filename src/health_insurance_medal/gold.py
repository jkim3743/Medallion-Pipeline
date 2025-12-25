from typing import List, Dict


def compute_response_rate(response_count: int, lead_count: int) -> float:
    """
    Safely compute response rate.
    """
    if lead_count == 0:
        return 0.0
    return response_count / lead_count


def compute_opportunity_score(
    response_rate: float,
    lead_count: int,
    avg_premium: float
) -> float:
    """
    Opportunity score used for analytics dashboards.

    Formula:
    expected_value = response_rate * lead_count * avg_premium
    """
    if response_rate < 0 or lead_count < 0 or avg_premium < 0:
        return 0.0

    return response_rate * lead_count * avg_premium


def validate_gold_columns(columns: List[str]) -> bool:
    """
    Ensure Gold table has required columns.
    """
    required = {
        "lead_count",
        "response_count",
        "response_rate",
        "avg_reco_policy_premium"
    }

    return required.issubset(set(columns))
