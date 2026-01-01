from app.core.ai_profile import get_district_profile

def is_bootstrap_locked(district_id):
    profile = get_district_profile(district_id)
    return profile.get('bootstrap', False)

def filter_strategies_for_district(district_id, strategies, pairs):
    if is_bootstrap_locked(district_id):
        return strategies[:1], pairs[:1]
    return strategies, pairs
