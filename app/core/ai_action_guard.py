from app.core.ai_profile import get_district_profile

def enforce_strategy_policy(district_id, strategy_id, pair):
    profile = get_district_profile(district_id)

    if not profile:
        raise RuntimeError(f'Unknown district {district_id}')

    if strategy_id not in profile.get('allowed_strategies', []):
        raise PermissionError(
            f'Strategy {strategy_id} not allowed for district {district_id}'
        )

    if pair not in profile.get('allowed_pairs', []):
        raise PermissionError(
            f'Pair {pair} not allowed for district {district_id}'
        )
