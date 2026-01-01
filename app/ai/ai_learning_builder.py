from app.core.ai_profile import get_district_profile

def learning_weight_for_district(district_id):
    profile = get_district_profile(district_id)

    mode = profile.get('learning_mode', 'adaptive')

    return {
        'aggressive': 1.5,
        'adaptive': 1.0,
        'conservative': 0.5,
        'passive': 0.1,
    }.get(mode, 1.0)
