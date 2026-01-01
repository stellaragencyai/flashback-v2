import yaml
from pathlib import Path

_AI_PROFILE_CACHE = None

def load_ai_profiles():
    global _AI_PROFILE_CACHE
    if _AI_PROFILE_CACHE is None:
        path = Path('config/ai_profiles.yaml')
        with open(path, 'r', encoding='utf-8') as f:
            _AI_PROFILE_CACHE = yaml.safe_load(f)
    return _AI_PROFILE_CACHE

def get_district_profile(district_id: str) -> dict:
    profiles = load_ai_profiles()
    return profiles['districts'].get(district_id)
