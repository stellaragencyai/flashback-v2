import uuid
import yaml
from pathlib import Path
from app.core.strategy_registry import register_strategy

with open(Path('config/strategy_cloning.yaml'), 'r', encoding='utf-8') as f:
    CLONE_RULES = yaml.safe_load(f)['cloning_rules']

def can_clone(strategy_state, metrics, active_clones):
    if strategy_state['stage'] not in CLONE_RULES['eligibility']['allowed_stages']:
        return False

    if metrics.get('confidence', 0) < CLONE_RULES['eligibility']['min_confidence']:
        return False

    if active_clones >= CLONE_RULES['eligibility']['max_active_clones']:
        return False

    return True

def clone_strategy(parent_strategy_id, target_district_id, parent_config):
    clone_id = f"{parent_strategy_id}_clone_{uuid.uuid4().hex[:6]}"

    clone_config = parent_config.copy()
    clone_config['parent'] = parent_strategy_id
    clone_config['capital_ratio'] = CLONE_RULES['clone_defaults']['starting_capital_ratio']
    clone_config['autonomy'] = CLONE_RULES['clone_defaults']['autonomy_level']
    clone_config['learning_mode'] = CLONE_RULES['clone_defaults']['learning_mode']
    clone_config['pair_limit'] = CLONE_RULES['clone_defaults']['allowed_pairs_limit']

    register_strategy(clone_id, target_district_id)

    return clone_id, clone_config
