
from app.core.ai_action_bus import emit_action

def emit_evolution_actions(decisions):
    for s in decisions['promotions']:
        emit_action('STRATEGY_PROMOTE', {'strategy_id': s})

    for s in decisions['demotions']:
        emit_action('STRATEGY_DEMOTE', {'strategy_id': s})

    for s in decisions['kills']:
        emit_action('STRATEGY_KILL', {'strategy_id': s})

