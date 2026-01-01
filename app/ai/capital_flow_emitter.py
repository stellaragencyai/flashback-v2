
from app.core.ai_action_bus import emit_action

def emit_capital_flows(flows):
    for f in flows.inject:
        emit_action('CAPITAL_INJECT', f)

    for f in flows.drain:
        emit_action('CAPITAL_DRAIN', f)

    for f in flows.recall:
        emit_action('CAPITAL_RECALL', f)

    for f in flows.hq_transfer:
        emit_action('CAPITAL_TO_HQ', f)

