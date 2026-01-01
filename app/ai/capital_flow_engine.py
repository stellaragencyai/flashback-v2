
from app.ai.capital_flow_policy import CAPITAL_POLICY

class CapitalFlowDecision:
    def __init__(self):
        self.inject = []
        self.drain = []
        self.recall = []
        self.hq_transfer = []

def compute_capital_flows(evolution_decisions, policy_stats):
    flows = CapitalFlowDecision()

    for s in evolution_decisions.get('promotions', []):
        flows.inject.append({
            'strategy_id': s,
            'pct': CAPITAL_POLICY['promotion']['allocation_pct']
        })

    for s in evolution_decisions.get('demotions', []):
        flows.drain.append({
            'strategy_id': s,
            'pct': CAPITAL_POLICY['demotion']['deallocation_pct']
        })

    for s in evolution_decisions.get('kills', []):
        flows.recall.append({
            'strategy_id': s,
            'pct': CAPITAL_POLICY['kill']['recall_pct']
        })

    for s in policy_stats:
        if s.get('realized_profit', 0) > 0:
            flows.hq_transfer.append({
                'strategy_id': s['strategy_id'],
                'pct': CAPITAL_POLICY['hq_siphon']['profit_pct']
            })

    return flows

