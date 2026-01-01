import yaml
from app.ai.strategy_fit_engine import score_strategy_fit

def load_matrix():
    with open('config/strategy_regime_matrix.yaml', 'r') as f:
        return yaml.safe_load(f)['matrix']

def route_strategies(strategies, regime, metrics_by_strategy):
    matrix = load_matrix()
    decisions = {}

    for strategy_id, bots in strategies.items():
        rules = matrix.get(strategy_id, {})
        preferred = rules.get('preferred', [])
        allowed = rules.get('allowed', [])
        avoid = rules.get('avoid', [])

        if regime in avoid:
            decision = 'pause'
        elif regime in preferred:
            decision = 'boost'
        elif regime in allowed:
            decision = 'run'
        else:
            decision = 'throttle'

        fit = score_strategy_fit(
            strategy_profile={'preferred_regimes': preferred, 'avoid_regimes': avoid},
            regime=regime,
            metrics=metrics_by_strategy.get(strategy_id, {})
        )

        decisions[strategy_id] = {
            'decision': decision,
            'fit_score': fit,
            'bots': bots
        }

    return decisions
