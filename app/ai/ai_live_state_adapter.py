
from app.ai.ai_policy_stats import load_policy_stats
from app.ai.ai_regime_scanner import get_current_regimes

class StrategyState:
    def __init__(self, s):
        self.id = s['strategy_id']
        self.n_trades = s['n_trades']
        self.expectancy = s['expectancy']
        self.drawdown = s['max_drawdown']
        self.regime_baseline = s.get('regime_baseline', 0)
        self.soft_floor = s.get('soft_floor', -0.05)
        self.kill_floor = s.get('kill_floor', -0.15)
        self.promotion_n_min = s.get('promotion_n_min', 200)
        self.kill_n_min = s.get('kill_n_min', 500)

class LiveAIState:
    def __init__(self):
        self.policy_stats = load_policy_stats()
        self.regimes = get_current_regimes()
        self.active_strategies = [
            StrategyState(s) for s in self.policy_stats
        ]

