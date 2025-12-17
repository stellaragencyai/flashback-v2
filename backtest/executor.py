from typing import Dict, Any

class ExecutorSim:
    def __init__(self, model, strategy_config, risk_manager):
        self.model = model
        self.strategy = strategy_config
        self.risk = risk_manager

    def should_enter(self, features: Dict[str, Any]) -> bool:
        """
        Evaluate signal + model
        """
        score = self.model.predict_proba([features])[0][1]
        return score >= self.strategy["entry_threshold"]

    def simulate_entry(self, sim, symbol, price):
        size = self.risk.position_size(sim.equity)
        sim.open_position(symbol, price, size, "long")
