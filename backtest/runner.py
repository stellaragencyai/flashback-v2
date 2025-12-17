from backtest.loader import load_candles
from backtest.simulator import Simulator
from backtest.executor_sim import ExecutorSim
from backtest.metrics import compute_metrics

def run_backtest(
    symbol: str,
    timeframe: str,
    prices_file: str,
    strategy_config,
    model,
    initial_balance: float = 100000,
):
    """
    strategy_config example:
    {
      "entry_threshold": 0.6,
      "exit_threshold": 0.4,
      "max_positions": 1
    }
    """
    candles = load_candles(prices_file)
    sim = Simulator(initial_balance)
    exec_sim = ExecutorSim(model, strategy_config, risk_manager=None)

    trades = []
    for idx, row in candles.iterrows():
        price = row["close"]
        features = {}  # call your feature builder here

        # ENTRY DECISION
        if exec_sim.should_enter(features):
            exec_sim.simulate_entry(sim, symbol, price)

        # EXIT DECISION — simplistic: exit every bar
        if symbol in sim.positions:
            pnl = sim.close_position(symbol, price)
            trades.append({
                "symbol": symbol,
                "entry_price": price,
                "exit_price": price,
                "pnl": pnl,
            })

    results = compute_metrics(trades)
    return results

if __name__ == "__main__":
    import joblib

    # example usage
    model = joblib.load("models/trend_v1_v1.pkl")
    strat_cfg = {
        "entry_threshold": 0.65,
        "exit_threshold": 0.35,
        "max_positions": 1,
    }

    r = run_backtest(
        symbol="BTCUSDT",
        timeframe="15m",
        prices_file="data/BTCUSDT_15m.parquet",
        strategy_config=strat_cfg,
        model=model,
    )

    print("Backtest summary:", r)
