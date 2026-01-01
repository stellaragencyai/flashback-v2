def score_strategy_fit(strategy_profile, regime, metrics):
    preferences = strategy_profile.get('preferred_regimes', [])
    penalties = strategy_profile.get('avoid_regimes', [])

    score = 0.5

    if regime in preferences:
        score += 0.3

    if regime in penalties:
        score -= 0.4

    rr = metrics.get('avg_rr', 0)
    win_rate = metrics.get('win_rate', 0)

    score += min(rr * 0.05, 0.1)
    score += min(win_rate * 0.1, 0.1)

    return round(max(0, min(score, 1)), 3)
