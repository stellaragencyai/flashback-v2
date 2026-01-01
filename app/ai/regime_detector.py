def detect_regime(market_features):
    vol = market_features.get('volatility', 0)
    trend = market_features.get('trend_strength', 0)
    volume = market_features.get('volume', 0)

    if vol > 0.8 and trend < 0.3:
        return 'volatile'

    if trend > 0.7 and vol < 0.6:
        return 'trending'

    if vol < 0.3 and trend < 0.3:
        return 'ranging'

    if vol > 0.7 and volume > 0.8:
        return 'momentum_burst'

    if volume < 0.2:
        return 'low_liquidity'

    return 'unknown'
