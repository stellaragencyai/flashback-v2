
from app.ai.ai_strategy_promoter import evaluate_promotions
from app.ai.ai_strategy_demoter import evaluate_demotions
from app.ai.ai_strategy_killer import evaluate_kills

def run_evolution_cycle(ai_state):
    promotions = evaluate_promotions(ai_state)
    demotions = evaluate_demotions(ai_state)
    kills = evaluate_kills(ai_state)

    return {
        'promotions': promotions,
        'demotions': demotions,
        'kills': kills
    }

