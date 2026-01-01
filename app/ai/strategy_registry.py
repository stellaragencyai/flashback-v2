import yaml

def load_yaml(path):
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def load_strategies():
    strategies = load_yaml('config/strategies.yaml')
    bots = load_yaml('config/bots.yaml')
    subaccounts = load_yaml('config/subaccounts.yaml')

    registry = {}

    for bot in bots.get('bots', []):
        strategy_id = bot['strategy']
        registry.setdefault(strategy_id, []).append({
            'bot_id': bot['id'],
            'subaccount': bot['subaccount'],
            'status': 'idle'
        })

    return registry
