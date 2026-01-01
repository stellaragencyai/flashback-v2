
def clone_strategy(strategy, target_district):
    return {
        'source': strategy.id,
        'district': target_district,
        'status': 'probationary_clone'
    }

