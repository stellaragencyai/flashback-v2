
import yaml
import time
from pathlib import Path

BASE = Path(__file__).resolve().parents[2]
GOV_FILE = BASE / 'config' / 'governance.yaml'

def _load():
    if not GOV_FILE.exists():
        return {'districts': {}}
    with open(GOV_FILE, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f) or {'districts': {}}

def _save(data):
    with open(GOV_FILE, 'w', encoding='utf-8') as f:
        yaml.safe_dump(data, f)

def get_governance(uid):
    data = _load()
    defaults = data.get('districts', {}).get('default', {})
    specific = data.get('districts', {}).get(uid, {})
    return {**defaults, **specific}

def set_flag(uid, flag, value, source='ui'):
    data = _load()
    data.setdefault('districts', {})
    data['districts'].setdefault(uid, {})
    data['districts'][uid][flag] = value
    data['districts'][uid]['last_command'] = f'{source}:{flag}'
    data['districts'][uid]['updated_at'] = int(time.time())
    _save(data)

