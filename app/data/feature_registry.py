import json
from pathlib import Path
from typing import Dict, List, Tuple

STATE = Path('state/features')
REGISTRY = STATE / 'registry.json'
ACTIVE = STATE / 'active_version.txt'


def get_active_version() -> str:
    if not ACTIVE.exists():
        return 'v1.0'
    return ACTIVE.read_text(encoding='utf-8').strip()


def load_registry() -> Dict[str, Dict]:
    if not REGISTRY.exists():
        return {}
    with REGISTRY.open('r', encoding='utf-8') as f:
        return json.load(f)


def save_registry(reg):
    STATE.mkdir(parents=True, exist_ok=True)
    with REGISTRY.open('w', encoding='utf-8') as f:
        json.dump(reg, f, indent=2, sort_keys=True)


def extract_schema(rows: List[dict]) -> Dict[str, str]:
    schema = {}
    for r in rows:
        for k, v in r.items():
            if k in schema:
                continue
            if v is None:
                schema[k] = 'null'
            else:
                schema[k] = type(v).__name__
    return schema


def diff_schema(old: Dict[str, str], new: Dict[str, str]) -> Tuple[List[str], List[str], List[str]]:
    removed = [k for k in old if k not in new]
    added = [k for k in new if k not in old]
    changed = [k for k in new if k in old and old[k] != new[k]]
    return removed, added, changed


def enforce_schema(rows: List[dict]):
    if not rows:
        return

    version = get_active_version()
    registry = load_registry()
    new_schema = extract_schema(rows)

    if version not in registry:
        registry[version] = new_schema
        save_registry(registry)
        return

    old_schema = registry[version]
    removed, added, changed = diff_schema(old_schema, new_schema)

    if removed:
        raise RuntimeError(f'FEATURE REGISTRY VIOLATION: removed columns {removed}')

    if changed:
        raise RuntimeError(f'FEATURE REGISTRY VIOLATION: type changes {changed}')

    if added:
        for k in added:
            old_schema[k] = new_schema[k]
        registry[version] = old_schema
        save_registry(registry)
