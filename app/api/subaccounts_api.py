import json
import os
from fastapi import APIRouter

router = APIRouter()

STATE_FILE = os.path.join(
    os.path.dirname(__file__),
    '..',
    'state',
    'subaccounts_state.json'
)

GOV_FILE = os.path.join(
    os.path.dirname(__file__),
    '..',
    'state',
    'governance_state.json'
)

@router.get('/subaccounts')
def get_subaccounts():
    if not os.path.exists(STATE_FILE):
        return []

    with open(STATE_FILE, 'r', encoding='utf-8') as f:
        state = json.load(f)

    subaccounts = state.get('subaccounts', [])

    governance = {}
    if os.path.exists(GOV_FILE):
        with open(GOV_FILE, 'r', encoding='utf-8') as f:
            governance = json.load(f).get('districts', {})

    for sa in subaccounts:
        gid = sa['subaccount_uid']
        if gid in governance:
            sa.update(governance[gid])

    return subaccounts

