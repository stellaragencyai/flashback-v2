import json
import os
from datetime import datetime
from fastapi import APIRouter, HTTPException

router = APIRouter()

STATE_PATH = os.path.join(
    os.path.dirname(__file__),
    '..',
    'state',
    'governance_state.json'
)

@router.get('/governance')
def get_governance():
    if not os.path.exists(STATE_PATH):
        return {}

    with open(STATE_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)

@router.patch('/governance/{district_id}')
def update_governance(district_id: str, updates: dict):
    if not os.path.exists(STATE_PATH):
        raise HTTPException(status_code=500, detail='Governance state missing')

    with open(STATE_PATH, 'r', encoding='utf-8') as f:
        payload = json.load(f)

    if district_id not in payload['districts']:
        raise HTTPException(status_code=404, detail='Unknown district')

    for key, value in updates.items():
        if key in payload['districts'][district_id]:
            payload['districts'][district_id][key] = value

    payload['districts'][district_id]['last_updated_utc'] = datetime.utcnow().isoformat()

    with open(STATE_PATH, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2)

    return payload['districts'][district_id]

