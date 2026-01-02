from app.ops.ops_emitter_v1 import emit_ops
from app.comms.telegram_bus_v1 import send_alert

def emit_and_notify(subaccount_id, **kwargs):
    emit_ops(subaccount_id, **kwargs)

    risk = kwargs.get('risk_level', 'normal')
    msg = f'[{subaccount_id}] {kwargs.get('last_event','update')} | risk={risk}'

    if risk in ('high','halted'):
        send_alert(subaccount_id, msg)
