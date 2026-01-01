import { useEffect, useState } from 'react';

export default function GovernanceCockpit() {
  const [districts, setDistricts] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch('http://127.0.0.1:8000/api/subaccounts')
      .then(res => res.json())
      .then(data => {
        setDistricts(data);
        setLoading(false);
      })
      .catch(() => setLoading(false));
  }, []);

  if (loading) {
    return <div style={{ padding: 24 }}>Loading districts…</div>;
  }

  return (
    <div style={{
      display: 'grid',
      gridTemplateColumns: 'repeat(auto-fill, minmax(320px, 1fr))',
      gap: 16,
      padding: 24
    }}>
      {districts.map(d => (
        <div key={d.subaccount_uid}
          style={{
            border: '1px solid #333',
            borderRadius: 12,
            padding: 16,
            background: '#111'
          }}
        >
          <h2 style={{ fontSize: 18, marginBottom: 8 }}>
            {d.subaccount_name}
          </h2>

          <div>Status: <b>{d.status}</b></div>
          <div>Strategy: {d.current_strategy}</div>
          <div>Balance: </div>
          <div>Win Rate: {(d.win_rate * 100).toFixed(1)}%</div>
          <div>RR: {d.avg_rr}</div>

          <hr style={{ margin: '12px 0', opacity: 0.2 }} />

          <div>Autonomy: {d.autonomy_ready ? 'ENABLED' : 'OFF'}</div>
          <div>Telegram: {d.telegram_enabled ? 'ON' : 'OFF'}</div>
          <div>Strategy Locked: {d.strategy_locked ? 'YES' : 'NO'}</div>
          <div>Frozen: {d.frozen ? 'YES' : 'NO'}</div>
        </div>
      ))}
    </div>
  );
}
