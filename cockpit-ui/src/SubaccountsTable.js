import React, { useEffect, useState } from 'react';

export default function SubaccountsTable() {
  const [rows, setRows] = useState([]);

  useEffect(() => {
    let alive = true;

    const load = async () => {
      try {
        const res = await fetch('/api/subaccounts', { cache: 'no-store' });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        if (alive) setRows(Array.isArray(data) ? data : []);
      } catch (e) {
        if (alive) setRows([]);
      }
    };

    // initial + poll
    load();
    const t = setInterval(load, 2000);

    return () => {
      alive = false;
      clearInterval(t);
    };
  }, []);

  const fmtWinPct = (v) => {
    const n = Number(v);
    if (!Number.isFinite(n)) return '0.0%';
    // API currently returns 0.xx, not 47.xx
    const pct = (n <= 1.0 ? n * 100.0 : n);
    return `${pct.toFixed(1)}%`;
  };

  const fmtMoney = (v) => {
    const n = Number(v);
    if (!Number.isFinite(n)) return '';
    return n.toFixed(2);
  };

  return (
    <div className='panel'>
      <h2>Subaccount Districts</h2>

      <table className='city-table'>
        <thead>
          <tr>
            <th>UID</th>
            <th>Name</th>
            <th>Strategy</th>
            <th>Status</th>
            <th>Trades</th>
            <th>Win %</th>
            <th>R:R</th>
            <th>N</th>
            <th>Balance</th>
            <th>Autonomy</th>
            <th>Telegram</th>
          </tr>
        </thead>

        <tbody>
          {rows.map((r) => (
            <tr key={r.subaccount_uid}>
              <td>{r.subaccount_uid}</td>
              <td>{r.subaccount_name}</td>
              <td>{r.current_strategy}</td>
              <td>
                <span className={'dot ' + r.status}></span>
                {r.status}
              </td>
              <td>{r.total_trades}</td>
              <td>{fmtWinPct(r.win_rate)}</td>
              <td>{r.avg_rr}</td>
              <td>{r.n_bucket}</td>
              <td>{fmtMoney(r.balance)}</td>
              <td>{r.autonomy_ready ? 'READY' : 'LEARNING'}</td>
              <td>{r.telegram_enabled ? 'ON' : 'OFF'}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
