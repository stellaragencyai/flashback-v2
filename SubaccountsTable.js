
import React, { useEffect, useState } from 'react';

export default function SubaccountsTable() {
  const [rows, setRows] = useState([]);

  useEffect(() => {
    fetch('/subaccounts_state.json')
      .then(r => r.json())
      .then(setRows)
      .catch(() => setRows([]));
  }, []);

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
          {rows.map(r => (
            <tr key={r.subaccount_uid}>
              <td>{r.subaccount_uid}</td>
              <td>{r.subaccount_name}</td>
              <td>{r.current_strategy}</td>
              <td>
                <span className={'dot ' + r.status}></span>
                {r.status}
              </td>
              <td>{r.total_trades}</td>
              <td>{r.win_rate}%</td>
              <td>{r.avg_rr}</td>
              <td>{r.n_bucket}</td>
              <td></td>
              <td>{r.autonomy_ready ? 'READY' : 'LEARNING'}</td>
              <td>{r.telegram_enabled ? 'ON' : 'OFF'}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

