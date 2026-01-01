import { useEffect, useState } from 'react';

export default function GovernanceCockpit() {
  const [districts, setDistricts] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    async function load() {
      try {
        const res = await fetch('/api/subaccounts');
        if (!res.ok) {
          throw new Error('API returned ' + res.status);
        }
        const data = await res.json();
        setDistricts(data);
      } catch (err) {
        console.error('API ERROR:', err);
        setError('Failed to fetch subaccounts — backend offline or blocked.');
      } finally {
        setLoading(false);
      }
    }
    load();
  }, []);

  if (loading) {
    return <div className="p-6 text-lg">Loading districts…</div>;
  }

  if (error) {
    return (
      <div className="p-6 text-red-500 font-semibold">
        ⚠ {error}
      </div>
    );
  }

  return (
    <div className="p-6 grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
      {districts.map(d => (
        <div
          key={d.subaccount_uid}
          className="rounded-xl border p-4 shadow"
        >
          <h2 className="text-lg font-bold">{d.subaccount_name}</h2>
          <div className="text-sm opacity-80">Strategy: {d.current_strategy}</div>
          <div className="mt-2 text-sm">Balance: </div>
          <div className="text-sm">Win rate: {Math.round(d.win_rate * 100)}%</div>
          <div className="text-sm">Trades: {d.total_trades}</div>
        </div>
      ))}
    </div>
  );
}
