import React, { useEffect, useState } from "react";

const statusColors = {
  online: "#22c55e",
  warning: "#f59e0b",
  offline: "#ef4444"
};

const regimeColors = {
  trending: "#dbeafe",
  volatile: "#fef3c7",
  ranging: "#ede9fe",
  inefficient: "#fee2e2"
};

export default function SubaccountsTable() {
  const [rows, setRows] = useState([]);

  useEffect(() => {
    fetch("/subaccounts_state.json")
      .then(r => r.json())
      .then(setRows)
      .catch(() => setRows([]));
  }, []);

  return (
    <div style={styles.wrapper}>
      <h1 style={styles.title}>City Control Grid — Subaccount Bots</h1>
      <p style={styles.subtitle}>
        Autonomous districts extracting market value
      </p>

      <div style={styles.card}>
        <table style={styles.table}>
          <thead>
            <tr>
              <th>ID</th>
              <th>Strategy</th>
              <th>Pair</th>
              <th>Status</th>
              <th>Trades</th>
              <th>Win %</th>
              <th>Avg R:R</th>
              <th>N</th>
              <th>Regime</th>
              <th>AI</th>
              <th>Balance</th>
            </tr>
          </thead>
          <tbody>
            {rows.map(r => (
              <tr key={r.subaccount_id}>
                <td style={styles.id}>{r.subaccount_id}</td>
                <td>{r.strategy}</td>
                <td>{r.pair}</td>

                <td>
                  <span style={{
                    ...styles.pill,
                    background: statusColors[r.status] || "#94a3b8"
                  }}>
                    {r.status.toUpperCase()}
                  </span>
                </td>

                <td style={styles.num}>{r.trades}</td>
                <td style={{ color: "#16a34a" }}>{r.win_rate}%</td>
                <td>{r.avg_rr}</td>
                <td style={styles.num}>{r.n}</td>

                <td>
                  <span style={{
                    ...styles.regime,
                    background: regimeColors[r.regime] || "#e5e7eb"
                  }}>
                    {r.regime}
                  </span>
                </td>

                <td>
                  {r.ai_autonomy === "full" ? "🤖 Full" : "🧠 Assisted"}
                </td>

                <td style={styles.balance}>
                  ${Number(r.balance).toLocaleString()}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

const styles = {
  wrapper: {
    padding: "32px",
    fontFamily: "Inter, system-ui, sans-serif"
  },
  title: {
    fontSize: "22px",
    fontWeight: 700,
    marginBottom: "4px"
  },
  subtitle: {
    fontSize: "13px",
    color: "#6b7280",
    marginBottom: "20px"
  },
  card: {
    background: "#ffffff",
    borderRadius: "14px",
    boxShadow: "0 10px 30px rgba(0,0,0,0.06)",
    padding: "16px"
  },
  table: {
    width: "100%",
    borderCollapse: "collapse",
    fontSize: "13px"
  },
  id: {
    fontWeight: 600
  },
  num: {
    textAlign: "right"
  },
  balance: {
    textAlign: "right",
    fontWeight: 600
  },
  pill: {
    padding: "4px 10px",
    borderRadius: "999px",
    color: "#fff",
    fontSize: "11px",
    fontWeight: 600
  },
  regime: {
    padding: "4px 10px",
    borderRadius: "999px",
    fontSize: "11px",
    fontWeight: 600,
    color: "#111827"
  }
};
