import GovernanceCockpit from './GovernanceCockpit';

export default function App() {
  return (
    <div style={{ minHeight: '100vh', padding: '24px' }}>
      <header style={{ marginBottom: '24px' }}>
        <h1 style={{ fontSize: '28px', fontWeight: 'bold' }}>
          Flashback — District Governance Cockpit
        </h1>
        <p style={{ opacity: 0.7 }}>
          Autonomous subaccount oversight & real-time governance
        </p>
      </header>

      <main>
        <GovernanceCockpit />
      </main>
    </div>
  );
}
