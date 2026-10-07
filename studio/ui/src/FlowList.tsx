import type { FlowSummary } from './flow/api'

type Props = {
  flows: FlowSummary[]
  currentId: string | null
  onOpen: (id: string) => void
  onCreate: () => void
  onDelete: (id: string) => void
}

export default function FlowList({ flows, currentId, onOpen, onCreate, onDelete }: Props) {
  return (
    <section>
      <h2>
        Flows <button onClick={onCreate}>New</button>
      </h2>
      <ul className="flows">
        {flows.map((f) => (
          <li key={f.id} className={f.id === currentId ? 'current' : ''}>
            <span onClick={() => onOpen(f.id)}>
              {f.name} <small>({f.status})</small>
            </span>
            <button onClick={() => onDelete(f.id)} title="Delete flow">
              ×
            </button>
          </li>
        ))}
      </ul>
      {flows.length === 0 && <p className="hint">No flows yet. Click New.</p>}
    </section>
  )
}
