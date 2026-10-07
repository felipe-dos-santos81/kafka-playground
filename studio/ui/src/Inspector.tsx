import type { StudioNode } from './nodes/types'

type Props = {
  node: StudioNode | null
  onChange: (id: string, patch: Record<string, unknown>) => void
}

export default function Inspector({ node, onChange }: Props) {
  if (!node) return <p className="hint">Select a node to edit it.</p>
  const set = (patch: Record<string, unknown>) => onChange(node.id, patch)

  return (
    <div>
      <h2>
        {node.type} <small>{node.id}</small>
      </h2>
      {node.type === 'producer' && (
        <>
          <label>Source</label>
          <select
            value={node.data.source}
            onChange={(e) =>
              set({
                source: e.target.value,
                ...(e.target.value === 'timer' && { interval_ms: node.data.interval_ms ?? 1000 }),
              })
            }
          >
            <option value="manual">manual</option>
            <option value="timer">timer</option>
          </select>
          {node.data.source === 'timer' && (
            <>
              <label>Interval (ms)</label>
              <input
                type="number"
                min={10}
                value={node.data.interval_ms ?? 1000}
                onChange={(e) => set({ interval_ms: Number(e.target.value) })}
              />
            </>
          )}
          <label>Key template</label>
          <input
            value={node.data.key}
            placeholder="{{.Seq}} — empty means keyless"
            onChange={(e) => set({ key: e.target.value })}
          />
          <label>Value template (renders JSON)</label>
          <textarea value={node.data.value} onChange={(e) => set({ value: e.target.value })} />
          <p className="hint">Templates may use {'{{.Seq}}'}, {'{{.Now}}'} and {'{{.Rand}}'}.</p>
        </>
      )}
      {node.type === 'topic' && (
        <>
          <label>Name</label>
          <input value={node.data.name} onChange={(e) => set({ name: e.target.value })} />
          <label>Partitions</label>
          <input
            type="number"
            min={1}
            value={node.data.partitions}
            onChange={(e) => set({ partitions: Number(e.target.value) })}
          />
          <label>Replication factor</label>
          <input
            type="number"
            min={1}
            max={1}
            value={node.data.replication_factor}
            onChange={(e) => set({ replication_factor: Number(e.target.value) })}
          />
          <p className="hint">Single broker: replication factor stays 1.</p>
        </>
      )}
      {node.type === 'consumer' && (
        <>
          <label>Group id</label>
          <input value={node.data.group} onChange={(e) => set({ group: e.target.value })} />
          <label>auto.offset.reset</label>
          <select
            value={node.data.auto_offset_reset}
            onChange={(e) => set({ auto_offset_reset: e.target.value })}
          >
            <option value="earliest">earliest</option>
            <option value="latest">latest</option>
          </select>
          <label>Sink</label>
          <select value={node.data.sink.kind} disabled>
            <option value="log">log (tail panel)</option>
          </select>
        </>
      )}
      {node.type === 'transform' && (
        <>
          <label>Expression</label>
          <textarea value={node.data.expr} onChange={(e) => set({ expr: e.target.value })} />
          <p className="hint">An expr-lang expression over msg; it runs once deploy lands (M5).</p>
        </>
      )}
    </div>
  )
}
