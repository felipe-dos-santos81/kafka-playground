import { DEFAULT_INTERVAL_MS } from './flow/schema'
import type { StudioNode } from './nodes/types'

type Props = {
  node: StudioNode | null
  flowId?: string // the open flow, for the producer's webhook line
  onChange: (id: string, patch: Record<string, unknown>) => void
}

export default function Inspector({ node, flowId, onChange }: Props) {
  if (!node) return <p className="hint">Select a node to edit it.</p>
  const set = (patch: Record<string, unknown>) => onChange(node.id, patch)

  return (
    <div>
      <h2>
        {node.type} <small>{node.id}</small>
      </h2>
      {node.type === 'producer' && (
        <>
          <label htmlFor="inspector-source">Source</label>
          <select id="inspector-source"
            value={node.data.source}
            onChange={(e) =>
              set({
                source: e.target.value,
                ...(e.target.value === 'timer' && { interval_ms: node.data.interval_ms ?? DEFAULT_INTERVAL_MS }),
              })
            }
          >
            <option value="manual">manual</option>
            <option value="timer">timer</option>
          </select>
          {node.data.source === 'timer' && (
            <>
              <label htmlFor="inspector-interval-ms">Interval (ms)</label>
              <input id="inspector-interval-ms"
                type="number"
                min={10}
                value={node.data.interval_ms ?? DEFAULT_INTERVAL_MS}
                onChange={(e) => set({ interval_ms: Number(e.target.value) })}
              />
            </>
          )}
          <label htmlFor="inspector-key-template">Key template</label>
          <input id="inspector-key-template"
            value={node.data.key}
            placeholder="{{.Seq}} — empty means keyless"
            onChange={(e) => set({ key: e.target.value })}
          />
          <label htmlFor="inspector-value-template-renders-json">Value template (renders JSON)</label>
          <textarea id="inspector-value-template-renders-json" value={node.data.value} onChange={(e) => set({ value: e.target.value })} />
          <p className="hint">Templates may use {'{{.Seq}}'}, {'{{.Now}}'} and {'{{.Rand}}'}.</p>
          {flowId && (
            <>
              <label>Webhook</label>
              <code className="curl">{`curl -X POST '${location.origin}/api/flows/${flowId}/nodes/${node.id}/send?key=k1' --data '{"id": 1}'`}</code>
              <p className="hint">While the flow runs, the body is produced as the value. Other nodes reach the studio at http://studio:8082.</p>
            </>
          )}
        </>
      )}
      {node.type === 'topic' && (
        <>
          <label htmlFor="inspector-name">Name</label>
          <input id="inspector-name" value={node.data.name} onChange={(e) => set({ name: e.target.value })} />
          <label htmlFor="inspector-partitions">Partitions</label>
          <input id="inspector-partitions"
            type="number"
            min={1}
            value={node.data.partitions}
            onChange={(e) => set({ partitions: Number(e.target.value) })}
          />
          <label htmlFor="inspector-replication-factor">Replication factor</label>
          <input id="inspector-replication-factor"
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
          <label htmlFor="inspector-group-id">Group id</label>
          <input id="inspector-group-id" value={node.data.group} onChange={(e) => set({ group: e.target.value })} />
          <label htmlFor="inspector-auto-offset-reset">auto.offset.reset</label>
          <select id="inspector-auto-offset-reset"
            value={node.data.auto_offset_reset}
            onChange={(e) => set({ auto_offset_reset: e.target.value })}
          >
            <option value="earliest">earliest</option>
            <option value="latest">latest</option>
          </select>
          <label htmlFor="inspector-instances">Instances</label>
          <input id="inspector-instances"
            type="number"
            min={1}
            max={10}
            value={node.data.instances ?? 1}
            onChange={(e) => set({ instances: Number(e.target.value) })}
          />
          <p className="hint">Containers in the group (1–10); they split the topic's partitions.</p>
          <label htmlFor="inspector-sink">Sink</label>
          <select id="inspector-sink"
            value={node.data.sink.kind}
            onChange={(e) =>
              set({ sink: e.target.value === 'http' ? { kind: 'http', url: node.data.sink.url ?? '' } : { kind: 'log' } })
            }
          >
            <option value="log">log: the tail and the container log</option>
            <option value="http">http: POST each value</option>
          </select>
          {node.data.sink.kind === 'http' && (
            <>
              <label htmlFor="inspector-url">URL</label>
              <input id="inspector-url"
                value={node.data.sink.url ?? ''}
                placeholder="http://studio:8082/api/flows/<id>/nodes/<node>/send"
                onChange={(e) => set({ sink: { kind: 'http', url: e.target.value } })}
              />
              <p className="hint">Each value is POSTed as JSON within 5 s; any answer but 2xx counts as an error.</p>
            </>
          )}
          <p className="hint">Wire it to a topic to forward every record there with the same key, or through a Transform to reshape or drop records first.</p>
        </>
      )}
      {node.type === 'transform' && (
        <>
          <label htmlFor="inspector-expression">Expression</label>
          <textarea id="inspector-expression" value={node.data.expr} onChange={(e) => set({ expr: e.target.value })} />
          <p className="hint">An expr-lang expression over msg, the record's value decoded from JSON, e.g. {'{id: msg.id, total: msg.qty * msg.price}'}. Its result is forwarded with the same key; nil drops the record. Deploy checks that it compiles.</p>
        </>
      )}
    </div>
  )
}
