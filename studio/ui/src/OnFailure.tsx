import { useEdges, useNodes, type Edge } from '@xyflow/react'
import type { ConsumerData, StudioNode } from './nodes/types'

type Props = {
  id: string // the consumer's node id
  data: ConsumerData
  set: (patch: Partial<ConsumerData>) => void
}

const DEFAULT_RETRY = { attempts: 3, delay_ms: 5000 }

// inputTopic is the name of the topic a consumer reads, '' while it reads none
// (or one not named yet).
function inputTopic(id: string, nodes: StudioNode[], edges: Edge[]): string {
  const from = edges.find((e) => e.target === id)?.source
  const topic = nodes.find((n) => n.id === from)
  return topic?.type === 'topic' ? topic.data.name : ''
}

// A consumer's failure handling (Go: ConsumerData's Retry and DLQ): retry through
// <input>__retry, then the DLQ <input>__dlq. The topics are named after the input
// topic and created on deploy, which refuses a retry without the DLQ.
export default function OnFailure({ id, data, set }: Props) {
  const input = inputTopic(id, useNodes<StudioNode>(), useEdges())
  const topic = (suffix: string) => <p className="hint">{input ? `${input}${suffix}` : 'wire a topic first'}</p>
  const retry = data.retry ?? null
  return (
    <fieldset className="on-failure">
      <legend>On failure</legend>
      <label htmlFor="inspector-retry">Retry</label>
      <input id="inspector-retry"
        type="checkbox"
        checked={retry !== null}
        onChange={(e) => set({ retry: e.target.checked ? DEFAULT_RETRY : undefined })}
      />
      {retry && (
        <>
          <label htmlFor="inspector-attempts">Attempts</label>
          <input id="inspector-attempts"
            type="number"
            min={1}
            max={10}
            value={retry.attempts}
            onChange={(e) => set({ retry: { ...retry, attempts: Number(e.target.value) } })}
          />
          <label htmlFor="inspector-delay-ms">Delay (ms)</label>
          <input id="inspector-delay-ms"
            type="number"
            min={100}
            max={60000}
            value={retry.delay_ms}
            onChange={(e) => set({ retry: { ...retry, delay_ms: Number(e.target.value) } })}
          />
        </>
      )}
      {topic('__retry')}
      <label htmlFor="inspector-dlq">DLQ</label>
      <input id="inspector-dlq" type="checkbox" checked={data.dlq ?? false} onChange={(e) => set({ dlq: e.target.checked || undefined })} />
      {topic('__dlq')}
      <p className="hint">
        A failed sink or forward is tried again after the delay, up to Attempts times, then goes to the DLQ; a failed
        transform or router goes there at once. Retry needs the DLQ.
      </p>
    </fieldset>
  )
}
