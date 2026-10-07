import { createContext, useContext, type ReactNode } from 'react'
import { Handle, Position, type NodeProps } from '@xyflow/react'
import { DEFAULT_INTERVAL_MS, hasInput, hasOutput, type NodeType } from '../flow/schema'
import type { ConsumerNode, ProducerNode, TopicNode, TransformNode } from './types'

// Container state per deployed node id (running, exited, missing…); empty while the flow is stopped.
export const RuntimeContext = createContext<Record<string, string>>({})

// The frame every node shares: type as title (plus its container state while
// deployed), a one-line summary, and the input/output handles the allowed-edge
// table gives its type.
function Shell({ id, type, selected, children }: { id: string; type: NodeType; selected?: boolean; children: ReactNode }) {
  const state = useContext(RuntimeContext)[id]
  return (
    <div className={`node ${type}${selected ? ' selected' : ''}`}>
      <div className="node-title">
        {type} {state && <span className={`node-state ${state}`}>{state}</span>}
      </div>
      <div className="node-summary">{children}</div>
      {hasInput(type) && <Handle type="target" position={Position.Left} />}
      {hasOutput(type) && <Handle type="source" position={Position.Right} />}
    </div>
  )
}

export const nodeTypes = {
  producer: ({ id, data, selected }: NodeProps<ProducerNode>) => (
    <Shell id={id} type="producer" selected={selected}>
      {data.source === 'timer' ? `timer · every ${data.interval_ms ?? DEFAULT_INTERVAL_MS} ms` : 'manual'}
    </Shell>
  ),
  topic: ({ id, data, selected }: NodeProps<TopicNode>) => (
    <Shell id={id} type="topic" selected={selected}>
      {data.name || '(unnamed)'} · {data.partitions} partition{data.partitions === 1 ? '' : 's'}
    </Shell>
  ),
  consumer: ({ id, data, selected }: NodeProps<ConsumerNode>) => (
    <Shell id={id} type="consumer" selected={selected}>
      {data.group || '(no group)'} · sink: {data.sink.kind}
    </Shell>
  ),
  transform: ({ id, data, selected }: NodeProps<TransformNode>) => (
    <Shell id={id} type="transform" selected={selected}>
      {data.expr || '(no expression)'}
    </Shell>
  ),
}
