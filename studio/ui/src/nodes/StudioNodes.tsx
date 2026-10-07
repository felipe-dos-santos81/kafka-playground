import type { ReactNode } from 'react'
import { Handle, Position, type NodeProps } from '@xyflow/react'
import { DEFAULT_INTERVAL_MS, hasInput, hasOutput, type NodeType } from '../flow/schema'
import type { ConsumerNode, ProducerNode, TopicNode, TransformNode } from './types'

// The frame every node shares: type as title, a one-line summary, and the
// input/output handles the allowed-edge table gives its type.
function Shell({ type, selected, children }: { type: NodeType; selected?: boolean; children: ReactNode }) {
  return (
    <div className={`node ${type}${selected ? ' selected' : ''}`}>
      <div className="node-title">{type}</div>
      <div className="node-summary">{children}</div>
      {hasInput(type) && <Handle type="target" position={Position.Left} />}
      {hasOutput(type) && <Handle type="source" position={Position.Right} />}
    </div>
  )
}

export const nodeTypes = {
  producer: ({ data, selected }: NodeProps<ProducerNode>) => (
    <Shell type="producer" selected={selected}>
      {data.source === 'timer' ? `timer · every ${data.interval_ms ?? DEFAULT_INTERVAL_MS} ms` : 'manual'}
    </Shell>
  ),
  topic: ({ data, selected }: NodeProps<TopicNode>) => (
    <Shell type="topic" selected={selected}>
      {data.name || '(unnamed)'} · {data.partitions} partition{data.partitions === 1 ? '' : 's'}
    </Shell>
  ),
  consumer: ({ data, selected }: NodeProps<ConsumerNode>) => (
    <Shell type="consumer" selected={selected}>
      {data.group || '(no group)'} · sink: {data.sink.kind}
    </Shell>
  ),
  transform: ({ data, selected }: NodeProps<TransformNode>) => (
    <Shell type="transform" selected={selected}>
      {data.expr || '(no expression)'}
    </Shell>
  ),
}
