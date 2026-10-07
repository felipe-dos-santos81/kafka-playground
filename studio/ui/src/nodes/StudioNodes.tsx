import { createContext, useContext, type ReactNode } from 'react'
import { Handle, Position, type NodeProps } from '@xyflow/react'
import type { NodeRuntime } from '../flow/api'
import { DEFAULT_INTERVAL_MS, hasInput, hasOutput, type NodeType } from '../flow/schema'
import type { ConsumerNode, ProducerNode, TopicNode, TransformNode } from './types'

// The live snapshot per node id while the flow runs; empty while it is stopped.
export const RuntimeContext = createContext<Record<string, NodeRuntime>>({})

// One line of live numbers under a deployed node's summary.
function runtimeLine(type: NodeType, rt: NodeRuntime): string {
  if (type === 'topic') {
    return [`${rt.partitions ?? 0} partitions`, `end ${rt.endOffset ?? 0}`, rt.warning].filter(Boolean).join(' · ')
  }
  const parts = [`${rt.total ?? 0} msgs`, `${(rt.rate ?? 0).toFixed(1)}/s`]
  if (rt.errors) parts.push(`${rt.errors} errors`)
  if (rt.lag !== undefined) parts.push(`lag ${rt.lag}`)
  const held = Object.values(rt.assigned ?? {}).flat()
  if (held.length > 0) parts.push(`p${held.join(',')}`)
  return parts.join(' · ')
}

// The frame every node shares: type as title (plus its state while deployed), a
// one-line summary, a line of live numbers while deployed (the last error as its
// tooltip), and the input/output handles the allowed-edge table gives its type.
function Shell({ id, type, selected, children }: { id: string; type: NodeType; selected?: boolean; children: ReactNode }) {
  const rt = useContext(RuntimeContext)[id]
  return (
    <div className={`node ${type}${selected ? ' selected' : ''}`} title={rt?.lastError || undefined}>
      <div className="node-title">
        {type} {rt && <span className={`node-state ${rt.state}`}>{rt.state}</span>}
      </div>
      <div className="node-summary">{children}</div>
      {rt && <div className="node-runtime">{runtimeLine(type, rt)}</div>}
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
