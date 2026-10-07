import { createContext, useContext, type ReactNode } from 'react'
import { Handle, Position, type NodeProps } from '@xyflow/react'
import { containersOf, type NodeRuntime } from '../flow/api'
import { DEFAULT_INTERVAL_MS, hasInput, hasOutput, type NodeType } from '../flow/schema'
import type { ConsumerNode, ProducerNode, TopicNode, TransformNode } from './types'

// The live snapshot per node id while the flow runs; empty while it is stopped.
export const RuntimeContext = createContext<Record<string, NodeRuntime>>({})

// One line of live numbers under a deployed node's summary, or '' when there is
// nothing to say. A node's own counts show once one of its containers answered
// /stats (it has a boot): a container just started shows nothing yet, and one
// that does not answer shows its warning. Lag and partitions come from the broker.
function runtimeLine(type: NodeType, rt: NodeRuntime): string {
  if (type === 'topic') {
    return [`${rt.partitions ?? 0} partitions`, `end ${rt.endOffset ?? 0}`, rt.warning].filter(Boolean).join(' · ')
  }
  const answered = containersOf(rt).some((c) => c.boot)
  const parts = answered ? [`${rt.total ?? 0} msgs`, `${(rt.rate ?? 0).toFixed(1)}/s`] : []
  if (rt.errors) parts.push(`${rt.errors} errors`)
  if (rt.lag !== undefined) parts.push(`lag ${rt.lag}`)
  for (const c of containersOf(rt)) {
    const held = Object.values(c.assigned ?? {}).flat() // partitions this container's client holds
    if (held.length > 0) parts.push(`${c.instance ? `#${c.instance} ` : ''}p${held.join(',')}`)
  }
  if (rt.warning) parts.push(rt.warning)
  return parts.join(' · ')
}

// The frame every node shares: type as title (plus its state while deployed, or
// how many of its instances run), a one-line summary, a line of live numbers while
// deployed (the last error as its tooltip), and the input/output handles the
// allowed-edge table gives its type.
function Shell({ id, type, selected, children }: { id: string; type: NodeType; selected?: boolean; children: ReactNode }) {
  const rt = useContext(RuntimeContext)[id]
  const containers = rt ? containersOf(rt) : []
  const runningCount = containers.filter((c) => c.state === 'running').length
  const badge = rt?.instances ? `${runningCount}/${containers.length} running` : rt?.state
  const badgeClass = rt?.instances ? (runningCount === containers.length ? 'running' : 'exited') : rt?.state
  const line = rt ? runtimeLine(type, rt) : ''
  return (
    <div data-testid={`node-${id}`} className={`node ${type}${selected ? ' selected' : ''}`} title={rt?.lastError || undefined}>
      <div className="node-title">
        {type} {rt && <span className={`node-state ${badgeClass}`}>{badge}</span>}
      </div>
      <div className="node-summary">{children}</div>
      {line && (
        <div data-testid={`runtime-${id}`} className="node-runtime">
          {line}
        </div>
      )}
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
