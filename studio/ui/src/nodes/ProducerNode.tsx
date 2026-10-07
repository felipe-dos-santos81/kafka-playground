import { Handle, Position, type NodeProps } from '@xyflow/react'
import type { ProducerNode as ProducerNodeType } from './types'

export default function ProducerNode({ data, selected }: NodeProps<ProducerNodeType>) {
  const summary = data.source === 'timer' ? `timer · every ${data.interval_ms ?? 1000} ms` : 'manual'
  return (
    <div className={`node producer${selected ? ' selected' : ''}`}>
      <div className="node-title">Producer</div>
      <div className="node-summary">{summary}</div>
      <Handle type="source" position={Position.Right} />
    </div>
  )
}
