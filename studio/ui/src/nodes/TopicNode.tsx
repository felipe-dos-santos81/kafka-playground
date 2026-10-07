import { Handle, Position, type NodeProps } from '@xyflow/react'
import type { TopicNode as TopicNodeType } from './types'

export default function TopicNode({ data, selected }: NodeProps<TopicNodeType>) {
  return (
    <div className={`node topic${selected ? ' selected' : ''}`}>
      <div className="node-title">Topic</div>
      <div className="node-summary">
        {data.name || '(unnamed)'} · {data.partitions} partition{data.partitions === 1 ? '' : 's'}
      </div>
      <Handle type="target" position={Position.Left} />
      <Handle type="source" position={Position.Right} />
    </div>
  )
}
