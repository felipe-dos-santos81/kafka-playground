import { Handle, Position, type NodeProps } from '@xyflow/react'
import type { TransformNode as TransformNodeType } from './types'

export default function TransformNode({ data, selected }: NodeProps<TransformNodeType>) {
  return (
    <div className={`node transform${selected ? ' selected' : ''}`}>
      <div className="node-title">Transform</div>
      <div className="node-summary">{data.expr || '(no expression)'}</div>
      <Handle type="target" position={Position.Left} />
      <Handle type="source" position={Position.Right} />
    </div>
  )
}
