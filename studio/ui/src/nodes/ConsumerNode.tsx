import { Handle, Position, type NodeProps } from '@xyflow/react'
import type { ConsumerNode as ConsumerNodeType } from './types'

export default function ConsumerNode({ data, selected }: NodeProps<ConsumerNodeType>) {
  return (
    <div className={`node consumer${selected ? ' selected' : ''}`}>
      <div className="node-title">Consumer</div>
      <div className="node-summary">
        {data.group || '(no group)'} · sink: {data.sink.kind}
      </div>
      <Handle type="target" position={Position.Left} />
      <Handle type="source" position={Position.Right} />
    </div>
  )
}
