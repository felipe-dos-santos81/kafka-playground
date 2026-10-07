import { useCallback, type DragEvent, type Dispatch, type SetStateAction } from 'react'
import {
  Background,
  Controls,
  MiniMap,
  ReactFlow,
  addEdge,
  useReactFlow,
  type Connection,
  type Edge,
  type OnEdgesChange,
  type OnNodesChange,
  type Viewport,
} from '@xyflow/react'
import { DRAG_TYPE } from './Palette'
import { canConnect, defaultData, nextId, type NodeType } from './flow/schema'
import ProducerNode from './nodes/ProducerNode'
import TopicNode from './nodes/TopicNode'
import ConsumerNode from './nodes/ConsumerNode'
import TransformNode from './nodes/TransformNode'
import type { StudioNode } from './nodes/types'

const nodeTypes = { producer: ProducerNode, topic: TopicNode, consumer: ConsumerNode, transform: TransformNode }

type Props = {
  nodes: StudioNode[]
  edges: Edge[]
  onNodesChange: OnNodesChange<StudioNode>
  onEdgesChange: OnEdgesChange
  setNodes: Dispatch<SetStateAction<StudioNode[]>>
  setEdges: Dispatch<SetStateAction<Edge[]>>
  onSelect: (id: string | null) => void
  onEdit: () => void // called for edits that bypass onNodesChange/onEdgesChange (drop, connect)
  defaultViewport?: Viewport // initial viewport (read on mount); fits the view when absent
}

export default function Canvas({ nodes, edges, onNodesChange, onEdgesChange, setNodes, setEdges, onSelect, onEdit, defaultViewport }: Props) {
  const { screenToFlowPosition } = useReactFlow()

  // Only the pairs flow.go allows; the backend re-checks on save. addEdge drops duplicates itself.
  const isValidConnection = useCallback(
    (c: Edge | Connection) => {
      const s = nodes.find((n) => n.id === c.source)
      const t = nodes.find((n) => n.id === c.target)
      return !!s && !!t && s.id !== t.id && canConnect(s.type, t.type)
    },
    [nodes],
  )

  const onConnect = useCallback(
    (c: Connection) => {
      setEdges((eds) => addEdge(c, eds))
      onEdit()
    },
    [setEdges, onEdit],
  )

  const onDrop = useCallback(
    (e: DragEvent) => {
      e.preventDefault()
      const type = e.dataTransfer.getData(DRAG_TYPE) as NodeType
      if (!type) return
      const position = screenToFlowPosition({ x: e.clientX, y: e.clientY })
      setNodes((nds) => [...nds, { id: nextId(type, nds), type, position, data: defaultData(type) } as StudioNode])
      onEdit()
    },
    [screenToFlowPosition, setNodes, onEdit],
  )

  return (
    <ReactFlow
      nodes={nodes}
      edges={edges}
      nodeTypes={nodeTypes}
      onNodesChange={onNodesChange}
      onEdgesChange={onEdgesChange}
      onConnect={onConnect}
      isValidConnection={isValidConnection}
      onDrop={onDrop}
      onDragOver={(e) => {
        e.preventDefault()
        e.dataTransfer.dropEffect = 'move'
      }}
      onSelectionChange={({ nodes: sel }) => onSelect(sel[0]?.id ?? null)}
      deleteKeyCode={['Backspace', 'Delete']}
      defaultViewport={defaultViewport}
      fitView={!defaultViewport}
    >
      <Background />
      <Controls />
      <MiniMap />
    </ReactFlow>
  )
}
