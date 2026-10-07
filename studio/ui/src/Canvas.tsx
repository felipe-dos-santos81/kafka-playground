import { useCallback, useState, type DragEvent, type Dispatch, type SetStateAction } from 'react'
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
import { nodeTypes } from './nodes/StudioNodes'
import type { StudioNode } from './nodes/types'

type Props = {
  nodes: StudioNode[]
  edges: Edge[]
  onNodesChange: OnNodesChange<StudioNode>
  onEdgesChange: OnEdgesChange
  setNodes: Dispatch<SetStateAction<StudioNode[]>>
  setEdges: Dispatch<SetStateAction<Edge[]>>
  defaultViewport?: Viewport // initial viewport (read on mount); fits the view when absent and the flow has nodes
}

export default function Canvas({ nodes, edges, onNodesChange, onEdgesChange, setNodes, setEdges, defaultViewport }: Props) {
  const { screenToFlowPosition, getNode } = useReactFlow()
  // Fit only a flow opened with nodes: on an empty canvas React Flow would fit
  // (and zoom in on) the first node dropped.
  const [fitOnOpen] = useState(() => !defaultViewport && nodes.length > 0)

  // Only the pairs flow.go allows and no self edges; the backend re-checks on save.
  // addEdge drops duplicates itself.
  const isValidConnection = useCallback(
    (c: Edge | Connection) => c.source !== c.target && canConnect(getNode(c.source)?.type, getNode(c.target)?.type),
    [getNode],
  )

  const onConnect = useCallback((c: Connection) => setEdges((eds) => addEdge(c, eds)), [setEdges])

  const onDrop = useCallback(
    (e: DragEvent) => {
      e.preventDefault()
      const type = e.dataTransfer.getData(DRAG_TYPE) as NodeType
      if (!type) return
      const position = screenToFlowPosition({ x: e.clientX, y: e.clientY })
      setNodes((nds) => [...nds, { id: nextId(type, nds), type, position, data: defaultData(type) } as StudioNode])
    },
    [screenToFlowPosition, setNodes],
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
      deleteKeyCode={['Backspace', 'Delete']}
      defaultViewport={defaultViewport}
      fitView={fitOnOpen}
    >
      <Background />
      <Controls />
      <MiniMap />
    </ReactFlow>
  )
}
