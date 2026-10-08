import { useCallback, useContext, useMemo, useState, type DragEvent, type Dispatch, type SetStateAction } from 'react'
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
import { RuntimeContext, nodeTypes } from './nodes/StudioNodes'
import type { RouterData, StudioNode } from './nodes/types'

type Props = {
  nodes: StudioNode[]
  edges: Edge[]
  onNodesChange: OnNodesChange<StudioNode>
  onEdgesChange: OnEdgesChange
  setNodes: Dispatch<SetStateAction<StudioNode[]>>
  setEdges: Dispatch<SetStateAction<Edge[]>>
  defaultViewport?: Viewport // initial viewport (read on mount); fits the view when absent and the flow has nodes
}

// The label on a router's edge to target: the rules that send there (#1, #2) and
// the default, each with its count while the flow runs (#1 · 80).
function routeLabel(data: RouterData, target: string, branches?: number[]): string {
  const tos = [...data.rules.map((r) => r.to), data.default] // in the order of branches: the rules', then the default's
  return tos
    .flatMap((to, i) => {
      if (to !== target) return []
      const name = i < data.rules.length ? `#${i + 1}` : 'default'
      return [branches ? `${name} · ${branches[i] ?? 0}` : name]
    })
    .join(', ')
}

export default function Canvas({ nodes, edges, onNodesChange, onEdgesChange, setNodes, setEdges, defaultViewport }: Props) {
  const { screenToFlowPosition, getNode, getEdges } = useReactFlow()
  const runtime = useContext(RuntimeContext)
  // Fit only a flow opened with nodes: on an empty canvas React Flow would fit
  // (and zoom in on) the first node dropped.
  const [fitOnOpen] = useState(() => !defaultViewport && nodes.length > 0)

  // Only the pairs flow.go allows and no self edges; the backend re-checks on save.
  // addEdge drops duplicates itself.
  const isValidConnection = useCallback(
    (c: Edge | Connection) => c.source !== c.target && canConnect(getNode(c.source)?.type, getNode(c.target)?.type),
    [getNode],
  )

  // A new edge from a router adds a rule for its topic, with a condition to fill in
  // (Deploy refuses an empty one). A duplicate edge, which addEdge drops, adds none.
  const onConnect = useCallback(
    (c: Connection) => {
      const isNew = !getEdges().some((e) => e.source === c.source && e.target === c.target)
      setEdges((eds) => addEdge(c, eds))
      if (!isNew || getNode(c.source)?.type !== 'router') return
      setNodes((nds) =>
        nds.map((n) => (n.id === c.source && n.type === 'router' ? { ...n, data: { ...n.data, rules: [...n.data.rules, { when: '', to: c.target }] } } : n)),
      )
    },
    [getEdges, getNode, setEdges, setNodes],
  )

  // A router's edges carry labels worked out from its rules, never saved.
  const shownEdges = useMemo(
    () =>
      edges.map((e) => {
        const router = nodes.find((n) => n.id === e.source)
        return router?.type === 'router' ? { ...e, label: routeLabel(router.data, e.target, runtime[router.id]?.branches) } : e
      }),
    [edges, nodes, runtime],
  )

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
      edges={shownEdges}
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
