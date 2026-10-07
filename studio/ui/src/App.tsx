import { useState } from 'react'
import { ReactFlowProvider, useEdgesState, useNodesState, type Edge } from '@xyflow/react'
import Canvas from './Canvas'
import Palette from './Palette'
import type { StudioNode } from './nodes/types'

function Studio() {
  const [nodes, setNodes, onNodesChange] = useNodesState<StudioNode>([])
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([])
  const [selected, setSelected] = useState<string | null>(null)

  return (
    <div className="studio">
      <header className="topbar">
        <strong>Pipeline Studio</strong>
      </header>
      <aside className="side">
        <Palette />
      </aside>
      <main className="canvas">
        <Canvas
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          setNodes={setNodes}
          setEdges={setEdges}
          onSelect={setSelected}
          onEdit={() => {}}
        />
      </main>
      <aside className="inspector">{selected ? `Selected: ${selected}` : 'Select a node.'}</aside>
    </div>
  )
}

export default function App() {
  return (
    <ReactFlowProvider>
      <Studio />
    </ReactFlowProvider>
  )
}
