import { useCallback, useEffect, useState } from 'react'
import { ReactFlowProvider, useEdgesState, useNodesState, useReactFlow, type Edge } from '@xyflow/react'
import Canvas from './Canvas'
import FlowList from './FlowList'
import Inspector from './Inspector'
import Palette from './Palette'
import { api, ApiError, type FlowSummary } from './flow/api'
import type { FlowFile } from './flow/schema'
import type { StudioNode } from './nodes/types'

function describe(e: unknown): string {
  return e instanceof ApiError ? `${e.status}: ${e.message}` : String(e)
}

function Studio() {
  const [nodes, setNodes, onNodesChange] = useNodesState<StudioNode>([])
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([])
  const { getViewport, setViewport } = useReactFlow()
  const [flows, setFlows] = useState<FlowSummary[]>([])
  const [current, setCurrent] = useState<{ id: string; name: string } | null>(null)
  const [selected, setSelected] = useState<string | null>(null)
  const [dirty, setDirty] = useState(false)
  const [error, setError] = useState('')

  const refresh = useCallback(() => api.list().then(setFlows).catch((e) => setError(describe(e))), [])
  useEffect(() => {
    refresh()
  }, [refresh])

  const load = async (id: string) => {
    try {
      const f = await api.get(id)
      setNodes(f.nodes as unknown as StudioNode[]) // the backend validated the per-type data
      setEdges(f.edges)
      setViewport(f.viewport ?? { x: 0, y: 0, zoom: 1 })
      setCurrent({ id: f.id, name: f.name })
      setSelected(null)
      setDirty(false)
      setError('')
    } catch (e) {
      setError(describe(e))
    }
  }

  // React Flow's shape is the file format: only drop runtime-only fields.
  const toFile = (id: string, name: string): FlowFile => ({
    id,
    name,
    nodes: nodes.map(({ id, type, position, data }) => ({ id, type: type!, position, data })),
    edges: edges.map(({ id, source, target }) => ({ id, source, target })),
    viewport: getViewport(),
  })

  const save = async () => {
    if (!current) return
    try {
      await api.save(toFile(current.id, current.name))
      setDirty(false)
      setError('')
      refresh()
    } catch (e) {
      setError(describe(e))
    }
  }

  const create = async () => {
    const name = window.prompt('Flow name')?.trim()
    if (!name) return
    try {
      const f = await api.create({ name, nodes: [], edges: [] })
      await refresh()
      await load(f.id)
    } catch (e) {
      setError(describe(e))
    }
  }

  const remove = async (id: string) => {
    if (!window.confirm('Delete this flow?')) return
    try {
      await api.remove(id)
      if (current?.id === id) {
        setCurrent(null)
        setNodes([])
        setEdges([])
      }
      refresh()
    } catch (e) {
      setError(describe(e))
    }
  }

  // Selection and measurement changes are not edits.
  const touch = (changes: { type: string }[]) => {
    if (changes.some((c) => c.type !== 'select' && c.type !== 'dimensions')) setDirty(true)
  }

  const updateData = (id: string, patch: Record<string, unknown>) => {
    setNodes((nds) => nds.map((n) => (n.id === id ? ({ ...n, data: { ...n.data, ...patch } } as StudioNode) : n)))
    setDirty(true)
  }

  const node = nodes.find((n) => n.id === selected) ?? null

  return (
    <div className="studio">
      <header className="topbar">
        <strong>Pipeline Studio</strong>
        {current && (
          <>
            <input
              value={current.name}
              onChange={(e) => {
                setCurrent({ ...current, name: e.target.value })
                setDirty(true)
              }}
            />
            <button onClick={save} disabled={!dirty}>
              {dirty ? 'Save' : 'Saved'}
            </button>
          </>
        )}
        {error && <span className="error">{error}</span>}
      </header>
      <aside className="side">
        <FlowList flows={flows} currentId={current?.id ?? null} onOpen={load} onCreate={create} onDelete={remove} />
        <Palette />
      </aside>
      <main className="canvas">
        {current ? (
          <Canvas
            nodes={nodes}
            edges={edges}
            onNodesChange={(c) => {
              touch(c)
              onNodesChange(c)
            }}
            onEdgesChange={(c) => {
              touch(c)
              onEdgesChange(c)
            }}
            setNodes={setNodes}
            setEdges={setEdges}
            onSelect={setSelected}
            onEdit={() => setDirty(true)}
          />
        ) : (
          <p className="hint" style={{ padding: 16 }}>
            Open a flow on the left or click New.
          </p>
        )}
      </main>
      <aside className="inspector">
        <Inspector node={node} onChange={updateData} />
      </aside>
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
