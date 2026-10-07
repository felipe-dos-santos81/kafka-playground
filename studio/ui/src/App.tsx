import { useCallback, useEffect, useState } from 'react'
import { ReactFlowProvider, useEdgesState, useNodesState, useReactFlow, type Edge, type Viewport } from '@xyflow/react'
import Canvas from './Canvas'
import FlowList from './FlowList'
import Inspector from './Inspector'
import Palette from './Palette'
import { api, ApiError, type FlowSummary } from './flow/api'
import { defaultData, type FlowFile } from './flow/schema'
import type { StudioNode } from './nodes/types'

function describe(e: unknown): string {
  return e instanceof ApiError ? `${e.status}: ${e.message}` : String(e)
}

function Studio() {
  const [nodes, setNodes, onNodesChange] = useNodesState<StudioNode>([])
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([])
  const { getViewport } = useReactFlow()
  const [flows, setFlows] = useState<FlowSummary[]>([])
  const [current, setCurrent] = useState<{ id: string; name: string; viewport?: Viewport } | null>(null)
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
      // Hand-edited files may omit data fields: fill them from the defaults so the node components never crash.
      setNodes(f.nodes.map((n) => ({ ...n, data: { ...defaultData(n.type), ...n.data } })) as unknown as StudioNode[])
      setEdges(f.edges)
      setCurrent({ id: f.id, name: f.name, viewport: f.viewport ?? undefined }) // Canvas mounts per flow and reads it as defaultViewport
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

  const discard = () => !dirty || window.confirm('Discard unsaved changes?')

  const open = (id: string) => {
    if (discard()) load(id)
  }

  const create = async () => {
    if (!discard()) return
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
        setDirty(false)
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
        <FlowList flows={flows} currentId={current?.id ?? null} onOpen={open} onCreate={create} onDelete={remove} />
        <Palette />
      </aside>
      <main className="canvas">
        {current ? (
          <Canvas
            key={current.id}
            defaultViewport={current.viewport}
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
