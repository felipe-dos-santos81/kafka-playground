import { useCallback, useEffect, useState } from 'react'
import { ReactFlowProvider, useEdgesState, useNodesState, useReactFlow, type Edge, type Viewport } from '@xyflow/react'
import Canvas from './Canvas'
import FlowList from './FlowList'
import Inspector from './Inspector'
import Palette from './Palette'
import { api, ApiError, type FlowSummary } from './flow/api'
import { fillDefaults, type FlowFile } from './flow/schema'
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
  const [dirty, setDirty] = useState(false)
  const [error, setError] = useState('')

  const refresh = useCallback(() => api.list().then(setFlows).catch((e) => setError(describe(e))), [])
  useEffect(() => {
    refresh()
  }, [refresh])

  // Runs an API action, showing any failure in the top bar.
  const withTopBarError = async (action: () => Promise<void>) => {
    try {
      await action()
    } catch (e) {
      setError(describe(e))
    }
  }

  const load = (id: string) =>
    withTopBarError(async () => {
      const f = await api.get(id)
      // Hand-edited files may omit data fields: fill them from the defaults so the node
      // components never crash, and mark the flow unsaved so the difference is visible.
      const loaded = f.nodes.map(fillDefaults)
      setNodes(loaded.map((l) => l.node) as unknown as StudioNode[])
      setEdges(f.edges)
      setCurrent({ id: f.id, name: f.name, viewport: f.viewport ?? undefined }) // Canvas mounts per flow and reads it as defaultViewport
      setDirty(loaded.some((l) => l.filled))
      setError('')
    })

  // React Flow's shape is the file format: only drop runtime-only fields.
  const toFile = (id: string, name: string): FlowFile => ({
    id,
    name,
    nodes: nodes.map(({ id, type, position, data }) => ({ id, type: type!, position, data })),
    edges: edges.map(({ id, source, target }) => ({ id, source, target })),
    viewport: getViewport(),
  })

  const save = () =>
    withTopBarError(async () => {
      if (!current) return
      await api.save(toFile(current.id, current.name))
      setDirty(false)
      setError('')
      refresh()
    })

  const discard = () => !dirty || window.confirm('Discard unsaved changes?')

  const open = (id: string) => {
    if (discard()) load(id)
  }

  const create = async () => {
    if (!discard()) return
    const name = window.prompt('Flow name')?.trim()
    if (!name) return
    await withTopBarError(async () => {
      const f = await api.create({ name, nodes: [], edges: [] })
      await refresh()
      await load(f.id)
    })
  }

  const remove = async (id: string) => {
    if (!window.confirm('Delete this flow?')) return
    await withTopBarError(async () => {
      await api.remove(id)
      if (current?.id === id) {
        setCurrent(null)
        setDirty(false)
        setNodes([])
        setEdges([])
      }
      refresh()
    })
  }

  // Selection and measurement changes are not edits.
  const markDirtyOnEdit = (changes: { type: string }[]) => {
    if (changes.some((c) => c.type !== 'select' && c.type !== 'dimensions')) setDirty(true)
  }

  const updateData = (id: string, patch: Record<string, unknown>) => {
    setNodes((nds) => nds.map((n) => (n.id === id ? ({ ...n, data: { ...n.data, ...patch } } as StudioNode) : n)))
    setDirty(true)
  }

  // React Flow tracks selection on the nodes; the inspector edits exactly one.
  const picked = nodes.filter((n) => n.selected)
  const node = picked.length === 1 ? picked[0] : null

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
              markDirtyOnEdit(c)
              onNodesChange(c)
            }}
            onEdgesChange={(c) => {
              markDirtyOnEdit(c)
              onEdgesChange(c)
            }}
            setNodes={setNodes}
            setEdges={setEdges}
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
