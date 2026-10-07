import { useCallback, useEffect, useState } from 'react'
import { ReactFlowProvider, useEdgesState, useNodesState, useReactFlow, type Edge, type Viewport } from '@xyflow/react'
import Canvas from './Canvas'
import FlowList from './FlowList'
import Inspector from './Inspector'
import Palette from './Palette'
import { api, describe, watch, type FlowState, type FlowSummary, type NodeRuntime } from './flow/api'
import { fileContent, fillDefaults, type CanvasEdge, type CanvasNode } from './flow/schema'
import TailDrawer from './TailDrawer'
import { RuntimeContext } from './nodes/StudioNodes'
import type { StudioNode } from './nodes/types'

// Sorts object keys while stringifying, so a snapshot compares values, not key order.
const sortKeys = (_: string, v: unknown) =>
  v && typeof v === 'object' && !Array.isArray(v) ? Object.fromEntries(Object.entries(v).sort(([a], [b]) => (a < b ? -1 : 1))) : v

// Compared to tell unsaved edits; the viewport is left out because panning is not an edit.
const snapshot = (name: string, nodes: CanvasNode[], edges: CanvasEdge[]) =>
  JSON.stringify({ name, ...fileContent(nodes, edges) }, sortKeys)

const NO_NODES: Record<string, NodeRuntime> = {}

function Studio() {
  const [nodes, setNodes, onNodesChange] = useNodesState<StudioNode>([])
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([])
  const { getViewport } = useReactFlow()
  const [flows, setFlows] = useState<FlowSummary[]>([])
  const [current, setCurrent] = useState<{ id: string; name: string; viewport?: Viewport } | null>(null)
  const [savedSnapshot, setSavedSnapshot] = useState('') // the flow as last loaded or saved
  const [error, setError] = useState('')
  const [flowState, setFlowState] = useState<FlowState | null>(null)
  const [deployedSnapshot, setDeployedSnapshot] = useState('') // savedSnapshot at the last Deploy from this page
  const dirty = current !== null && snapshot(current.name, nodes, edges) !== savedSnapshot

  const refresh = useCallback(() => api.list().then(setFlows).catch((e) => setError(describe(e))), [])
  useEffect(() => {
    refresh()
  }, [refresh])

  // The open flow's live snapshot: one `tick` a second over SSE (spec 3.6).
  const flowId = current?.id
  useEffect(() => {
    setFlowState(null)
    if (!flowId) return
    return watch(flowId, setFlowState)
  }, [flowId])
  const running = flowState?.status === 'running'

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
      // components never crash. The snapshot is the file as stored, so a fill shows as unsaved.
      setNodes(f.nodes.map(fillDefaults) as unknown as StudioNode[])
      setEdges(f.edges)
      setCurrent({ id: f.id, name: f.name, viewport: f.viewport ?? undefined }) // Canvas mounts per flow and reads it as defaultViewport
      setSavedSnapshot(snapshot(f.name, f.nodes, f.edges))
      setDeployedSnapshot('')
      setError('')
    })

  const save = () =>
    withTopBarError(async () => {
      if (!current) return
      const c = fileContent(nodes, edges)
      await api.save({ id: current.id, name: current.name, ...c, viewport: getViewport() })
      setSavedSnapshot(snapshot(current.name, c.nodes, c.edges)) // edits made while the request ran stay unsaved
      setError('')
      refresh()
    })

  // Deploy runs the saved file, so the button waits for Save.
  const deploy = () =>
    withTopBarError(async () => {
      if (!current) return
      await api.deploy(current.id)
      setDeployedSnapshot(savedSnapshot)
      setError('')
      refresh()
    })

  const stop = () =>
    withTopBarError(async () => {
      if (!current) return
      await api.stop(current.id)
      setDeployedSnapshot('')
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
        setNodes([])
        setEdges([])
      }
      refresh()
    })
  }

  const updateData = (id: string, patch: Record<string, unknown>) => {
    setNodes((nds) => nds.map((n) => (n.id === id ? ({ ...n, data: { ...n.data, ...patch } } as StudioNode) : n)))
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
              onChange={(e) => setCurrent({ ...current, name: e.target.value })}
            />
            <button onClick={save} disabled={!dirty}>
              {dirty ? 'Save' : 'Saved'}
            </button>
            <button onClick={deploy} disabled={dirty || running} title={dirty ? 'Save before deploying' : undefined}>
              Deploy
            </button>
            <button onClick={stop} disabled={!running}>
              Stop
            </button>
            <span className="status">{running ? 'running' : 'stopped'}</span>
            {running && deployedSnapshot !== '' && savedSnapshot !== deployedSnapshot && (
              <span className="hint">saved changes apply on redeploy</span>
            )}
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
          <RuntimeContext.Provider value={flowState?.nodes ?? NO_NODES}>
            <Canvas
              key={current.id}
              defaultViewport={current.viewport}
              nodes={nodes}
              edges={edges}
              onNodesChange={onNodesChange}
              onEdgesChange={onEdgesChange}
              setNodes={setNodes}
              setEdges={setEdges}
            />
          </RuntimeContext.Provider>
        ) : (
          <p className="hint" style={{ padding: 16 }}>
            Open a flow on the left or click New.
          </p>
        )}
      </main>
      <aside className="inspector">
        <Inspector node={node} onChange={updateData} />
      </aside>
      {current && running && node && (node.type === 'producer' || node.type === 'consumer') && (
        <TailDrawer key={`${current.id}/${node.id}`} flowId={current.id} node={node} tailSeq={flowState?.nodes[node.id]?.tailSeq} boot={flowState?.nodes[node.id]?.boot} />
      )}
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
