import { FlowSchema, type FlowFile } from './schema'

export type FlowSummary = { id: string; name: string; status: string }
// One node of the live snapshot (Go: NodeState in engine.go). Absent fields are zero,
// except lag, which is absent when the broker did not answer. A consumer with
// instances lists one entry per container in instances; its total, rate and errors
// are their sums.
export type NodeRuntime = {
  instance?: number
  state: string
  total?: number
  rate?: number
  errors?: number
  lastError?: string
  tailSeq?: number
  boot?: string
  lag?: number
  assigned?: Record<string, number[]>
  partitions?: number
  endOffset?: number
  warning?: string
  instances?: NodeRuntime[]
}

// The snapshot GET /api/flows/{id}/state answers and every SSE tick carries.
export type FlowState = { status: 'running' | 'stopped'; nodes: Record<string, NodeRuntime> }

// watch opens the flow's event stream: onTick gets a snapshot once a second.
// EventSource reconnects by itself; the returned function closes the stream.
export function watch(id: string, onTick: (s: FlowState) => void): () => void {
  const es = new EventSource(`/api/flows/${id}/events`)
  es.addEventListener('tick', (e) => onTick(JSON.parse((e as MessageEvent<string>).data)))
  return () => es.close()
}

// One record of a node's tail (Go: tailEntry in node.go).
export type TailEntry = { seq: number; time: string; partition: number; offset: number; key: string; value: string }
type Problem = { node?: string; edge?: string; message: string }

// No constructor parameter properties: the Vite template enables erasableSyntaxOnly.
export class ApiError extends Error {
  status: number
  constructor(status: number, message: string) {
    super(message)
    this.status = status
  }
}

async function call<T>(path: string, init: RequestInit = {}): Promise<T> {
  const res = await fetch(path, { headers: { 'Content-Type': 'application/json' }, ...init })
  if (res.status === 204) return undefined as T
  const body = await res.json().catch(() => ({}))
  if (!res.ok) {
    const problems: Problem[] = body.errors ?? []
    const message =
      body.error ??
      (problems.length
        ? problems.map((p) => `${p.node ?? p.edge ?? 'flow'}: ${p.message}`).join('; ')
        : res.statusText)
    throw new ApiError(res.status, message)
  }
  return body as T
}

export const api = {
  list: () => call<FlowSummary[]>('/api/flows'),
  get: async (id: string) => FlowSchema.parse(await call<unknown>(`/api/flows/${id}`)),
  create: (flow: Omit<FlowFile, 'id'>) =>
    call<FlowFile>('/api/flows', { method: 'POST', body: JSON.stringify(flow) }),
  save: (flow: FlowFile) =>
    call<FlowFile>(`/api/flows/${flow.id}`, { method: 'PUT', body: JSON.stringify(flow) }),
  remove: (id: string) => call<void>(`/api/flows/${id}`, { method: 'DELETE' }),
  deploy: (id: string) => call<{ status: string }>(`/api/flows/${id}/deploy`, { method: 'POST' }),
  stop: (id: string) => call<{ status: string }>(`/api/flows/${id}/stop`, { method: 'POST' }),
  // An empty body makes the producer render its own key and value templates.
  send: (id: string, node: string) =>
    call<{ partition: number; offset: number }>(`/api/flows/${id}/nodes/${node}/send`, { method: 'POST' }),
  // instance picks one of a consumer's instances; 0 is the node's only container.
  tail: (id: string, node: string, since: number, instance = 0) =>
    call<TailEntry[]>(`/api/flows/${id}/nodes/${node}/tail?since=${since}${instance ? `&instance=${instance}` : ''}`),
}

export function describe(e: unknown): string {
  return e instanceof ApiError ? `${e.status}: ${e.message}` : String(e)
}
