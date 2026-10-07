import { FlowSchema, type FlowFile } from './schema'

export type FlowSummary = { id: string; name: string; status: string }
// GET /api/flows/{id}/state: each producer's and consumer's container state while deployed.
export type FlowState = { status: 'running' | 'stopped'; nodes: Record<string, { state: string }> }
type Problem ={ node?: string; edge?: string; message: string }

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
  state: (id: string) => call<FlowState>(`/api/flows/${id}/state`),
}
