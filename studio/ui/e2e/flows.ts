// Flows through the studio's API, and the parts the tests build them from.
import config from '../playwright.config'
import type { FlowFile, NodeType } from '../src/flow/schema'

export const STUDIO_URL = String(config.use?.baseURL)

// The app's flow file (src/flow/schema.ts), without the id the studio assigns.
export type Flow = Omit<FlowFile, 'id'>
type FlowNode = Flow['nodes'][number]
type FlowEdge = Flow['edges'][number]
type Data = Record<string, unknown>

export async function api<T = unknown>(method: string, path: string, body?: unknown): Promise<T> {
  const res = await fetch(STUDIO_URL + path, {
    method,
    headers: { 'Content-Type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  })
  const text = await res.text()
  if (!res.ok) throw new Error(`${method} ${path}: ${res.status} ${text}`)
  return (text ? JSON.parse(text) : undefined) as T
}

export const deployFlow = (id: string) => api('POST', `/api/flows/${id}/deploy`)
export const deleteFlow = (id: string) => api('DELETE', `/api/flows/${id}`) // stops its containers first

export async function flowIdOf(name: string): Promise<string> {
  const f = (await api<{ id: string; name: string }[]>('GET', '/api/flows')).find((f) => f.name === name)
  if (!f) throw new Error(`no flow named ${name}`)
  return f.id
}

// A flow's parts, laid out left to right.
export const node = (id: string, type: NodeType, x: number, data: Data): FlowNode => ({ id, type, position: { x, y: 0 }, data })
export const edge = (source: string, target: string): FlowEdge => ({ id: `${source}-${target}`, source, target })
export const manual = { source: 'manual', key: '', value: '{"id": {{.Seq}}}' }
export const timer = (ms: number) => ({ source: 'timer', interval_ms: ms, key: '{{.Seq}}', value: '{"id": {{.Seq}}}' })
export const topic = (name: string, partitions = 1) => ({ name, partitions, replication_factor: 1 })
export const consumer = (group: string, more: Data = {}) => ({ group, auto_offset_reset: 'earliest', sink: { kind: 'log' }, ...more })

// chain is the flow producer-1 → topic-1 → consumer-1, with each node's data.
export function chain(name: string, producerData: Data, topicData: Data, consumerData: Data): Flow {
  return {
    name,
    nodes: [
      node('producer-1', 'producer', 0, producerData),
      node('topic-1', 'topic', 250, topicData),
      node('consumer-1', 'consumer', 500, consumerData),
    ],
    edges: [edge('producer-1', 'topic-1'), edge('topic-1', 'consumer-1')],
  }
}

// simple is a chain whose topic and consumer group are both named after the flow.
export const simple = (name: string, producerData: Data = manual) => chain(name, producerData, topic(name), consumer(name))

// The topics and consumer groups a flow file names.
export function topicsAndGroups(flow: Flow): { topics: string[]; groups: string[] } {
  const names = (type: string, field: string) =>
    flow.nodes.filter((n) => n.type === type && n.data[field]).map((n) => String(n.data[field]))
  return { topics: names('topic', 'name'), groups: names('consumer', 'group') }
}
