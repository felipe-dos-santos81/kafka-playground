import { z } from 'zod'

export const NODE_TYPES = ['producer', 'topic', 'consumer', 'transform'] as const
export type NodeType = (typeof NODE_TYPES)[number]

// Mirrors allowedEdges in ../../flow.go; the backend is the authority.
const ALLOWED: Record<NodeType, readonly NodeType[]> = {
  producer: ['topic'],
  topic: ['consumer'],
  consumer: ['topic', 'transform'],
  transform: ['topic'],
}

export function canConnect(source: string | undefined, target: string | undefined): boolean {
  return (ALLOWED[source as NodeType] ?? []).includes(target as NodeType)
}

// Which handles a node shows follows from the table: an input if anything may
// connect to it, an output if it may connect to anything.
export const hasInput = (type: NodeType) => NODE_TYPES.some((s) => canConnect(s, type))
export const hasOutput = (type: NodeType) => ALLOWED[type].length > 0

// The file format is React Flow's own shape plus id and name (spec 4.1).
export const FlowSchema = z.object({
  id: z.string(),
  name: z.string(),
  nodes: z.array(
    z.object({
      id: z.string(),
      type: z.enum(NODE_TYPES),
      position: z.object({ x: z.number(), y: z.number() }),
      data: z.record(z.string(), z.unknown()),
    }),
  ),
  edges: z.array(z.object({ id: z.string(), source: z.string(), target: z.string() })),
  viewport: z.object({ x: z.number(), y: z.number(), zoom: z.number() }).nullish(),
})
export type FlowFile = z.infer<typeof FlowSchema>

export const DEFAULT_INTERVAL_MS = 1000 // a timer producer's interval until one is set

// What a node dragged from the palette starts with.
export function defaultData(type: NodeType): Record<string, unknown> {
  switch (type) {
    case 'producer':
      return { source: 'manual', key: '', value: '{"id": {{.Seq}}}' }
    case 'topic':
      return { name: '', partitions: 1, replication_factor: 1 }
    case 'consumer':
      return { group: '', auto_offset_reset: 'earliest', sink: { kind: 'log' } }
    case 'transform':
      return { expr: 'msg' }
  }
}

const isObject = (v: unknown): v is Record<string, unknown> => typeof v === 'object' && v !== null && !Array.isArray(v)

type FileNode = FlowFile['nodes'][number]

// The node with every missing default data field filled in (one level into objects
// such as sink); filled reports whether anything was added, i.e. the file lacked it.
export function fillDefaults(node: FileNode): { node: FileNode; filled: boolean } {
  const out = { ...node.data }
  let filled = false
  for (const [k, v] of Object.entries(defaultData(node.type))) {
    if (!(k in out)) {
      out[k] = v
      filled = true
    } else if (isObject(v) && isObject(out[k])) {
      const inner = { ...v, ...out[k] }
      if (Object.keys(inner).length > Object.keys(out[k]).length) {
        out[k] = inner
        filled = true
      }
    }
  }
  return { node: { ...node, data: out }, filled }
}

// Smallest unused "<type>-<n>"; ids must match the Go nodeIDRe.
export function nextId(type: NodeType, nodes: { id: string }[]): string {
  const used = new Set(nodes.map((n) => n.id))
  for (let n = 1; ; n++) {
    const id = `${type}-${n}`
    if (!used.has(id)) return id
  }
}
